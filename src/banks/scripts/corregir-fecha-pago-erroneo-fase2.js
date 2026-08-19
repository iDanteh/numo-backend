'use strict';

/**
 * corregir-fecha-pago-erroneo-fase2.js — Fase 2 del fix de fecha_pago erróneo.
 *
 * Fase 1 (enriquecer-facturas-p-fecha-pago-erroneo.js) ya resolvió serie-folio de venta +
 * fecha XML actual usando solo Mongo. Esta fase consulta Kore en vivo (GET /cuentas-pendientes,
 * mismo patrón que erp.routes.js#_rangoDesdeFollo/_sincronizarConRetry/_rangoSpilloverSiguienteMes)
 * para traer `fechaRealPago` — el dato correcto contra el que se valida el desfase de la fecha
 * XML (confirmado con el usuario 2026-08-19, ejemplo real: venta B0-260508132, fechaRealPago
 * Kore = 2026-06-30T00:00:00Z vs fecha XML 2026-06-29 18:00 hora local → cuadra con +6hrs).
 *
 * IMPORTANTE: .env normalmente apunta ERP_CAJA_BASE_URL a un servidor de TEST. Este script
 * SIEMPRE fuerza la URL de producción (KORE_CAJA_PENDIENTES_PROD_URL más abajo) antes de
 * requerir erp.routes.js, porque erp-sync.service.js lee ERP_CAJA_BASE_URL una sola vez al
 * cargar el módulo — el usuario confirmó que el mismo ERP_TOKEN sirve para test y producción.
 *
 * Input:  pagos_con_erroresV2.xlsx (raíz del repo) — columna 'ventas' trae serie-folio de
 *         cada venta liquidada por el Pago (multi-venta concatenado con ' || ', alineado por
 *         posición con 'fechas_pago'/'importes', mismo criterio ya usado en ese archivo).
 * Output: pagos_con_erroresV2_corregido.xlsx — el MISMO workbook original (mismas columnas,
 *         formato, anchos, sin tocar nada existente) + UNA columna nueva 'fechaRealPago' al
 *         final, formato dd/mm/yyyy sin hora (pedido explícito del usuario 2026-08-19). Filas
 *         multi-venta concatenan con ' || ' igual que 'ventas'/'fechas_pago'. Si una venta no
 *         se pudo resolver, el motivo va en el mismo texto (ej. "No encontrado en Kore.") en
 *         vez de una columna aparte — el usuario pidió agregar solo esta columna.
 *
 * Solo lectura contra Kore (GET), no escribe nada en Mongo. Dedupea ventas repetidas entre
 * filas para no pegarle dos veces a Kore por la misma CxC.
 *
 * Uso:
 *   node src/banks/scripts/corregir-fecha-pago-erroneo-fase2.js [--limit N] [inputPath] [outputPath]
 *
 * --limit N: procesa solo las primeras N filas del Excel (para probar antes de correr las
 *            ~2340 ventas únicas reales contra producción, que a SYNC_DELAY_MS toma ~40min).
 */

require('dotenv').config();

const KORE_CAJA_PENDIENTES_PROD_URL = 'https://app.cajas.tubosyconexiones.mx';
process.env.ERP_CAJA_BASE_URL = KORE_CAJA_PENDIENTES_PROD_URL;

const path    = require('path');
const ExcelJS = require('exceljs');
const erpRoutes = require('../domains/erp/erp.routes');

const REPO_ROOT = path.resolve(__dirname, '../../../../');

const args      = process.argv.slice(2);
const limitIdx  = args.indexOf('--limit');
const LIMIT     = limitIdx !== -1 ? parseInt(args[limitIdx + 1], 10) : null;
const positional = args.filter((a, i) => a !== '--limit' && i !== limitIdx + 1);

const inputPath  = positional[0] ? path.resolve(positional[0]) : path.join(REPO_ROOT, 'pagos_con_erroresV2.xlsx');
const outputPath = positional[1] ? path.resolve(positional[1]) : path.join(REPO_ROOT, 'pagos_con_erroresV2_corregido.xlsx');

const SYNC_DELAY_MS = erpRoutes.SYNC_DELAY_MS ?? 1000;
const _sleep = ms => new Promise(r => setTimeout(r, ms));

// 'A0-260504308' → { serie: 'A0', folio: '260504308' }. El folio nunca trae '-', el serie sí
// puede tener 1-2 caracteres alfanuméricos (A0, G1, H0, etc.) — split en el ÚLTIMO '-'.
function parseSerieFolio(str) {
  const s = String(str || '').trim();
  const idx = s.lastIndexOf('-');
  if (idx === -1) return null;
  return { serie: s.slice(0, idx), folio: s.slice(idx + 1) };
}

// Kore devuelve fechaRealPago en ISO UTC (ej. "2026-06-30T00:00:00Z"). Se usan los getters
// UTC (no locales) para no correr el día si la máquina que corre el script tiene otro huso
// horario — el valor ya representa el día calendario correcto tal cual lo manda Kore.
function formatFechaDDMMYYYY(iso) {
  const d = new Date(iso);
  if (isNaN(d.getTime())) return '';
  const dd   = String(d.getUTCDate()).padStart(2, '0');
  const mm   = String(d.getUTCMonth() + 1).padStart(2, '0');
  const yyyy = d.getUTCFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

async function consultarVenta(serie, folio) {
  const rango = erpRoutes._rangoDesdeFollo(folio);
  if (!rango) return { error: 'No se pudo determinar el rango de fecha para este folio.' };

  try {
    let { raw } = await erpRoutes._sincronizarConRetry({
      serieExterna: serie, folioExterno: folio,
      fechaDesde: rango.fechaDesde, fechaHasta: rango.fechaHasta,
    });

    if (raw.length === 0) {
      const spillover = erpRoutes._rangoSpilloverSiguienteMes(folio);
      if (spillover) {
        await _sleep(SYNC_DELAY_MS);
        const retryRes = await erpRoutes._sincronizarConRetry({
          serieExterna: serie, folioExterno: folio,
          fechaDesde: spillover.fechaDesde, fechaHasta: spillover.fechaHasta,
        });
        if (retryRes.raw.length > 0) raw = retryRes.raw;
      }
    }

    const raw0 = raw.find(c => String(c.folioExterno) === folio && String(c.serieExterna) === serie);
    if (!raw0) return { error: 'No encontrado en Kore.' };

    return { fechaRealPago: raw0.fechaRealPago ?? null };
  } catch (err) {
    return { error: err.message || 'Error al consultar Kore.' };
  }
}

async function main() {
  console.log(`Leyendo ${inputPath}...`);
  console.log(`ERP_CAJA_BASE_URL forzado a: ${process.env.ERP_CAJA_BASE_URL}`);
  const wbIn = new ExcelJS.Workbook();
  await wbIn.xlsx.readFile(inputPath);
  const wsIn = wbIn.worksheets[0];

  const headerRow = wsIn.getRow(1).values.slice(1);
  const ventasCol = headerRow.indexOf('ventas') + 1;
  if (ventasCol === 0) throw new Error(`No se encontró la columna 'ventas' en ${inputPath}`);

  const allRows = [];
  wsIn.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return;
    allRows.push({ rowNumber, ventas: row.getCell(ventasCol).value });
  });
  const rowsToQuery = LIMIT ? allRows.slice(0, LIMIT) : allRows;
  console.log(`${allRows.length} filas en el archivo, ${rowsToQuery.length} a consultar${LIMIT ? ` (--limit ${LIMIT})` : ''}.`);

  // Dedupe: la misma venta puede repetirse si un pago fue registrado dos veces por error, etc.
  const cache = new Map(); // 'SERIE|FOLIO' -> { fechaRealPago } | { error }
  const pendientes = [];
  for (const r of rowsToQuery) {
    for (const item of String(r.ventas || '').split(' || ')) {
      const parsed = parseSerieFolio(item);
      if (!parsed) continue;
      const key = `${parsed.serie}|${parsed.folio}`;
      if (!cache.has(key)) pendientes.push({ key, ...parsed });
    }
  }
  const unicas = [...new Map(pendientes.map(p => [p.key, p])).values()];
  console.log(`${unicas.length} ventas únicas a consultar en Kore (producción).`);

  let ok = 0, sinDatos = 0, errores = 0;
  for (let i = 0; i < unicas.length; i++) {
    const { key, serie, folio } = unicas[i];
    const resultado = await consultarVenta(serie, folio);
    cache.set(key, resultado);
    if (resultado.fechaRealPago) ok++;
    else if (resultado.error === 'No encontrado en Kore.') sinDatos++;
    else errores++;

    if ((i + 1) % 25 === 0 || i === unicas.length - 1) {
      console.log(`  [${i + 1}/${unicas.length}] ok=${ok} sin_datos=${sinDatos} errores=${errores}`);
    }
    await _sleep(SYNC_DELAY_MS);
  }

  // Se agrega la columna nueva DIRECTO sobre el worksheet ya leído (mismo workbook, mismas
  // celdas/estilos/anchos existentes intactos) — nunca se reconstruye la hoja, para no alterar
  // el formato del archivo original.
  const nuevaCol = headerRow.length + 1;
  wsIn.getRow(1).getCell(nuevaCol).value = 'fechaRealPago';

  for (const r of allRows) {
    const items = String(r.ventas || '').split(' || ').map(parseSerieFolio);
    const resultados = items.map(p => (p && cache.has(`${p.serie}|${p.folio}`)) ? cache.get(`${p.serie}|${p.folio}`) : null);

    const texto = resultados.map(res => {
      if (!res) return '';
      if (res.fechaRealPago) return formatFechaDDMMYYYY(res.fechaRealPago);
      return res.error || 'Sin dato.';
    }).join(' || ');

    wsIn.getRow(r.rowNumber).getCell(nuevaCol).value = texto;
  }

  await wbIn.xlsx.writeFile(outputPath);
  console.log(`\nListo: ${outputPath}`);
  console.log(`Ventas únicas: ${unicas.length} | ok=${ok} sin_datos=${sinDatos} errores=${errores}`);
}

main().catch(function (err) { console.error(err); process.exit(1); });
