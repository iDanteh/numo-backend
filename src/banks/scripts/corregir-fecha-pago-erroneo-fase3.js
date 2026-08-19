'use strict';

/**
 * corregir-fecha-pago-erroneo-fase3.js — Fase 3 (definitiva) del fix de fecha_pago erróneo.
 *
 * Reemplaza el enfoque de Fase 2 (corregir-fecha-pago-erroneo-fase2.js), que asumía que
 * `cuenta.fechaRealPago` (nivel CxC, un solo valor) servía para CUALQUIER fila que
 * referenciara esa venta. Eso es falso cuando una venta recibe MÁS DE UN abono parcial:
 * cada fila (cada CFDI de Pago) necesita la fecha de SU abono específico, no la de la CxC
 * completa (encontrado por el usuario 2026-08-19, venta A0-260600337: fila 4 = abono
 * ABO-260700206/Aut 032510 → depósito real 30/06/2026; fila 124 = abono ABO-260702936/Aut
 * 033170 → depósito real 03/07/2026 — `fechaRealPago` de Kore solo coincidía con UNA de
 * las dos).
 *
 * Fuente de verdad, de más a menos confiable:
 *   1. LOCAL (sin tocar Kore) — BankMovement.erpLinks[].movimientosKore ya tiene cacheado
 *      el mismo array de movimientos que Kore devolvería en vivo (se llena/actualiza cada
 *      vez que corre el job "Sync ERP-Kore"). Se busca la venta por
 *      erpLinks.serie+erpLinks.folioExterno, se ubica el movimiento ABO con el folio exacto
 *      de la columna 'abonos', se extrae su "Aut" (formasPago[].adicionales, nombre='Aut'),
 *      y se busca DIRECTO `BankMovement.findOne({folio: aut})` — folio en BankMovement es
 *      EXACTAMENTE el mismo string que Kore reporta como "Aut" (verificado con 3 ejemplos
 *      reales del usuario, incluyendo ceros a la izquierda — sin normalizar, comparación
 *      exacta). Su campo `fecha` es la fecha real del depósito, verificada contra el banco.
 *   2. KORE EN VIVO (respaldo, solo cuando (1) no resuelve) — mismo patrón que Fase 2:
 *      GET /cuentas-pendientes por serieExterna/folioExterno de la venta, ubicar el
 *      movimiento ABO por folio exacto, extraer Aut, y de nuevo intentar
 *      `BankMovement.findOne({folio: aut})` primero (puede existir en Mongo sin estar
 *      linkeado vía erpLinks todavía); si tampoco existe ahí, se usa como último recurso
 *      el `fecha` del propio movimiento que reporta Kore (menos confiable — es la fecha en
 *      que KORE procesó el abono, no necesariameente la fecha real bancaria, ver hallazgo
 *      2026-08-19 de un desfase de +1 día en un caso real).
 *
 * Toda fila/abono que requirió (2) queda registrada en una hoja nueva "Requirieron Kore"
 * del archivo de salida (pedido explícito del usuario 2026-08-19), con el método usado.
 *
 * Input:  pagos_con_erroresV2.xlsx (raíz del repo, el ORIGINAL — no el de Fase 2).
 * Output: pagos_con_erroresV2_corregido.xlsx — mismo workbook original + UNA columna
 *         'fechaRealPago' (dd/mm/yyyy) en la hoja principal + hoja nueva "Requirieron Kore".
 *
 * Uso:
 *   node src/banks/scripts/corregir-fecha-pago-erroneo-fase3.js [--limit N] [inputPath] [outputPath]
 */

require('dotenv').config();

const KORE_CAJA_PENDIENTES_PROD_URL = 'https://app.cajas.tubosyconexiones.mx';
process.env.ERP_CAJA_BASE_URL = KORE_CAJA_PENDIENTES_PROD_URL;

const path    = require('path');
const ExcelJS = require('exceljs');
const erpRoutes = require('../domains/erp/erp.routes');
const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const BankMovement = require('../domains/banks/BankMovement.model');
const { ERP_TOLERANCE } = require('../domains/banks/bank.service');

const REPO_ROOT = path.resolve(__dirname, '../../../../');

const args      = process.argv.slice(2);
const limitIdx  = args.indexOf('--limit');
const LIMIT     = limitIdx !== -1 ? parseInt(args[limitIdx + 1], 10) : null;
const positional = args.filter((a, i) => a !== '--limit' && i !== limitIdx + 1);

const inputPath  = positional[0] ? path.resolve(positional[0]) : path.join(REPO_ROOT, 'pagos_con_erroresV2.xlsx');
const outputPath = positional[1] ? path.resolve(positional[1]) : path.join(REPO_ROOT, 'pagos_con_erroresV2_corregido.xlsx');

const SYNC_DELAY_MS = erpRoutes.SYNC_DELAY_MS ?? 1000;
const _sleep = ms => new Promise(r => setTimeout(r, ms));

function parseSerieFolio(str) {
  const s = String(str || '').trim();
  const idx = s.lastIndexOf('-');
  if (idx === -1) return null;
  return { serie: s.slice(0, idx), folio: s.slice(idx + 1) };
}

function formatFechaDDMMYYYY(fechaOrIso) {
  const d = fechaOrIso instanceof Date ? fechaOrIso : new Date(fechaOrIso);
  if (isNaN(d.getTime())) return '';
  const dd   = String(d.getUTCDate()).padStart(2, '0');
  const mm   = String(d.getUTCMonth() + 1).padStart(2, '0');
  const yyyy = d.getUTCFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

function extraerAut(movimientoAbo) {
  const fp = (movimientoAbo.formasPago || [])[0];
  const ad = (fp?.adicionales || []).find(a => a.nombre === 'Aut');
  return ad?.valor || null;
}

// El folio de la columna 'abonos' del Excel NO siempre coincide con el folio real del
// movimiento ABO en Kore — verificado 2026-08-19: en filas multi-venta (un solo abono
// bancario partido entre varias ventas), cada venta reporta su PROPIO folio de ABO en
// Kore, distinto del que trae esta columna del Excel (origen desconocido, posiblemente un
// id sintético del proceso que armó el concentrado). El importe SÍ es confiable siempre
// (`movimiento.total` es negativo, igual en valor absoluto al importe de la fila) — se usa
// como criterio de match, verificado contra los 3 ejemplos de un solo abono Y contra el
// caso multi-venta real (fila 5: -6138.83 calza exacto con importe 6138.83).
//
// Tolerancia: NO comparación exacta — verificado 2026-08-19 (venta A0-260604480) que el
// importe del Excel (1640.94) puede diferir en centavos del `total` que reporta Kore para
// ese movimiento (1641), por redondeo de impuestos entre el XML y el ledger de Kore. Se usa
// ERP_TOLERANCE (±$1 MXN, misma constante que ya usa bank-autorizaciones.service.js para
// este mismo tipo de match) en vez de una comparación exacta. Si hay más de un candidato
// dentro de tolerancia, se toma el de importe MÁS CERCANO (no el primero).
function encontrarMovimientoAbo(movimientos, importe) {
  if (!Number.isFinite(importe)) return null;
  const candidatos = (movimientos || [])
    .filter(m => m.serie === 'ABO' && Math.abs(Math.abs(m.total) - importe) <= ERP_TOLERANCE)
    .sort((a, b) => Math.abs(Math.abs(a.total) - importe) - Math.abs(Math.abs(b.total) - importe));
  return candidatos[0] || null;
}

// Monto TOTAL de la transacción bancaria detrás de un movimiento ABO (no la porción de ESTA
// CxC): `formasPago[0].monto` es el monto real cobrado en el banco — para un depósito/cheque
// que paga UNA sola CxC coincide con `total`, pero para uno que paga VARIAS a la vez (ej.
// fila 196: un cheque de $14,223.10 que liquida 2 ventas juntas) `total` por CxC es solo la
// porción (-12062.9), mientras que `monto` es el cobro completo (14223.1) — el que hay que
// comparar contra `BankMovement.deposito` para saber si ESTE documento es la fuente real.
function montoTransaccion(mov) {
  const monto = mov.formasPago?.[0]?.monto;
  return Number.isFinite(monto) ? monto : Math.abs(mov.total);
}

// ── Paso 1: mapa venta ("serie|folioExterno") → candidatos locales (docId + checkpoints) ──
async function construirVentaMap() {
  const cursor = BankMovement.find(
    { 'erpLinks.0': { $exists: true } },
    { 'erpLinks.serie': 1, 'erpLinks.folioExterno': 1, 'erpLinks.recomputedFormasPagoAt': 1, 'erpLinks.conciliacionFinalizadaAt': 1 },
  ).lean().cursor();

  const map = new Map(); // 'serie|folio' -> [{docId, recomputedFormasPagoAt, conciliacionFinalizadaAt}]
  let docs = 0;
  for await (const doc of cursor) {
    docs++;
    for (const link of (doc.erpLinks || [])) {
      if (!link.serie || !link.folioExterno) continue;
      const key = `${link.serie}|${link.folioExterno}`;
      if (!map.has(key)) map.set(key, []);
      map.get(key).push({
        docId: doc._id,
        recomputedFormasPagoAt: link.recomputedFormasPagoAt,
        conciliacionFinalizadaAt: link.conciliacionFinalizadaAt,
      });
    }
  }
  console.log(`ventaMap construido: ${docs} BankMovement con erpLinks, ${map.size} ventas distintas referenciadas.`);
  return map;
}

// ── Paso 2: para una venta con candidatos locales, traer cada documento CON su propio
// movimientosKore (sin mezclar entre documentos — la provenencia importa, ver abajo) ──
//
// `movimientosKore` de un documento es un snapshot de TODA la historia de pagos de esa CxC,
// no solo la parte que aportó ESTE documento — verificado 2026-08-19 (venta A0-260600337):
// el documento del PRIMER abono (folio 032510) ya lista TAMBIÉN el segundo abono (que en
// realidad pertenece a otro documento, folio 033170). Por eso no basta con "¿este importe
// aparece en el movimientosKore de este candidato?" — hay que confirmar que el documento
// candidato es realmente la FUENTE de ese movimiento específico antes de usar su fecha.
//
// La confirmación es el monto TOTAL de la transacción (`montoTransaccion()`, arriba) contra
// el `deposito` del propio documento — eso sí es único por documento, a diferencia del
// `total` del movimiento (que es solo la porción de ESTA CxC, igual entre documentos
// distintos que comparten importes coincidentes es raro pero el monto total es la firma
// real). Verificado con el caso CHEQUE de la fila 196: `total` del movimiento = -12062.9
// (porción de esta venta), pero `formasPago[0].monto` = 14223.1 = EXACTO al `deposito` del
// documento que realmente lo generó (paga 2 ventas juntas) — así se distingue sin necesitar
// "Aut" (que este tipo de pago, CHEQUE/efectivo, nunca trae).
async function obtenerCandidatosLocales(serie, folioExterno, candidatos) {
  candidatos.sort((a, b) => {
    const ta = a.recomputedFormasPagoAt || a.conciliacionFinalizadaAt || 0;
    const tb = b.recomputedFormasPagoAt || b.conciliacionFinalizadaAt || 0;
    return new Date(tb) - new Date(ta);
  });

  const porDocumento = []; // [{fecha, deposito, movimientosKore}]
  const merged = new Map(); // 'serie|folio' del movimiento -> movimiento (dedup, para el respaldo por Aut)
  for (const c of candidatos) {
    const doc = await BankMovement.findById(c.docId).select('erpLinks fecha deposito').lean();
    if (!doc) continue;
    const link = (doc.erpLinks || []).find(l => l.serie === serie && l.folioExterno === folioExterno);
    const movimientosKore = link?.movimientosKore || [];
    porDocumento.push({ fecha: doc.fecha, deposito: doc.deposito, movimientosKore });
    for (const mov of movimientosKore) {
      const key = `${mov.serie}|${mov.folio}`;
      if (!merged.has(key)) merged.set(key, mov);
    }
  }
  return { porDocumento, movimientosKoreMerged: [...merged.values()] };
}

// Confirma, entre los documentos candidatos, cuál generó realmente el abono que calza con
// `importe` — exige que el monto TOTAL de la transacción (no solo la porción de esta CxC)
// coincida con el `deposito` propio del documento. Devuelve la fecha de ESE documento, la
// única fuente confiable (`movimientosKore[].fecha` es cuándo Kore procesó el abono
// internamente, no la fecha bancaria real — ver hallazgo 2026-08-19).
function resolverPorDocumentoPropio(porDocumento, importe) {
  for (const doc of (porDocumento || [])) {
    const mov = encontrarMovimientoAbo(doc.movimientosKore, importe);
    if (mov && Number.isFinite(doc.deposito) && Math.abs(montoTransaccion(mov) - doc.deposito) <= ERP_TOLERANCE) {
      return doc.fecha;
    }
  }
  return null;
}

// ── Kore en vivo (respaldo) — mismo patrón que Fase 2 ──
// _sincronizarConRetry (erp.routes.js) solo reintenta 429/503 — un error de RED (sin
// respuesta HTTP: DNS, timeout, conexión abortada) no tiene status y se propaga de
// inmediato. Verificado 2026-08-19 en la corrida completa: 51/1478 filas fallaron así
// (39 ENOTFOUND, 11 timeout, 1 ECONNABORTED) — todos transitorios, ninguno un dato real
// faltante. Se reintenta acá, a nivel de esta función, con backoff corto.
const NETWORK_RETRY_MAX = 4;
const NETWORK_RETRY_DELAY_MS = 3000;

function esErrorDeRed(err) {
  return !err.response && ['ENOTFOUND', 'ECONNABORTED', 'ECONNRESET', 'ETIMEDOUT'].includes(err.code)
    || /timeout of \d+ms exceeded/i.test(err.message || '');
}

async function consultarVentaKore(serie, folio) {
  const rango = erpRoutes._rangoDesdeFollo(folio);
  if (!rango) return { error: 'No se pudo determinar el rango de fecha para este folio.' };

  for (let intento = 0; intento < NETWORK_RETRY_MAX; intento++) {
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
      return { movimientos: raw0.movimientos || [] };
    } catch (err) {
      if (esErrorDeRed(err) && intento < NETWORK_RETRY_MAX - 1) {
        console.warn(`  [red] ${serie}-${folio}: ${err.message} — reintento ${intento + 1}/${NETWORK_RETRY_MAX}`);
        await _sleep(NETWORK_RETRY_DELAY_MS);
        continue;
      }
      return { error: err.message || 'Error al consultar Kore.' };
    }
  }
}

async function main() {
  console.log(`Leyendo ${inputPath}...`);
  const wbIn = new ExcelJS.Workbook();
  await wbIn.xlsx.readFile(inputPath);
  const wsIn = wbIn.worksheets[0];

  const headerRow  = wsIn.getRow(1).values.slice(1);
  const ventasCol  = headerRow.indexOf('ventas') + 1;
  const abonosCol  = headerRow.indexOf('abonos') + 1;
  const importesCol = headerRow.indexOf('importes') + 1;
  if (!ventasCol || !abonosCol || !importesCol) throw new Error(`Faltan columnas 'ventas'/'abonos'/'importes' en ${inputPath}`);

  const allRows = [];
  wsIn.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return;
    allRows.push({
      rowNumber,
      ventas:   row.getCell(ventasCol).value,
      abonos:   row.getCell(abonosCol).value,
      importes: row.getCell(importesCol).value,
    });
  });
  const rowsToProcess = LIMIT ? allRows.slice(0, LIMIT) : allRows;
  console.log(`${allRows.length} filas en el archivo, ${rowsToProcess.length} a procesar${LIMIT ? ` (--limit ${LIMIT})` : ''}.`);

  await connectMongo();

  const ventaMap = await construirVentaMap();

  // Cache de movimientosKore locales YA resueltos por venta (para no repetir la query por doc)
  const movimientosLocalCache = new Map(); // 'serie|folio' venta -> Map('serie|folio' abono -> mov) | null (no hay locales)
  // Cache de resultados Kore en vivo por venta (para no repetir la llamada si la venta se repite)
  const koreCache = new Map(); // 'serie|folio' venta -> { movimientos } | { error }
  // Cache de fecha por Aut (BankMovement.folio) ya resuelto
  const fechaPorAutCache = new Map(); // aut -> Date | null

  async function resolverFechaPorAut(aut) {
    if (fechaPorAutCache.has(aut)) return fechaPorAutCache.get(aut);
    const bm = await BankMovement.findOne({ folio: aut }).select('fecha').lean();
    const fecha = bm?.fecha ?? null;
    fechaPorAutCache.set(aut, fecha);
    return fecha;
  }

  const requirieronKore = []; // filas para la hoja "Requirieron Kore"
  let statLocal = 0, statKoreBancos = 0, statKoreSinVerificar = 0, statError = 0;

  const resultadosPorItem = new Map(); // `${rowNumber}|${idx}` -> { texto, metodo }

  for (let rIdx = 0; rIdx < rowsToProcess.length; rIdx++) {
    const r = rowsToProcess[rIdx];
    const ventasItems  = String(r.ventas   || '').split(' || ').map(parseSerieFolio);
    const abonosItems  = String(r.abonos   || '').split(' || ');
    const importesItems = String(r.importes || '').split(' || ').map(v => parseFloat(v));

    for (let i = 0; i < ventasItems.length; i++) {
      const venta = ventasItems[i];
      const abonoStr = (abonosItems[i] || '').trim();
      const importe = importesItems[i];
      const key = `${r.rowNumber}|${i}`;
      if (!venta || !Number.isFinite(importe)) { resultadosPorItem.set(key, { texto: 'Serie-folio/importe inválido.', metodo: 'error' }); statError++; continue; }

      const ventaKey = `${venta.serie}|${venta.folio}`;

      // ── Intento 1: local (documentos con erpLinks a esta venta, ya cacheados) ──
      if (!movimientosLocalCache.has(ventaKey)) {
        const candidatos = ventaMap.get(ventaKey);
        if (candidatos && candidatos.length) {
          movimientosLocalCache.set(ventaKey, await obtenerCandidatosLocales(venta.serie, venta.folio, candidatos));
        } else {
          movimientosLocalCache.set(ventaKey, null);
        }
      }
      const local = movimientosLocalCache.get(ventaKey);

      if (local) {
        // 1a — el documento candidato ES la fuente real de este abono (monto total de la
        // transacción coincide con su propio deposito) — cubre efectivo/cheque/transferencia
        // por igual, sin depender de que Kore reporte "Aut". Máxima confianza.
        const fechaPropia = resolverPorDocumentoPropio(local.porDocumento, importe);
        if (fechaPropia) {
          resultadosPorItem.set(key, { texto: formatFechaDDMMYYYY(fechaPropia), metodo: 'local-directo' });
          statLocal++;
          continue;
        }
        // 1b — respaldo: el abono pertenece a OTRO documento que no está entre los
        // candidatos conocidos de esta venta (ej. un pago anterior de la misma CxC que esta
        // venta no alcanzó a capturar) — se ubica por su "Aut" en el snapshot combinado.
        const movLocal = encontrarMovimientoAbo(local.movimientosKoreMerged, importe);
        if (movLocal) {
          const aut = extraerAut(movLocal);
          const fechaBanco = aut ? await resolverFechaPorAut(aut) : null;
          if (fechaBanco) {
            resultadosPorItem.set(key, { texto: formatFechaDDMMYYYY(fechaBanco), metodo: 'local-aut' });
            statLocal++;
            continue;
          }
        }
      }

      // ── Intento 2: Kore en vivo (respaldo) ──
      if (!koreCache.has(ventaKey)) {
        const res = await consultarVentaKore(venta.serie, venta.folio);
        koreCache.set(ventaKey, res);
        await _sleep(SYNC_DELAY_MS);
      }
      const koreRes = koreCache.get(ventaKey);

      let texto, metodo;
      if (koreRes.error) {
        texto = koreRes.error; metodo = 'error'; statError++;
      } else {
        const movKore = encontrarMovimientoAbo(koreRes.movimientos, importe);
        if (!movKore) {
          texto = 'Abono no encontrado en Kore.'; metodo = 'error'; statError++;
        } else {
          const aut = extraerAut(movKore);
          const fechaBanco = aut ? await resolverFechaPorAut(aut) : null;
          if (fechaBanco) {
            texto = formatFechaDDMMYYYY(fechaBanco); metodo = 'kore+bancos'; statKoreBancos++;
          } else {
            texto = formatFechaDDMMYYYY(movKore.fecha); metodo = 'kore-sin-verificar'; statKoreSinVerificar++;
          }
        }
      }
      resultadosPorItem.set(key, { texto, metodo });
      requirieronKore.push({
        fila: r.rowNumber, ventaSerieFolio: `${venta.serie}-${venta.folio}`, abono: abonoStr,
        importe: importesItems[i], resultado: texto, metodo,
      });
    }

    if ((rIdx + 1) % 100 === 0 || rIdx === rowsToProcess.length - 1) {
      console.log(`  [${rIdx + 1}/${rowsToProcess.length}] local=${statLocal} kore+bancos=${statKoreBancos} kore-sin-verificar=${statKoreSinVerificar} error=${statError}`);
    }
  }

  console.log(`Total: local=${statLocal} kore+bancos=${statKoreBancos} kore-sin-verificar=${statKoreSinVerificar} error=${statError}`);

  // ── Escribir columna nueva en la hoja principal (mismo archivo, sin reconstruir nada) ──
  const nuevaCol = headerRow.length + 1;
  wsIn.getRow(1).getCell(nuevaCol).value = 'fechaRealPago';

  for (const r of allRows) {
    const ventasItems = String(r.ventas || '').split(' || ');
    const textos = ventasItems.map((_, i) => resultadosPorItem.get(`${r.rowNumber}|${i}`)?.texto ?? '');
    wsIn.getRow(r.rowNumber).getCell(nuevaCol).value = textos.join(' || ');
  }

  // ── Hoja nueva: concentrado de lo que requirió Kore ──
  const wsKore = wbIn.addWorksheet('Requirieron Kore');
  wsKore.columns = [
    { header: 'fila_excel',   key: 'fila',      width: 12 },
    { header: 'venta',        key: 'venta',     width: 18 },
    { header: 'abono',        key: 'abono',     width: 18 },
    { header: 'importe',      key: 'importe',   width: 14 },
    { header: 'fechaRealPago', key: 'resultado', width: 16 },
    { header: 'metodo',       key: 'metodo',    width: 20 },
  ];
  for (const item of requirieronKore) {
    wsKore.addRow({ fila: item.fila, venta: item.ventaSerieFolio, abono: item.abono, importe: item.importe, resultado: item.resultado, metodo: item.metodo });
  }

  await wbIn.xlsx.writeFile(outputPath);
  await disconnectMongo();

  console.log(`\nListo: ${outputPath}`);
  console.log(`Requirieron Kore: ${requirieronKore.length} abonos (hoja "Requirieron Kore")`);
}

main().catch(function (err) { console.error(err); process.exit(1); });
