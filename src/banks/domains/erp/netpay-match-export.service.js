'use strict';

// netpay-match-export.service.js — export de la bandeja de matching Netpay↔BBVA
// (NetpayMatch) a Excel, pedido explícito del usuario: debe enriquecer los folios con
// datos de Kore, igual que ya hace la ventana de carga del reporte manual (ver
// netpay-reporte.service.js#consultarFoliosPendientes/consultarFolioKore) — pero acá la
// unidad no es un solo NetpayReporte sino TODOS los buckets que matchean el filtro de la
// bandeja (potencialmente muchos más folios), así que antes de pegarle a Kore se pone un
// techo duro (MAX_FOLIOS_PENDIENTES_EXPORT) — si se supera, falla ANTES de hacer ninguna
// llamada, en vez de arriesgarse a colgar el request (decisión explícita del usuario,
// 2026-09-30: "que avise en vez de arriesgarse a colgar").

const ExcelJS = require('exceljs');
const NetpayMatch = require('./NetpayMatch.model');
const { buscarTransaccionesNetpay } = require('./kore-caja.service');
const { UnprocessableError, ConflictError } = require('../../shared/errors/AppError');
const { logger } = require('../../../shared/utils/logger');
const {
  STATUS_LABELS, _formatFecha, _koreCacheColumnas,
} = require('./netpay-reporte-export.service');

// Mismo patrón de lotes/pausa/reintento-ante-429 que
// netpay-reporte.service.js#consultarFoliosPendientes — ver ese archivo para el porqué de
// estos valores (no se reusa tal cual: esa función está atada a UN NetpayReporte, acá la
// unidad de persistencia es un folio dentro de un bucket NetpayMatch cualquiera).
const CONSULTAR_FOLIOS_CONCURRENCIA = 5;
const CONSULTAR_FOLIOS_PAUSA_MS = 400;
const CONSULTAR_FOLIOS_MAX_INTENTOS_429 = 3;

// Techo duro de folios sin koreCache.cuenta que este export está dispuesto a consultar en
// un solo request — a diferencia del export de UN reporte (20-50 folios típico), la
// bandeja filtrada puede cubrir muchos buckets a la vez.
const MAX_FOLIOS_PENDIENTES_EXPORT = 300;

// Presupuesto de tiempo del LOOP de consulta (no de la ruta completa) — deja 60s de margen
// bajo el req.setTimeout(300000)/res.setTimeout(300000) de la ruta para lo que viene después
// (populate + generarExcelBandejaNetpay + envío del buffer). Hallazgo de revisión de
// resiliencia (2026-09-30): con 300 folios topeados, si suficientes lotes caen en reintento
// 429 (cada uno hasta ~60-66s, ver CONSULTAR_FOLIOS_MAX_INTENTOS_429/_consultarYPersistirFolio),
// la suma podía superar el timeout de la ruta y colgar el request sin responder nada — el
// corte acá garantiza que SIEMPRE se llegue a generar el Excel con lo que se pudo resolver,
// en vez de depender de que el socket timeout salve la situación.
const PRESUPUESTO_TIEMPO_MS = 240000;

// Peor caso real de UN LOTE completo (5 folios en paralelo — el lote tarda lo que tarde el
// más lento de los 5): un folio individual puede recibir 429 hasta
// (CONSULTAR_FOLIOS_MAX_INTENTOS_429 - 1) veces antes de agotar sus intentos, cada espera
// topada en 60s (ver _consultarYPersistirFolio), más una estimación conservadora de 5s de
// red por cada intento real contra Kore. Se resta de PRESUPUESTO_TIEMPO_MS ANTES de decidir
// si lanzar un lote nuevo (no alcanza con comparar solo el tiempo YA transcurrido: eso
// dejaría arrancar un lote que, en su propio peor caso, todavía se pasa del presupuesto).
const PEOR_CASO_UN_LOTE_MS =
  (CONSULTAR_FOLIOS_MAX_INTENTOS_429 - 1) * 60000 + CONSULTAR_FOLIOS_MAX_INTENTOS_429 * 5000;

// Motivo textual — mismos 7 valores del enum NetpayMatch.motivoDiscrepancia (ver
// NetpayMatch.model.js) para la hoja "Bandeja" del Excel.
const MOTIVO_LABELS = {
  sin_candidato:          'Sin candidato',
  multiples_candidatos:   'Múltiples candidatos',
  candidato_en_conflicto: 'Candidato en conflicto',
  cobertura_parcial:      'Cobertura parcial',
  revertido:              'Revertido',
  reporte_revertido:      'Reporte revertido',
  vinculo_huerfano:       'Vínculo huérfano',
};

function _sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Guard in-memory contra doble disparo concurrente del MISMO filtro (hallazgo de revisión:
// doble clic en "Exportar Excel", o dos pestañas, duplicaba la carga real contra Kore justo
// cuando ya podía haber presión de 429). No hace falta nada externo (Redis, etc.) — un solo
// proceso Node atiende estos requests, mismo criterio que otros guards in-process del
// dominio (ver ej. bank.service.js "Concurrent link guard").
const _exportacionesEnCurso = new Set();

// Clave estable por filtro — NO se usa JSON.stringify(filtro) tal cual porque filtro.dia
// contiene objetos Date (y $gte/$lte pueden faltar) cuyo orden de claves no está garantizado
// entre dos llamadas equivalentes; se arma a mano con los 4 campos posibles, siempre en el
// mismo orden.
function _claveFiltro(filtro) {
  return [
    filtro.dia?.$gte ? new Date(filtro.dia.$gte).toISOString() : '',
    filtro.dia?.$lte ? new Date(filtro.dia.$lte).toISOString() : '',
    filtro.terminalID ?? '',
    filtro.estatusMatch ?? '',
  ].join('|');
}

// Consulta puntual a Kore para UN folio de UN bucket, con reintento ante 429 (mismo
// backoff que netpay-reporte.service.js#_consultarFolioConReintento). A diferencia de
// consultarFolioKore (que recarga y re-guarda el NetpayReporte completo), acá se persiste
// con un $set posicional directo por índice — varios folios del MISMO bucket pueden
// resolverse en paralelo dentro del mismo lote, y un save() de documento completo pisaría
// los cambios de los demás.
async function _consultarYPersistirFolio(bucketId, folioIdx, referencia) {
  for (let intento = 1; intento <= CONSULTAR_FOLIOS_MAX_INTENTOS_429; intento++) {
    try {
      const { raw } = await buscarTransaccionesNetpay({ folio: referencia, withAccountInfo: true, status: 'completed' });
      const transacciones = raw?.Data?.transactions ?? [];
      const tx = transacciones.find(t => String(t.folio) === String(referencia)) ?? transacciones[0] ?? null;
      const cuenta = tx?.cuentas?.[0] ?? null;

      if (!cuenta) throw new Error(`No se encontró la cuenta en Kore para el folio ${referencia}`);

      const consultadoEn = new Date();
      await NetpayMatch.findByIdAndUpdate(bucketId, {
        $set: { [`snapshot.folios.${folioIdx}.koreCache`]: { consultadoEn, cuenta } },
      });
      return;
    } catch (err) {
      if (err?.statusCode === 429 && intento < CONSULTAR_FOLIOS_MAX_INTENTOS_429) {
        const dataMsg   = String(err?.koreBody?.Data ?? '');
        const match     = /retry after:\s*([\d.]+)/i.exec(dataMsg);
        const esperaSeg = Math.min((match ? Number(match[1]) : 10) + 1, 60);
        logger.warn(`[NetpayMatchExport] 429 de Kore al consultar folio ${referencia} (bucket=${bucketId}), reintentando en ${esperaSeg.toFixed(1)}s (intento ${intento}/${CONSULTAR_FOLIOS_MAX_INTENTOS_429})`);
        await _sleep(esperaSeg * 1000);
        continue;
      }
      throw err;
    }
  }
}

// consultarFoliosPendientesDeBandeja — versión "bulk" de
// netpay-reporte.service.js#consultarFoliosPendientes: aplana los folios sin
// koreCache.cuenta de TODOS los buckets que matchean `filtro` (mismo shape que ya arma
// GET /netpay/bandeja) y los consulta en lotes. Fallo parcial, nunca todo-o-nada — un
// folio individual que falle se loguea y se acumula en `fallos`, el resto sigue.
async function consultarFoliosPendientesDeBandeja(filtro) {
  const clave = _claveFiltro(filtro);
  if (_exportacionesEnCurso.has(clave)) {
    throw new ConflictError('Ya hay una exportación en curso para este filtro, esperá a que termine.');
  }
  _exportacionesEnCurso.add(clave);

  try {
    const buckets = await NetpayMatch.find(filtro).lean();

    const pendientes = [];
    for (const bucket of buckets) {
      (bucket.snapshot?.folios ?? []).forEach((folio, folioIdx) => {
        if (folio.referencia && !folio.koreCache?.cuenta) {
          pendientes.push({ bucketId: bucket._id, folioIdx, referencia: folio.referencia });
        }
      });
    }

    if (pendientes.length > MAX_FOLIOS_PENDIENTES_EXPORT) {
      throw new UnprocessableError(
        `Hay ${pendientes.length} folios sin consultar en Kore dentro de este rango (máximo ${MAX_FOLIOS_PENDIENTES_EXPORT}). Acotá el filtro de fechas/terminal/estado e intentá de nuevo.`,
      );
    }

    const fallos = [];
    const omitidosPorTiempo = [];
    const inicio = Date.now();
    for (let i = 0; i < pendientes.length; i += CONSULTAR_FOLIOS_CONCURRENCIA) {
      // Corte de presupuesto ANTES de lanzar el próximo lote — nunca a mitad de un lote ya
      // en vuelo (esas llamadas siguen hasta resolverse solas, solo dejamos de lanzar lotes
      // nuevos). Los folios de acá en adelante quedan en omitidosPorTiempo, distinto de
      // fallos (esos SÍ se intentaron contra Kore y fallaron de verdad).
      if (Date.now() - inicio + PEOR_CASO_UN_LOTE_MS > PRESUPUESTO_TIEMPO_MS) {
        omitidosPorTiempo.push(...pendientes.slice(i).map(p => ({ referencia: p.referencia })));
        break;
      }

      const lote = pendientes.slice(i, i + CONSULTAR_FOLIOS_CONCURRENCIA);
      const resultadosLote = await Promise.allSettled(
        lote.map(p => _consultarYPersistirFolio(p.bucketId, p.folioIdx, p.referencia)),
      );
      resultadosLote.forEach((r, idx) => {
        if (r.status === 'rejected') {
          const p = lote[idx];
          logger.warn(`[NetpayMatchExport] no se pudo consultar Kore para el folio ${p.referencia} (bucket=${p.bucketId}): ${r.reason.message}`);
          fallos.push({ referencia: p.referencia, error: r.reason.message });
        }
      });
      const esUltimoLote = i + CONSULTAR_FOLIOS_CONCURRENCIA >= pendientes.length;
      if (!esUltimoLote) await _sleep(CONSULTAR_FOLIOS_PAUSA_MS);
    }

    return {
      consultados: pendientes.length - fallos.length - omitidosPorTiempo.length,
      fallos,
      omitidosPorTiempo,
    };
  } finally {
    // SIEMPRE libera la marca — éxito, error (incluido el 422 del techo de 300), o corte
    // por presupuesto de tiempo caen todos acá.
    _exportacionesEnCurso.delete(clave);
  }
}

// Mismo criterio EXACTO que netpay-panel.component.ts#quienCuando — solo 3 de los 6
// estados tienen "quién/cuándo", el resto no aplica.
function _quienCuando(b) {
  if (b.estatusMatch === 'confirmado_automatico' && b.confirmadoPor) {
    return { nombre: b.confirmadoPor.nombre, en: b.confirmadoEn };
  }
  if (b.estatusMatch === 'resuelto_manual' && b.resueltoManualPor) {
    return { nombre: b.resueltoManualPor.nombre, en: b.resueltoManualEn };
  }
  if (b.estatusMatch === 'rechazado' && b.descartadoManualmentePor) {
    return { nombre: b.descartadoManualmentePor.nombre, en: b.descartadoManualmenteEn };
  }
  return null;
}

function _movimientosVinculados(movimientos) {
  if (!movimientos || movimientos.length === 0) return '—';
  return movimientos.map(m => {
    const fecha = _formatFecha(m.fecha) ?? '—';
    const monto = m.deposito != null ? Number(m.deposito).toFixed(2) : '—';
    return `${fecha} · $${monto} · ${m.banco ?? '—'} · ${m.numeroAutorizacion ?? '—'}`;
  }).join('; ');
}

// Mismo estilo institucional que netpay-reporte-export.service.js (header oscuro, filas
// pares, formato numérico) — factorizado acá porque este archivo tiene 2 hojas, no 1.
function _estilizarHoja(sheet, columnas, numColKeys) {
  const headerRow = sheet.getRow(1);
  headerRow.font = { bold: true, color: { argb: 'FFE0E7FF' } };
  headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E1B4B' } };

  const evenFill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF8FAFF' } };
  sheet.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return;
    const isEven = rowNumber % 2 === 0;
    columnas.forEach((col, idx) => {
      const cell = row.getCell(idx + 1);
      if (isEven) cell.fill = evenFill;
      if (numColKeys.has(col.key) && cell.value != null) cell.numFmt = '#,##0.00';
    });
  });
}

async function generarExcelBandejaNetpay(buckets) {
  const workbook = new ExcelJS.Workbook();

  // ── Hoja "Bandeja" — una fila por bucket ──────────────────────────────────
  const sheetBandeja = workbook.addWorksheet('Bandeja');
  const columnasBandeja = [
    { header: 'Terminal ID',                 key: 'terminalID',    width: 16 },
    { header: 'Almacén',                     key: 'almacen',       width: 12 },
    { header: 'Día',                         key: 'dia',           width: 13 },
    { header: 'Bucket',                      key: 'bucket',        width: 12 },
    { header: 'Neto Esperado',               key: 'netoEsperado',  width: 15 },
    { header: 'Estado',                      key: 'estado',        width: 20 },
    { header: 'Motivo',                      key: 'motivo',        width: 22 },
    { header: 'Movimiento(s) vinculado(s)',  key: 'movimientos',   width: 46 },
    { header: 'Quién',                       key: 'quien',         width: 22 },
    { header: 'Cuándo',                      key: 'cuando',        width: 16 },
    { header: 'Justificación',               key: 'justificacion', width: 30 },
    { header: 'Motivo de rechazo',           key: 'rechazoMotivo', width: 30 },
  ];
  sheetBandeja.columns = columnasBandeja;

  for (const b of buckets) {
    const qc = _quienCuando(b);
    sheetBandeja.addRow({
      terminalID:    b.terminalID,
      almacen:       b.almacen ?? '—',
      dia:           _formatFecha(b.dia),
      bucket:        b.bucket,
      netoEsperado:  b.netoEsperado,
      estado:        STATUS_LABELS[b.estatusMatch] ?? b.estatusMatch,
      motivo:        MOTIVO_LABELS[b.motivoDiscrepancia] ?? (b.motivoDiscrepancia ?? '—'),
      movimientos:   _movimientosVinculados(b.movementIdsConfirmados),
      quien:         qc?.nombre ?? '—',
      cuando:        qc ? _formatFecha(qc.en) : '—',
      justificacion: b.justificacion ?? '—',
      rechazoMotivo: b.rechazoMotivo ?? '—',
    });
  }
  _estilizarHoja(sheetBandeja, columnasBandeja, new Set(['netoEsperado']));

  // ── Hoja "Folios" — aplanado de snapshot.folios[] de todos los buckets ────
  const sheetFolios = workbook.addWorksheet('Folios');
  const columnasFolios = [
    { header: 'Referencia',          key: 'referencia',      width: 18 },
    { header: 'Terminal ID',         key: 'terminalID',      width: 16 },
    { header: 'Día',                 key: 'dia',             width: 13 },
    { header: 'Bucket',              key: 'bucket',          width: 12 },
    { header: 'Marca',               key: 'marca',           width: 12 },
    { header: 'Monto',               key: 'monto',           width: 14 },
    { header: 'Comisión',            key: 'comision',        width: 14 },
    { header: 'Pedido (Kore)',       key: 'serieFolioKore',  width: 16 },
    { header: 'Folio Fiscal (Kore)', key: 'folioFiscalKore', width: 18 },
    { header: 'Tipo de Pago (Kore)', key: 'tipoPagoKore',    width: 14 },
    { header: 'Subtotal (Kore)',     key: 'subtotalKore',    width: 14 },
    { header: 'Impuesto (Kore)',     key: 'impuestoKore',    width: 14 },
    { header: 'Total (Kore)',        key: 'totalKore',       width: 14 },
    { header: 'Almacén (Kore)',      key: 'almacenKore',     width: 14 },
  ];
  sheetFolios.columns = columnasFolios;

  for (const b of buckets) {
    for (const f of (b.snapshot?.folios ?? [])) {
      sheetFolios.addRow({
        referencia: f.referencia,
        terminalID: b.terminalID,
        dia:        _formatFecha(b.dia),
        bucket:     b.bucket,
        marca:      f.marca,
        monto:      f.monto,
        comision:   f.comision,
        ..._koreCacheColumnas(f),
      });
    }
  }
  _estilizarHoja(sheetFolios, columnasFolios, new Set(['monto', 'comision', 'subtotalKore', 'impuestoKore', 'totalKore']));

  return workbook.xlsx.writeBuffer();
}

module.exports = {
  consultarFoliosPendientesDeBandeja,
  generarExcelBandejaNetpay,
  MAX_FOLIOS_PENDIENTES_EXPORT,
};
