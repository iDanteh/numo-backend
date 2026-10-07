'use strict';

// netpay-reporte-export.service.js — genera el Excel de respaldo/auditoría de un
// NetpayReporte ya cargado. Mismo patrón de estilos que bank.service.js#exportMovements
// (ExcelJS, header oscuro, filas alternadas, formato numérico) — un solo reporte por
// archivo (a diferencia de exportMovements, que exporta un listado filtrado).

const ExcelJS = require('exceljs');

// v2 (fix 2026-09-29): reemplaza el viejo ['pendiente','confirmado','descartado'] —
// este archivo no formaba parte de netpay-matching-v2 (no está en tasks.md ni en el
// File Changes de design.md) y quedó mostrando el valor crudo del enum nuevo sin
// mapear. Mismo enum que NetpayReporte.model.js#estatus.
const STATUS_LABELS = {
  confirmado_automatico: 'Confirmado automático',
  pendiente_por_marca:   'Pendiente por marca',
  discrepancia:           'Discrepancia',
  resuelto_por_reporte:   'Resuelto por reporte',
  rechazado:              'Rechazado',
  resuelto_manual:        'Resuelto manual',
};

function _formatFecha(raw) {
  if (!raw) return null;
  const d = new Date(raw);
  if (isNaN(d.getTime())) return null;
  return `${String(d.getUTCDate()).padStart(2, '0')}/${String(d.getUTCMonth() + 1).padStart(2, '0')}/${d.getUTCFullYear()}`;
}

// _koreCacheColumnas — mismos campos crudos que el panel muestra en el dropdown de
// desglose (netpay-reporte-panel.component.html: SerieExterna/FolioExterno,
// FolioFiscal, TipoPago, Subtotal, Impuesto, Total, Almacen). Si el folio nunca se
// consultó (koreCache:null), devuelve todo en null — nunca pega a Kore acá.
function _koreCacheColumnas(f) {
  const cuenta = f.koreCache?.cuenta;
  if (!cuenta) {
    return {
      serieFolioKore: null, folioFiscalKore: null, tipoPagoKore: null,
      subtotalKore: null, impuestoKore: null, totalKore: null, almacenKore: null,
    };
  }
  const serie = cuenta.SerieExterna || null;
  const folioExt = cuenta.FolioExterno || null;
  return {
    serieFolioKore: (serie || folioExt) ? `${serie ?? '—'}-${folioExt ?? '—'}` : null,
    folioFiscalKore: cuenta.FolioFiscal ?? null,
    tipoPagoKore: cuenta.TipoPago ?? null,
    subtotalKore: cuenta.Subtotal ?? null,
    impuestoKore: cuenta.Impuesto ?? null,
    totalKore: cuenta.Total ?? null,
    almacenKore: cuenta.Almacen ?? null,
  };
}

async function generarExcelReporteNetpay(reporte) {
  const workbook = new ExcelJS.Workbook();

  // ── Hoja "Resumen" ─────────────────────────────────────────────────────────
  const sheetResumen = workbook.addWorksheet('Resumen');
  sheetResumen.columns = [
    { header: 'Campo', key: 'campo', width: 28 },
    { header: 'Valor', key: 'valor', width: 34 },
  ];
  const filasResumen = [
    ['Clave Rastreo', reporte.claveRastreo],
    ['Cuenta Depósito', reporte.cuentaDeposito],
    ['Fecha de movimiento', _formatFecha(reporte.fechaMovimiento)],
    ['Periodo desde', _formatFecha(reporte.periodoDesde)],
    ['Periodo hasta', _formatFecha(reporte.periodoHasta)],
    ['Monto depósito total', reporte.montoDepositoTotal],
    ['Monto transaccionado', reporte.resumenVentas?.montoTransaccionado ?? null],
    ['Comisiones', reporte.resumenVentas?.comisiones ?? null],
    ['IVA', reporte.resumenVentas?.iva ?? null],
    ['Monto depositado (ventas)', reporte.resumenVentas?.montoDepositado ?? null],
    ['Estatus', STATUS_LABELS[reporte.estatus] ?? reporte.estatus],
    ['Cargado por', reporte.cargadoPor?.nombre ?? null],
    ['Cargado en', _formatFecha(reporte.cargadoEn)],
    ['Confirmado por', reporte.confirmadoPor?.nombre ?? null],
    ['Confirmado en', _formatFecha(reporte.confirmadoEn)],
    ['Descartado por', reporte.descartadoPor?.nombre ?? null],
    ['Descartado en', _formatFecha(reporte.descartadoEn)],
    ['Motivo de descarte', reporte.descartadoMotivo ?? null],
    ['Archivo original', reporte.nombreArchivoOriginal ?? null],
  ];
  for (const [campo, valor] of filasResumen) sheetResumen.addRow({ campo, valor });
  sheetResumen.getRow(1).font = { bold: true, color: { argb: 'FFE0E7FF' } };
  sheetResumen.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E1B4B' } };

  // ── Hoja "Folios" ──────────────────────────────────────────────────────────
  const sheet = workbook.addWorksheet('Folios');
  sheet.columns = COLUMNAS_FOLIOS;
  for (const f of (reporte.folios ?? [])) sheet.addRow(_filaFolio(f));
  _estilizarHoja(sheet, COLUMNAS_FOLIOS, NUM_COL_KEYS_FOLIOS);

  return workbook.xlsx.writeBuffer();
}

// Columnas + mapeo de fila de la hoja "Folios", compartidos entre generarExcelReporteNetpay
// (1 reporte) y generarExcelReportesNetpay (N reportes, ver abajo) — antes vivían duplicados
// casi íntegros en ambas funciones (~70 líneas idénticas, incluido el desglose de
// folios[].koreCache del fix 2026-09-29); factorizado acá tras hallazgo de revisión de
// legibilidad (2026-10-07). generarExcelReportesNetpay solo necesita anteponer su propia
// columna extra "Clave Rastreo (Depósito)".
const COLUMNAS_FOLIOS = [
  { header: 'Referencia',            key: 'referencia',         width: 18 },
  { header: 'Terminal ID',           key: 'terminalID',         width: 16 },
  { header: 'Store ID',              key: 'storeId',            width: 14 },
  { header: 'Sucursal',              key: 'sucursal',           width: 26 },
  { header: 'Nombre Empresa',        key: 'nombreEmpresa',      width: 26 },
  { header: 'Fecha Trx',             key: 'fechaTrx',           width: 13 },
  { header: 'Hora Trx',              key: 'horaTrx',            width: 10 },
  { header: 'Monto Trx',             key: 'montoTrx',           width: 14 },
  { header: 'Comisión Base %',       key: 'comisionBasePct',    width: 15 },
  { header: 'Comisión Base $',       key: 'comisionBaseMonto',  width: 15 },
  { header: 'IVA Comisión',          key: 'ivaComision',        width: 14 },
  { header: 'Comisión + IVA',        key: 'comisionMasIva',     width: 15 },
  { header: 'Monto Depósito',        key: 'montoDeposito',      width: 15 },
  { header: 'Banco',                 key: 'banco',              width: 16 },
  { header: 'Tipo de Tarjeta',       key: 'tipoTarjeta',        width: 14 },
  { header: 'Código Autorización',   key: 'codigoAutorizacion', width: 18 },
  { header: 'Order ID',              key: 'orderId',            width: 30 },
  // Desglose de folios[].koreCache (fix 2026-09-29, pedido del usuario): solo se
  // llena para folios YA consultados manualmente en el panel (consultarFolioKore) —
  // nunca se pega a Kore al exportar, ver _koreCacheColumnas().
  { header: 'Pedido (Kore)',           key: 'serieFolioKore',   width: 16 },
  { header: 'Folio Fiscal (Kore)',     key: 'folioFiscalKore',  width: 18 },
  { header: 'Tipo de Pago (Kore)',     key: 'tipoPagoKore',     width: 14 },
  { header: 'Subtotal (Kore)',         key: 'subtotalKore',     width: 14 },
  { header: 'Impuesto (Kore)',         key: 'impuestoKore',     width: 14 },
  { header: 'Total (Kore)',            key: 'totalKore',        width: 14 },
  { header: 'Almacén (Kore)',          key: 'almacenKore',      width: 14 },
];

const NUM_COL_KEYS_FOLIOS = new Set([
  'montoTrx', 'comisionBaseMonto', 'ivaComision', 'comisionMasIva', 'montoDeposito',
  'subtotalKore', 'impuestoKore', 'totalKore',
]);

function _filaFolio(f) {
  return {
    referencia:         f.referencia,
    terminalID:         f.terminalID,
    storeId:            f.storeId,
    sucursal:           f.sucursal,
    nombreEmpresa:      f.nombreEmpresa,
    fechaTrx:           _formatFecha(f.fechaTrx),
    horaTrx:            f.horaTrx,
    montoTrx:           f.montoTrx,
    comisionBasePct:    f.comisionBasePct,
    comisionBaseMonto:  f.comisionBaseMonto,
    ivaComision:        f.ivaComision,
    comisionMasIva:     f.comisionMasIva,
    montoDeposito:      f.montoDeposito,
    banco:              f.banco,
    tipoTarjeta:        f.tipoTarjeta,
    codigoAutorizacion: f.codigoAutorizacion,
    orderId:            f.orderId,
    ..._koreCacheColumnas(f),
  };
}

// Mismo estilo institucional (header oscuro, filas pares, formato numérico) que usa
// netpay-match-export.service.js#generarExcelBandejaNetpay — ESA es la única definición
// (se exporta de acá, netpay-match-export.service.js la reusa) en vez de mantener 2 copias
// idénticas; no al revés, porque ese archivo ya depende de este (STATUS_LABELS/_formatFecha/
// _koreCacheColumnas) y la dirección opuesta crearía un require circular (corrección de
// revisión de legibilidad, 2026-10-07 — la copia duplicada original decía, incorrectamente,
// que no se podía reusar por no ser "dependencia de este archivo").
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

// generarExcelReportesNetpay — netpay-reporte export-lote (2026-10-07, pedido explícito del
// usuario): "excel general" con TODOS los depósitos de UN MISMO archivo recién cargado (no
// toda la colección NetpayReporte) — mismo criterio de 2 hojas que
// netpay-match-export.service.js#generarExcelBandejaNetpay (ahí la unidad es un bucket
// NetpayMatch, acá un NetpayReporte completo), reusando las mismas columnas/estilo que
// generarExcelReporteNetpay ya usa para UN solo reporte.
async function generarExcelReportesNetpay(reportes) {
  const workbook = new ExcelJS.Workbook();

  // ── Hoja "Depósitos" — versión tabular de la hoja "Resumen" individual, 1 fila por reporte ──
  const sheetDepositos = workbook.addWorksheet('Depósitos');
  const columnasDepositos = [
    { header: 'Clave Rastreo',             key: 'claveRastreo',        width: 20 },
    { header: 'Cuenta Depósito',           key: 'cuentaDeposito',      width: 18 },
    { header: 'Fecha de movimiento',       key: 'fechaMovimiento',     width: 16 },
    { header: 'Monto Depósito Total',      key: 'montoDepositoTotal',  width: 18 },
    { header: 'Monto Transaccionado',      key: 'montoTransaccionado', width: 18 },
    { header: 'Comisiones',                key: 'comisiones',          width: 14 },
    { header: 'IVA',                       key: 'iva',                 width: 12 },
    { header: 'Monto Depositado (Ventas)', key: 'montoDepositado',     width: 18 },
    { header: 'Estatus',                   key: 'estatus',             width: 20 },
    { header: 'Archivo original',          key: 'archivo',             width: 26 },
  ];
  sheetDepositos.columns = columnasDepositos;
  for (const r of reportes) {
    sheetDepositos.addRow({
      claveRastreo:        r.claveRastreo,
      cuentaDeposito:      r.cuentaDeposito,
      fechaMovimiento:     _formatFecha(r.fechaMovimiento),
      montoDepositoTotal:  r.montoDepositoTotal,
      montoTransaccionado: r.resumenVentas?.montoTransaccionado ?? null,
      comisiones:          r.resumenVentas?.comisiones ?? null,
      iva:                 r.resumenVentas?.iva ?? null,
      montoDepositado:     r.resumenVentas?.montoDepositado ?? null,
      estatus:             STATUS_LABELS[r.estatus] ?? r.estatus,
      archivo:             r.nombreArchivoOriginal ?? null,
    });
  }
  _estilizarHoja(sheetDepositos, columnasDepositos, new Set(['montoDepositoTotal', 'montoTransaccionado', 'comisiones', 'iva', 'montoDepositado']));

  // ── Hoja "Folios" — aplanado de folios[] de TODOS los reportes, con columna de depósito ──
  // Mismas columnas/mapeo que generarExcelReporteNetpay (COLUMNAS_FOLIOS/_filaFolio arriba),
  // solo se antepone la columna "Clave Rastreo (Depósito)" para distinguir de qué reporte es
  // cada folio.
  const sheetFolios = workbook.addWorksheet('Folios');
  const columnasFolios = [
    { header: 'Clave Rastreo (Depósito)', key: 'claveRastreo', width: 20 },
    ...COLUMNAS_FOLIOS,
  ];
  sheetFolios.columns = columnasFolios;
  for (const r of reportes) {
    for (const f of (r.folios ?? [])) {
      sheetFolios.addRow({ claveRastreo: r.claveRastreo, ..._filaFolio(f) });
    }
  }
  _estilizarHoja(sheetFolios, columnasFolios, NUM_COL_KEYS_FOLIOS);

  return workbook.xlsx.writeBuffer();
}

// STATUS_LABELS/_formatFecha/_koreCacheColumnas/_estilizarHoja también los usa
// netpay-match-export.service.js (export de la bandeja de matching) — mismo enum/shape de
// koreCache y mismo estilo institucional, no vale la pena duplicarlos.
module.exports = {
  generarExcelReporteNetpay, generarExcelReportesNetpay,
  STATUS_LABELS, _formatFecha, _koreCacheColumnas, _estilizarHoja,
};
