'use strict';

// netpay-reporte-export.service.js — genera el Excel de respaldo/auditoría de un
// NetpayReporte ya cargado. Mismo patrón de estilos que bank.service.js#exportMovements
// (ExcelJS, header oscuro, filas alternadas, formato numérico) — un solo reporte por
// archivo (a diferencia de exportMovements, que exporta un listado filtrado).

const ExcelJS = require('exceljs');

const STATUS_LABELS = {
  pendiente:  'Pendiente',
  confirmado: 'Confirmado',
  descartado: 'Descartado',
};

function _formatFecha(raw) {
  if (!raw) return null;
  const d = new Date(raw);
  if (isNaN(d.getTime())) return null;
  return `${String(d.getUTCDate()).padStart(2, '0')}/${String(d.getUTCMonth() + 1).padStart(2, '0')}/${d.getUTCFullYear()}`;
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
  const columnas = [
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
  ];
  sheet.columns = columnas;

  for (const f of (reporte.folios ?? [])) {
    sheet.addRow({
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
    });
  }

  const headerRow = sheet.getRow(1);
  headerRow.font = { bold: true, color: { argb: 'FFE0E7FF' } };
  headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E1B4B' } };

  const numColKeys = new Set([
    'montoTrx', 'comisionBaseMonto', 'ivaComision', 'comisionMasIva', 'montoDeposito',
  ]);
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

  return workbook.xlsx.writeBuffer();
}

module.exports = { generarExcelReporteNetpay };
