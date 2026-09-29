'use strict';

// netpay-reporte-export.service.test.js — cobertura MÍNIMA para 2 huecos reales
// encontrados al probar netpay-matching-v2 contra datos reales (2026-09-29):
// (1) STATUS_LABELS seguía con el enum viejo ['pendiente','confirmado','descartado'],
//     desactualizado desde que NetpayReporte.model.js pasó al enum v2 de 6 estados —
//     un reporte real mostraba su valor crudo (p.ej. "confirmado_automatico") en vez
//     de una etiqueta legible; (2) la hoja "Folios" nunca incluyó el desglose que
//     consultarFolioKore() cachea en folios[].koreCache — el usuario pidió que la
//     descarga refleje el mismo desglose (serie/folio, folio fiscal, etc.) que ya se
//     ve en el panel al consultar un folio puntual, solo para los folios YA
//     consultados (sin pegarle a Kore en el momento de exportar).
// generarExcelReporteNetpay() en sí NO tenía cobertura previa en todo el proyecto —
// este archivo cubre solo lo nuevo, no la función completa.
const ExcelJS = require('exceljs');
const { generarExcelReporteNetpay } = require('./netpay-reporte-export.service');

function fakeFolio(overrides = {}) {
  return {
    referencia: 'F-001', terminalID: '2840403056', storeId: 'S1', sucursal: 'Oaxaca 02',
    nombreEmpresa: 'TYC', fechaTrx: new Date('2026-09-24T00:00:00.000Z'), horaTrx: '10:00',
    montoTrx: 1000, comisionBasePct: 1.59, comisionBaseMonto: 15.9, ivaComision: 2.54,
    comisionMasIva: 18.44, montoDeposito: 981.56, banco: 'BBVA', tipoTarjeta: 'Débito',
    codigoAutorizacion: '868186', orderId: 'F20260922-00163', koreCache: null,
    ...overrides,
  };
}

function fakeReporte(overrides = {}) {
  return {
    claveRastreo: '42644264202609255810846530', cuentaDeposito: '012345',
    fechaMovimiento: new Date('2026-09-25'), periodoDesde: null, periodoHasta: null,
    montoDepositoTotal: 981.56, resumenVentas: {}, estatus: 'confirmado_automatico',
    cargadoPor: null, cargadoEn: null, confirmadoPor: null, confirmadoEn: null,
    descartadoPor: null, descartadoEn: null, descartadoMotivo: null,
    nombreArchivoOriginal: 'TYC02.xlsx', folios: [fakeFolio()],
    ...overrides,
  };
}

async function leerWorkbook(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  return wb;
}

// ExcelJS no persiste worksheet.columns[].key al releer un buffer ya escrito (`key`
// es solo un helper de escritura, no forma parte del xlsx real) — mapeamos por texto
// de encabezado en la fila 1 en su lugar.
const HEADERS_A_KEYS = {
  'Serie / Folio (Kore)':  'serieFolioKore',
  'Folio Fiscal (Kore)':   'folioFiscalKore',
  'Tipo de Pago (Kore)':   'tipoPagoKore',
  'Subtotal (Kore)':       'subtotalKore',
  'Impuesto (Kore)':       'impuestoKore',
  'Total (Kore)':          'totalKore',
  'Almacén (Kore)':        'almacenKore',
};

function filaComoObjeto(ws, rowNumber) {
  const headerRow = ws.getRow(1);
  const row = ws.getRow(rowNumber);
  const obj = {};
  headerRow.eachCell((cell, colNumber) => {
    const key = HEADERS_A_KEYS[cell.value];
    if (key) obj[key] = row.getCell(colNumber).value;
  });
  return obj;
}

describe('generarExcelReporteNetpay — labels v2 (fix 2026-09-29)', () => {
  test('hoja Resumen: estatus v2 (p.ej. confirmado_automatico) muestra una etiqueta legible, no el valor crudo', async () => {
    const buffer = await generarExcelReporteNetpay(fakeReporte({ estatus: 'confirmado_automatico' }));
    const wb = await leerWorkbook(buffer);
    const sheetResumen = wb.getWorksheet('Resumen');

    const filaEstatus = sheetResumen.getRows(1, sheetResumen.rowCount)
      .find(r => r.getCell(1).value === 'Estatus');

    expect(filaEstatus.getCell(2).value).not.toBe('confirmado_automatico');
    expect(typeof filaEstatus.getCell(2).value).toBe('string');
    expect(filaEstatus.getCell(2).value.toLowerCase()).not.toContain('_');
  });

  test.each(['pendiente_por_marca', 'discrepancia', 'resuelto_por_reporte', 'rechazado', 'resuelto_manual'])(
    'hoja Resumen: estatus "%s" también tiene una etiqueta legible (no undefined, no snake_case)',
    async (estatus) => {
      const buffer = await generarExcelReporteNetpay(fakeReporte({ estatus }));
      const wb = await leerWorkbook(buffer);
      const sheetResumen = wb.getWorksheet('Resumen');
      const filaEstatus = sheetResumen.getRows(1, sheetResumen.rowCount)
        .find(r => r.getCell(1).value === 'Estatus');

      expect(filaEstatus.getCell(2).value).toBeTruthy();
      expect(String(filaEstatus.getCell(2).value)).not.toContain('_');
    },
  );
});

describe('generarExcelReporteNetpay — desglose de koreCache en hoja Folios (2026-09-29)', () => {
  test('folio con koreCache.cuenta ya consultado: la fila incluye serie/folio, folio fiscal, tipo de pago, subtotal/impuesto/total y almacén', async () => {
    const cuenta = {
      SerieExterna: 'A', FolioExterno: '163', FolioFiscal: 'FF-9182',
      TipoPago: 'PUE', Subtotal: 850.0, Impuesto: 136.0, Total: 986.0, Almacen: 'M0',
    };
    const reporte = fakeReporte({
      folios: [fakeFolio({ referencia: 'F-001', koreCache: { consultadoEn: new Date('2026-09-26T10:00:00.000Z'), cuenta } })],
    });

    const buffer = await generarExcelReporteNetpay(reporte);
    const wb = await leerWorkbook(buffer);
    const sheet = wb.getWorksheet('Folios');
    const fila = filaComoObjeto(sheet,2);

    expect(fila.serieFolioKore).toBe('A-163');
    expect(fila.folioFiscalKore).toBe('FF-9182');
    expect(fila.tipoPagoKore).toBe('PUE');
    expect(fila.subtotalKore).toBe(850.0);
    expect(fila.impuestoKore).toBe(136.0);
    expect(fila.totalKore).toBe(986.0);
    expect(fila.almacenKore).toBe('M0');
  });

  test('folio SIN koreCache (nunca consultado): las columnas de desglose Kore quedan vacías, no rompe el export', async () => {
    const reporte = fakeReporte({ folios: [fakeFolio({ referencia: 'F-002', koreCache: null })] });

    const buffer = await generarExcelReporteNetpay(reporte);
    const wb = await leerWorkbook(buffer);
    const sheet = wb.getWorksheet('Folios');
    const fila = filaComoObjeto(sheet,2);

    expect(fila.serieFolioKore == null).toBe(true);
    expect(fila.folioFiscalKore == null).toBe(true);
    expect(fila.tipoPagoKore == null).toBe(true);
    expect(fila.subtotalKore == null).toBe(true);
    expect(fila.impuestoKore == null).toBe(true);
    expect(fila.totalKore == null).toBe(true);
    expect(fila.almacenKore == null).toBe(true);
  });

  test('reporte con 2 folios, uno consultado y otro no: cada fila refleja su propio estado de cache de forma independiente', async () => {
    const cuenta = { SerieExterna: 'B', FolioExterno: '200', FolioFiscal: 'FF-1', TipoPago: 'PPD', Subtotal: 100, Impuesto: 16, Total: 116, Almacen: 'M1' };
    const reporte = fakeReporte({
      folios: [
        fakeFolio({ referencia: 'F-A', koreCache: { consultadoEn: new Date(), cuenta } }),
        fakeFolio({ referencia: 'F-B', koreCache: null }),
      ],
    });

    const buffer = await generarExcelReporteNetpay(reporte);
    const wb = await leerWorkbook(buffer);
    const sheet = wb.getWorksheet('Folios');

    const filaA = filaComoObjeto(sheet,2);
    const filaB = filaComoObjeto(sheet,3);

    expect(filaA.folioFiscalKore).toBe('FF-1');
    expect(filaB.folioFiscalKore == null).toBe(true);
  });
});
