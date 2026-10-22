'use strict';

// Hoja "MOSTRADOR" (2026-10-21, PDF del usuario "MOSTRADOR 20 OCT 2026"):
// depósitos de Bancos de una factura de mostrador que quedó "Venta Sin Cobro".
// Datos reales: factura A0-261009872 (CEDIS 20-oct), 2 de sus 17 depósitos.
jest.mock('../../../shared/models/postgres', () => ({
  AccountPlan: {}, Terminal: {}, CfdiMappingRule: {}, PolizaMovimiento: {}, Poliza: {}, CentroCosto: {},
}));

const { _construirWorkbookPoliza } = require('./poliza.service');

const poliza = {
  tipo: 'I', numero: 192, fecha: '2026-10-20', concepto: 'Ingresos por Ventas Suc. CEDIS', ejercicio: 2026, periodo: 10,
  movimientosMostrador: [{
    uuid: '1C02D46C-FF4C-411E-BF11-7960AB39BA9C', serie: 'A0', folio: '261009872',
    fechaFactura: '2026-10-20T17:22:35.000Z', totalFactura: 229702.95, totalAplicado: 17081.6,
    depositos: [
      { fecha: '2026-10-14T12:00:00.000Z', descripcion: 'DEPOSITO EN EFECTIVO', importe: 11580.53, aplicado: 11580.53, banco: 'BBVA',
        remisiones: ['A0-260901786', 'A0-260901831'], cliente: 'JUAN LOPEZ MORALES', idNumo: '052586', estatus: 'POR FACTURAR' },
      { fecha: '2026-10-13T00:00:00.000Z', descripcion: 'DEPOSITO EN EFECTIVO ATM', importe: 5000, aplicado: 4999.74, banco: 'Santander',
        remisiones: ['A0-260701116', 'A0-260603748'], cliente: '', idNumo: '052549', estatus: 'POR FACTURAR' },
    ],
  }],
};

const valores = ws => ws.getSheetValues().filter(Boolean).map(r => r.slice(1));
const factura = poliza.movimientosMostrador[0];
const hoja = (...facturas) => {
  const ws = _construirWorkbookPoliza({ ...poliza, movimientosMostrador: facturas }, [], null, new Map()).getWorksheet('MOSTRADOR');
  return ws ? valores(ws) : null;
};
const COLUMNAS = ['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'APLICADO A FACTURA', 'BANCO', 'REM', 'CLIENTE', 'ID NUMO', 'ESTATUS', 'FACTURA'];
const nancy = {
  uuid: 'N', serie: 'A0', folio: '261003190', esGlobal: false, totalFactura: 1098.85, totalAplicado: 1098.85,
  depositos: [{ fecha: '2026-10-20T12:00:00.000Z', descripcion: 'Abono Interbancario', importe: 1098.85, aplicado: 1098.85, banco: 'Banamex',
    remisiones: ['A0-261003190'], cliente: 'NANCY MIGUEL LOPEZ', idNumo: '53714', estatus: 'FACTURADO', ppdPorFacturar: false }],
};

test('facturas globales: una sola tabla con la leyenda EN FACTURA GLOBAL y columna FACTURA', () => {
  const otraGlobal = { ...factura, uuid: 'G2', folio: '261009477', esGlobal: true, totalFactura: 100, totalAplicado: 100,
    depositos: [{ ...factura.depositos[0], idNumo: '9', importe: 100, aplicado: 100 }] };
  const filas = hoja({ ...factura, esGlobal: true }, otraGlobal);
  expect(filas[0][0]).toBe('EN FACTURA GLOBAL — A0-261009872, A0-261009477');
  expect(filas[1]).toEqual(COLUMNAS);
  expect(filas[2].slice(1)).toEqual(['DEPOSITO EN EFECTIVO', 11580.53, 11580.53, 'BBVA', 'A0-260901786\nA0-260901831', 'JUAN LOPEZ MORALES', '052586', 'POR FACTURAR', 'A0-261009872']);
  expect(filas[3][6]).toBe('CLIENTE NO IDENTIFICADO');
  expect(filas[4][9]).toBe('A0-261009477');
  expect(filas[5].slice(1, 7)).toEqual(['TOTAL', 16680.53, 17181.6, undefined, 'Total facturas', 229802.95]);
});

test('no global: solo los depósitos PPD "POR FACTURAR" en su tabla; lo FACTURADO (NANCY) no sale', () => {
  const noGlobal = { ...factura, esGlobal: false, depositos: [{ ...factura.depositos[0], ppdPorFacturar: true }, { ...factura.depositos[1], ppdPorFacturar: false }] };
  const filas = hoja(noGlobal, nancy);
  expect(filas[0][0]).toBe('PPD POR FACTURAR YA FACTURADO A NOMBRE DEL CLIENTE');
  expect(filas[1]).toEqual(COLUMNAS);
  expect(filas[2][7]).toBe('052586');
  expect(filas[3].slice(1, 4)).toEqual(['TOTAL', 11580.53, 11580.53]);
  expect(filas[3][6]).toBeUndefined();
  expect(filas.flat()).not.toContain('NANCY MIGUEL LOPEZ');
  expect(filas.flat()).not.toContain('052549');
});

test('solo facturas ya FACTURADAS no globales: no hay hoja', () => {
  expect(hoja(nancy)).toBeNull();
});

test('póliza generada antes de clasificar: bloques por factura bajo "SIN CLASIFICAR"', () => {
  const filas = hoja(factura);
  expect(filas[0][0]).toBe('SIN CLASIFICAR (regenerar la póliza)');
  expect(filas[1][6]).toBe('A0-261009872');
  expect(filas[2]).toEqual(COLUMNAS.slice(0, 9));
  expect(filas[5].slice(1, 4)).toEqual(['TOTAL', 16580.53, 17081.6]);
});

test('sin facturas de mostrador, o en archivos que no son Ventas, no hay hoja', () => {
  expect(_construirWorkbookPoliza({ ...poliza, movimientosMostrador: null }, [], null, new Map()).getWorksheet('MOSTRADOR')).toBeUndefined();
  expect(_construirWorkbookPoliza(poliza, [], null, new Map(), [], [], null, false).getWorksheet('MOSTRADOR')).toBeUndefined();
});
