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

test('hoja MOSTRADOR: encabezado de la factura, un renglón por depósito y total', () => {
  const ws = _construirWorkbookPoliza(poliza, [], null, new Map()).getWorksheet('MOSTRADOR');
  expect(ws).toBeTruthy();
  const filas = valores(ws);
  expect(filas[0][5]).toBe('A0');
  expect(filas[0][6]).toBe('A0-261009872');
  expect(filas[1]).toEqual(['FECHA', 'DESCRIPCIÓN', 'IMPORTE', 'APLICADO A FACTURA', 'BANCO', 'REM', 'CLIENTE', 'ID NUMO', 'ESTATUS']);
  expect(filas[2].slice(1)).toEqual(['DEPOSITO EN EFECTIVO', 11580.53, 11580.53, 'BBVA', 'A0-260901786\nA0-260901831', 'JUAN LOPEZ MORALES', '052586', 'POR FACTURAR']);
  expect(filas[3][6]).toBe('CLIENTE NO IDENTIFICADO');
  expect(filas[4].slice(1, 4)).toEqual(['TOTAL', 16580.53, 17081.6]);
});

test('sin facturas de mostrador, o en archivos que no son Ventas, no hay hoja', () => {
  expect(_construirWorkbookPoliza({ ...poliza, movimientosMostrador: null }, [], null, new Map()).getWorksheet('MOSTRADOR')).toBeUndefined();
  expect(_construirWorkbookPoliza(poliza, [], null, new Map(), [], [], null, false).getWorksheet('MOSTRADOR')).toBeUndefined();
});
