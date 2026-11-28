'use strict';

// CEDIS (2026-11-27, pedido del usuario; póliza 1878 del 26-nov): devolución DEV-059842 que Kore
// dejó como saldo a favor → NC A0-261113595 + anticipo "sin cobro" A0-261113669 (OPA-01516).
// La NC debe salir en DEV junto con la creación de su OPA y el anticipo ya no sale en Ventas.
jest.mock('../../../shared/models/postgres', () => ({
  AccountPlan: { findAll: jest.fn(async () => []), findOne: jest.fn(async () => null) },
  Terminal: { findAll: jest.fn(async () => []) },
  CfdiMappingRule: {}, PolizaMovimiento: {}, Poliza: {}, CentroCosto: {},
}));

const { _juntarDevolucionConSuOpa: juntar } = require('./poliza.service');

const NC = 'A0-261113595';
const OPA = 'A0-261113669';
const nc = [
  { serie: NC, tipoComprobante: 'E', tipoOrigen: 'Devolución', rfcReceptor: 'AUOD8706152U2', debe: '237.60', haber: '0.00', concepto: 'DEV-059842' },
  { serie: NC, tipoComprobante: 'E', tipoOrigen: 'Devolución', rfcReceptor: 'AUOD8706152U2', debe: '38.02', haber: '0.00', concepto: 'DEV-059842' },
  { serie: NC, tipoComprobante: 'E', tipoOrigen: 'Devolución', rfcReceptor: 'AUOD8706152U2', debe: '0.00', haber: '275.62', concepto: 'DEV-059842' },
];
// El Cargo 'Venta Sin Cobro' del anticipo ya no llega (lo quita `_extraerCobrosSucursal`).
const anticipo = [
  { serie: OPA, tipoComprobante: 'I', tipoOrigen: 'Venta', reglaNombre: 'Reg 22C — Recepción Anticipo por Descripción', rfcReceptor: 'AUOD8706152U2', debe: '0.00', haber: '38.02', concepto: 'OPA-01516' },
  { serie: OPA, tipoComprobante: 'I', tipoOrigen: 'Venta', reglaNombre: 'Reg 22C — Recepción Anticipo por Descripción', rfcReceptor: 'AUOD8706152U2', debe: '0.00', haber: '237.60', concepto: 'OPA-01516' },
];
const venta = [
  { serie: 'A0-1', tipoComprobante: 'I', tipoOrigen: 'Venta', reglaNombre: 'Reg 1', rfcReceptor: 'XAXX010101000', debe: '0.00', haber: '100.00' },
];

test('la NC sale con la creación de su OPA (sin abono de la NC) y el anticipo sale de Ventas', () => {
  const r = juntar([...anticipo, ...venta], [...nc]);
  expect(r.contado).toEqual(venta);
  expect(r.credito).toEqual([nc[0], nc[1]]);
  expect(r.pares).toEqual([{ serieNc: NC, abonosOpa: [anticipo[1], anticipo[0]] }]);
  const cargos = r.credito.reduce((s, m) => s + Number(m.debe), 0);
  const abonos = r.pares[0].abonosOpa.reduce((s, m) => s + Number(m.haber), 0);
  expect(cargos).toBeCloseTo(abonos, 2);
});

test('no se toca: anticipo con cobro real, otro cliente u otro importe', () => {
  const conCobro = [{ ...anticipo[0], debe: '275.62', haber: '0.00', tipoOrigen: 'Venta' }, ...anticipo];
  expect(juntar(conCobro, [...nc]).pares).toEqual([]);
  expect(juntar(anticipo.map(m => ({ ...m, rfcReceptor: 'OTRO' })), [...nc]).pares).toEqual([]);
  expect(juntar([anticipo[1]], [...nc]).pares).toEqual([]);
});
