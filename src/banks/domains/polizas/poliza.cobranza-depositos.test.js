'use strict';

// Cobranza (2026-10-13, PDF "incidencia numo cobranza"):
// - Una factura pagada con 2 transferencias lleva 2 bancos, cada uno con su
//   depósito por monto (HERRAMIENTAS CANANEA A0-260907729: $4,062.26 dep 051168
//   + $4,000 dep 5416), no uno solo de $8,062.26.
// - Depósito de una sola factura que difiere por centavos del complemento: el
//   banco va por el monto del depósito y el Abono a Clientes igual (JAVIER DIAZ
//   RODRIGUEZ dep 051726: $1,602.92 → $1,602.93).
jest.mock('../../../shared/models/postgres', () => ({
  AccountPlan: { findAll: jest.fn(async () => []), findOne: jest.fn(async () => null) },
  Terminal: { findAll: jest.fn(async () => []) },
  CfdiMappingRule: {}, PolizaMovimiento: {}, Poliza: {}, CentroCosto: {},
}));

const { _anotarCargosPorFacturaSinAgrupar: anotar } = require('./poliza.service');

const BANCOS = { codigo: '1102011005' };
const BANAMEX = { codigo: '1102012001', nombre: 'Banamex' };
const CLIENTES = { codigo: '1103010001' };
const IVA_X_TRAS = { codigo: '2105010001' };
const IVA_TRAS = { codigo: '2104010001' };

const factura = ({ pago, fact, monto, iva, concepto }) => {
  const base = { cfdiUuid: pago, facturaUuid: fact, formaPago: '03', serie: 'A0-260909212', concepto, centroCosto: '101', reglaNombre: 'Reg 7B', tipoOrigen: 'Pago' };
  return [
    { ...base, cuenta: BANCOS, debe: monto, haber: 0 },
    { ...base, cuenta: IVA_X_TRAS, debe: iva, haber: 0 },
    { ...base, cuenta: IVA_TRAS, debe: 0, haber: iva },
    { ...base, cuenta: CLIENTES, debe: 0, haber: monto },
  ];
};
const deposito = (id, referencia, monto, facturas = 1) => ({
  esTransferencia: true, categoriaConocida: true, referencia, cuentaBanco: BANAMEX,
  montoBancoReal: monto, _idDeposito: id, _facturasDeposito: facturas,
});
const bancos = (lineas) => lineas.filter(l => /^1102/.test(l.cuenta?.codigo || ''));

test('factura con 2 transferencias → 2 bancos, cada uno con su depósito', () => {
  const movs = [
    ...factura({ pago: 'P1', fact: 'F1', monto: 4062.26, iva: 560.31, concepto: 'HERRAMIENTAS CANANEA / A0-260901425' }),
    ...factura({ pago: 'P2', fact: 'F1', monto: 4000, iva: 551.72, concepto: 'HERRAMIENTAS CANANEA / A0-260901425' }),
  ];
  const d1 = deposito('a', '051168', 4062.26);
  const d2 = deposito('b', '5416', 4000);
  const verdad = new Map([['F1', { ...d1, _candidatos: [d1, d2] }]]);

  const b = bancos(anotar(movs, 20, verdad));
  expect(b.map(l => [l.serie, l.debe]).sort()).toEqual([['051168', 4062.26], ['5416', 4000]]);
});

test('sin depósito del mismo monto → como antes (el depósito guardado)', () => {
  const movs = factura({ pago: 'P1', fact: 'F1', monto: 3000, iva: 413.79, concepto: 'X / A0-1' });
  const d1 = deposito('a', '051168', 4062.26, 2);
  const d2 = deposito('b', '5416', 4000, 2);
  const verdad = new Map([['F1', { ...d1, _candidatos: [d1, d2] }]]);
  expect(bancos(anotar(movs, 20, verdad)).map(l => [l.serie, l.debe])).toEqual([['051168', 3000]]);
});

test('centavos: banco y Abono a Clientes al monto del depósito; IVA igual', () => {
  const movs = factura({ pago: 'P9', fact: 'F9', monto: 1602.92, iva: 221.09, concepto: 'JAVIER DIAZ RODRIGUEZ / A0-260900833' });
  const verdad = new Map([['F9', deposito('z', '051726', 1602.93)]]);

  const out = anotar(movs, 20, verdad);
  expect(bancos(out).map(l => [l.serie, l.debe])).toEqual([['051726', 1602.93]]);
  expect(out.find(l => l.cuenta.codigo === '1103010001').haber).toBe(1602.93);
  expect(out.find(l => l.cuenta.codigo === '2104010001').haber).toBe(221.09);
  const debe = out.reduce((s, l) => s + Number(l.debe), 0);
  const haber = out.reduce((s, l) => s + Number(l.haber), 0);
  expect(Math.round((debe - haber) * 100)).toBe(0);
});

test('depósito que paga varias facturas → no se ajustan centavos', () => {
  const movs = factura({ pago: 'P9', fact: 'F9', monto: 1602.92, iva: 221.09, concepto: 'X / A0-1' });
  const verdad = new Map([['F9', deposito('z', '051726', 1602.93, 2)]]);
  const out = anotar(movs, 20, verdad);
  expect(bancos(out)[0].debe).toBe(1602.92);
  expect(out.find(l => l.cuenta.codigo === '1103010001').haber).toBe(1602.92);
});
