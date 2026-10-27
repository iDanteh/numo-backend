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

// 2da parte (8-oct): factura pagada con porciones de varios depósitos — cada
// cobro toma el depósito que le aplicó ese monto (CxC vinculada en Bancos).
test('ALDARA 8-oct: 051243 = $7,025.30 y 050674 = $3,950 (por monto aplicado)', () => {
  const dep = (id, ref, total, aplicado, n) => ({ ...deposito(id, ref, total, n), _montoAplicado: aplicado });
  const movs = [
    ...factura({ pago: 'P1', fact: 'F892', monto: 1123.28, iva: 154.93, concepto: 'ALDARA / A0-260900892' }),
    ...factura({ pago: 'P1', fact: 'F1025', monto: 5902.02, iva: 814.07, concepto: 'ALDARA / A0-260901025' }),
    ...factura({ pago: 'P2', fact: 'F825', monto: 3210.36, iva: 442.81, concepto: 'ALDARA / A0-260900825' }),
    ...factura({ pago: 'P2', fact: 'F892', monto: 739.64, iva: 102.02, concepto: 'ALDARA / A0-260900892' }),
  ];
  const d892a = dep('a', '051243', 7025.30, 1123.28, 2);
  const d892b = dep('b', '050674', 3950, 739.64, 2);
  const verdad = new Map([
    ['F892', { ...d892a, _candidatos: [d892a, d892b] }],
    ['F1025', dep('a', '051243', 7025.30, 5902.02, 2)],
    ['F825', dep('b', '050674', 3950, 3210.36, 2)],
  ]);
  const b = bancos(anotar(movs, 20, verdad));
  expect(b.map(l => [l.serie, l.debe]).sort()).toEqual([['050674', 3950], ['051243', 7025.3]]);
});

test('NI: un subtotal y un IVA por depósito, con folio de Numo y todas las ventas', () => {
  const DNI = { codigo: '2103030001' };
  const IVA_NI = { codigo: '2104010002' };
  const ni = (fact, venta, sub, iva) => [
    { cfdiUuid: 'P7', facturaUuid: fact, formaPago: '03', serie: 'B0-1', concepto: `MINI ABASTOS / ${venta}`, centroCosto: '111', reglaNombre: 'NI', cuenta: DNI, debe: sub, haber: 0 },
    { cfdiUuid: 'P7', facturaUuid: fact, formaPago: '03', serie: 'B0-1', concepto: `MINI ABASTOS / ${venta}`, centroCosto: '111', reglaNombre: 'NI', cuenta: IVA_NI, debe: iva, haber: 0 },
  ];
  const movs = [...ni('F1', 'B0-260803365', 423.58, 67.77), ...ni('F2', 'B0-260803238', 494.52, 79.12), ...ni('F3', 'B0-260803492', 5470.28, 875.24)];
  const d = { ...deposito('x', '046486', 7479.42, 4), numeroAutorizacion: '29' };
  const verdad = new Map([['F1', d], ['F2', d], ['F3', d]]);

  const out = anotar(movs, 20, verdad).filter(l => l.reglaNombre === 'NI');
  expect(out.map(l => [l.cuenta.codigo, l.serie, l.debe])).toEqual([
    ['2103030001', '046486', 6388.38],
    ['2104010002', 'NI', 1022.13],
  ]);
  expect(out[0].concepto).toBe('MINI ABASTOS / B0-260803365 / B0-260803238 / B0-260803492');
});

test('NI con OPA del sobrante: el depósito 046486 sale completo ($7,479.42)', () => {
  const DNI = { codigo: '2103030001' };
  const IVA_NI = { codigo: '2104010002' };
  const ANT = { codigo: '2103010001' };
  const base = { cfdiUuid: 'P7', formaPago: '03', centroCosto: '111' };
  const ni = (fact, venta, sub, iva) => [
    { ...base, facturaUuid: fact, serie: 'B0-1', concepto: `MINI ABASTOS / ${venta}`, reglaNombre: 'NI', cuenta: DNI, debe: sub, haber: 0 },
    { ...base, facturaUuid: fact, serie: 'B0-1', concepto: `MINI ABASTOS / ${venta}`, reglaNombre: 'NI', cuenta: IVA_NI, debe: iva, haber: 0 },
  ];
  const opa = { ...base, facturaUuid: null, serie: 'OPA-01106', concepto: 'MINI ABASTOS / OPA-01106' };
  const movs = [
    ...ni('F1', 'B0-260803365', 423.58, 67.77), ...ni('F2', 'B0-260803238', 494.52, 79.12), ...ni('F3', 'B0-260803492', 5470.28, 875.24),
    { ...opa, reglaNombre: 'NI', cuenta: DNI, debe: 59.41, haber: 0 },
    { ...opa, reglaNombre: 'NI', cuenta: IVA_NI, debe: 9.5, haber: 0 },
    { ...opa, reglaNombre: 'OPA-COBRANZA', cuenta: ANT, debe: 0, haber: 59.41 },
    { ...opa, reglaNombre: 'OPA-COBRANZA', cuenta: IVA_NI, debe: 0, haber: 9.5 },
  ];
  const d = deposito('x', '046486', 7479.42, 4);
  const verdad = new Map([['F1', d], ['F2', d], ['F3', d], ['P7', d]]);

  const out = anotar(movs, 20, verdad);
  const niOut = out.filter(l => l.reglaNombre === 'NI');
  expect(niOut.map(l => [l.cuenta.codigo, l.serie, l.debe])).toEqual([
    ['2103030001', '046486', 6447.79],
    ['2104010002', 'NI', 1031.63],
  ]);
  expect(Math.round(niOut.reduce((s, l) => s + l.debe, 0) * 100) / 100).toBe(7479.42);
  expect(niOut[0].concepto).toBe('MINI ABASTOS / B0-260803365 / B0-260803238 / B0-260803492 / OPA-01106');
  expect(out.filter(l => l.reglaNombre === 'OPA-COBRANZA').map(l => [l.cuenta.codigo, l.haber])).toEqual([['2103010001', 59.41], ['2104010002', 9.5]]);
});

test('NI de una factura pagada con 2 transferencias → cada cobro con su depósito (AUTOOBRA 13-oct)', () => {
  const DNI = { codigo: '2103030001' };
  const IVA_NI = { codigo: '2104010002' };
  const ni = (pago, sub, iva) => [
    { cfdiUuid: pago, facturaUuid: 'F1', formaPago: '03', serie: 'A0-260916515', concepto: 'AUTOOBRA SISTEMAS CONSTRUCTIVOS / A0-260904617', centroCosto: '300', reglaNombre: 'NI', cuenta: DNI, debe: sub, haber: 0 },
    { cfdiUuid: pago, facturaUuid: 'F1', formaPago: '03', serie: 'A0-260916515', concepto: 'AUTOOBRA SISTEMAS CONSTRUCTIVOS / A0-260904617', centroCosto: '300', reglaNombre: 'NI', cuenta: IVA_NI, debe: iva, haber: 0 },
  ];
  const movs = [...ni('P1', 370.68, 59.31), ...ni('P2', 168.42, 26.95)];
  const d1 = { ...deposito('a', '048657', 429.99), _montoAplicado: 429.99 };
  const d2 = { ...deposito('b', '050112', 195.37), _montoAplicado: 195.37 };
  const verdad = new Map([['F1', { ...d2, _candidatos: [d1, d2] }]]);

  const out = anotar(movs, 20, verdad).filter(l => l.reglaNombre === 'NI');
  expect(out.map(l => [l.cuenta.codigo, l.serie, l.debe])).toEqual([
    ['2103030001', '048657', 370.68],
    ['2104010002', 'NI', 59.31],
    ['2103030001', '050112', 168.42],
    ['2104010002', 'NI', 26.95],
  ]);
});

// 2026-10-27: "siempre se toma el monto del depósito" — OSCAR JIMENEZ LOPEZ
// dep 054959: complemento $3,033.75, depósito $3,034.00 (aplicó $3,033.75).
test('depósito de este cobro con diferencia > 10 centavos → banco y Clientes al monto del depósito', () => {
  const movs = factura({ pago: 'P7', fact: 'F7', monto: 3033.75, iva: 418.45, concepto: 'OSCAR JIMENEZ LOPEZ / D0-261004304' });
  const verdad = new Map([['F7', { ...deposito('o', '054959', 3034), _montoAplicado: 3033.75 }]]);
  const out = anotar(movs, 20, verdad);
  expect(bancos(out).map(l => [l.serie, l.debe])).toEqual([['054959', 3034]]);
  expect(out.find(l => l.cuenta.codigo === '1103010001').haber).toBe(3034);
  const debe = out.reduce((s, l) => s + Number(l.debe), 0);
  const haber = out.reduce((s, l) => s + Number(l.haber), 0);
  expect(Math.round((debe - haber) * 100)).toBe(0);
});

test('depósito que aplicó otro monto a la factura (no es solo de este cobro) → se queda el complemento', () => {
  const movs = factura({ pago: 'P7', fact: 'F7', monto: 3033.75, iva: 418.45, concepto: 'X / D0-1' });
  const verdad = new Map([['F7', { ...deposito('o', '054959', 6067.50), _montoAplicado: 6067.50 }]]);
  expect(bancos(anotar(movs, 20, verdad)).map(l => l.debe)).toEqual([3033.75]);
});
