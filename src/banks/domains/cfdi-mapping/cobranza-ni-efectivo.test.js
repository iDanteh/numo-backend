'use strict';

// Cobranza (2026-11-11, incidencia "POLIZA COBRANZA INCIDENCIA"): JOSE MANUEL
// SANTIAGO OLIVERA pagó en EFECTIVO el 03-nov, en un solo cobro de $6,498.00,
// A0-261005361/5399/5362 (facturas PPD de octubre). Kore puso en el
// complemento FechaPago 31-oct ("fecha real de pago") y la póliza mandaba el
// Cargo a Depósitos No Identificados (2103030001) + IVA anticipos (2104010002)
// "NI" en vez de al efectivo del día. El efectivo cobrado en caja ese día va a
// Caja; la regla NI sigue para transferencias/depósitos de un mes anterior.
jest.mock('../banks/BankMovement.model', () => ({ find: jest.fn() }));
jest.mock('../../../visor/models/CFDI', () => ({ exists: jest.fn(), find: jest.fn() }));
jest.mock('../../../shared/models/postgres', () => ({
  Poliza: {}, PolizaMovimiento: { findAll: jest.fn(async () => []) }, AccountPlan: {}, CfdiMappingRule: {}, CobroSucursalPendienteCobranza: {},
}));

const { cfdiToMovimientosCobranza } = require('./cobranza-poliza-generator.service');

const CAJA = 3, BANCOS = 4, CLIENTES = 5, DNI = 6, IVA_NI = 7, IVA_PPD = 8, IVA_COBRADO = 9;
const cuentaMap = {
  '1101010003': CAJA, '1102011005': BANCOS, '1103010001': CLIENTES, '2103030001': DNI,
  '2104010002': IVA_NI, '2105010001': IVA_PPD, '2104010001': IVA_COBRADO,
};
const rule = { nombre: 'Cobranza', centroCosto: '300', cuentaCargo: '1102011005', cuentaAbono: '1103010001', cuentaIvaPPD: '2105010001', cuentaIva: '2104010001' };

// Las tres facturas del cobro múltiple: monto pagado e IVA de cada una.
const FACTURAS = [
  { folio: '261005361', monto: 1339.43, iva: 184.75 },
  { folio: '261005399', monto: 1444.65, iva: 199.26 },
  { folio: '261005362', monto: 3713.92, iva: 512.27 },
];

function caso({ fechaPago, formaPago }) {
  const total = FACTURAS.reduce((s, f) => s + f.monto, 0);
  const iva = FACTURAS.reduce((s, f) => s + f.iva, 0);
  const cfdi = {
    uuid: 'PAGO-1', serie: 'A0', folio: '261015572', tipoDeComprobante: 'P', fecha: '2026-11-03T09:19:00.000Z',
    receptor: { nombre: 'JOSE MANUEL SANTIAGO OLIVERA', rfc: 'SAOJ000000XX0' }, emisor: { rfc: 'EMISOR' },
    complementoPago: { totales: { montoTotalPagos: total, totalTrasladosImpuestoIVA16: iva }, pagos: [] },
  };
  const doctosPago = FACTURAS.map(f => ({
    serie: 'A0', folio: f.folio, monto: f.monto, montoSF: 0, ivaDoc: f.iva, idDocumento: null,
    tickets: [`A0-${f.folio}`], fechaPagoReal: fechaPago,
    desglosePagoReal: [{ monto: f.monto, claveSat: formaPago, claveCentro: '300', grupoPago: '300|2026-11-03T09:19|6498.00' }],
  }));
  return cfdiToMovimientosCobranza(cfdi, rule, cuentaMap, { doctosPago }).movs;
}

const cargoA = (movs, cuenta) => Math.round(movs.filter(m => m.cuentaId === cuenta).reduce((s, m) => s + (m.debe || 0), 0) * 100) / 100;

test('efectivo del 03-nov con FechaPago 31-oct: todo al efectivo del día, sin renglones NI', () => {
  const movs = caso({ fechaPago: '2026-10-31T00:00:00.000Z', formaPago: '01' });
  expect(movs.filter(m => m.reglaNombre === 'NI')).toHaveLength(0);
  expect(cargoA(movs, CAJA)).toBe(6498.00);
  expect(movs.filter(m => m.cuentaId === CAJA).every(m => m._formaPagoReal === '01')).toBe(true);
  // El cierre de la CxC y el IVA cobrado no cambian.
  expect(Math.round(movs.filter(m => m.cuentaId === CLIENTES).reduce((s, m) => s + m.haber, 0) * 100) / 100).toBe(6498.00);
  expect(cargoA(movs, IVA_PPD)).toBe(896.28);
  const debe = movs.reduce((s, m) => s + (m.debe || 0), 0);
  const haber = movs.reduce((s, m) => s + (m.haber || 0), 0);
  expect(Math.abs(debe - haber)).toBeLessThan(0.01);
});

test('transferencia de un mes anterior: sigue contra Depósitos No Identificados (regla NI)', () => {
  const movs = caso({ fechaPago: '2026-10-31T00:00:00.000Z', formaPago: '03' });
  expect(cargoA(movs, DNI)).toBe(5601.72);
  expect(cargoA(movs, IVA_NI)).toBe(896.28);
  expect(cargoA(movs, CAJA)).toBe(0);
});

test('efectivo del mismo mes: a Caja, como siempre', () => {
  const movs = caso({ fechaPago: '2026-11-03T09:19:00.000Z', formaPago: '01' });
  expect(movs.filter(m => m.reglaNombre === 'NI')).toHaveLength(0);
  expect(cargoA(movs, CAJA)).toBe(6498.00);
});
