'use strict';

// 2026-11-12, pedido del usuario: el UUID de una factura cobrada en OTRA
// sucursal no lleva renglón 'AD' en la póliza de la sucursal que cobró; solo
// en la póliza donde está la venta de esa factura. Caso real: Global HIDALGO
// B0-261100739 (CE43C1DB…) salía como 'AD' en CEDIS I-109, ATZOMPA I-804,
// REFORMA I-504 y SIMBOLOS I-604 del 7-nov. El asiento no cambia.
jest.mock('../../../shared/models/postgres', () => ({
  AccountPlan: {}, Terminal: {}, CfdiMappingRule: {}, PolizaMovimiento: {}, Poliza: {}, CentroCosto: {},
}));

const { _construirWorkbookPoliza, _uuidsSoloCobroOtraSucursal } = require('./poliza.service');

const GLOBAL_HIDALGO = 'CE43C1DB-0C4C-488A-AD19-F710DBBEBFB1';
const PROPIA = 'AAAAAAAA-0000-0000-0000-000000000001';
const SF_FACTURA_POSTERIOR = '701772B0-0F07-4902-9156-C38F4B4BD867';
const caja = { codigo: '1101010003' }, bancos = { codigo: '1102011005' }, ingresos = { codigo: '4100010001' }, iva = { codigo: '2104010001' };

const movimientos = [
  // CEDIS 7-nov: ticket B0-261101482 de HIDALGO cobrado en efectivo (par Venta + puente).
  { cfdiUuid: GLOBAL_HIDALGO, tipoOrigen: 'Venta', debe: 8485.23, haber: 0, cuenta: caja, concepto: 'PUBLICO EN GENERAL / B0-261101482' },
  { cfdiUuid: GLOBAL_HIDALGO, tipoOrigen: 'Cobro Sucursal', debe: 0, haber: 8485.23, cuenta: caja, concepto: 'PUBLICO EN GENERAL / B0-261101482' },
  // Venta propia de la sucursal: Cargo + Abonos de Ingresos/IVA.
  { cfdiUuid: PROPIA, tipoOrigen: 'Venta', debe: 116, haber: 0, cuenta: caja, concepto: 'PUBLICO EN GENERAL / A0-1' },
  { cfdiUuid: PROPIA, tipoOrigen: 'Venta', debe: 0, haber: 100, cuenta: ingresos, concepto: 'PUBLICO EN GENERAL / A0-1' },
  { cfdiUuid: PROPIA, tipoOrigen: 'Venta', debe: 0, haber: 16, cuenta: iva, concepto: 'PUBLICO EN GENERAL / A0-1' },
  // SF usado sobre una factura de otro día (PUERTO ESCONDIDO 6-nov, O0-261100161 del 9-nov): solo Cargo.
  { cfdiUuid: SF_FACTURA_POSTERIOR, tipoOrigen: 'Cargo Especial', debe: 78.62, haber: 0, cuenta: bancos, concepto: 'SF' },
];

test('solo se omiten los UUID que en la póliza son únicamente cobro (sin venta propia)', () => {
  const s = _uuidsSoloCobroOtraSucursal(movimientos);
  expect([...s].sort()).toEqual([SF_FACTURA_POSTERIOR, GLOBAL_HIDALGO].sort());
  expect(s.has(PROPIA)).toBe(false);
  // Cobro de otra sucursal por tarjeta (SIMBOLOS 7-nov): solo el Cargo 'Venta' trae el uuid.
  expect(_uuidsSoloCobroOtraSucursal([{ cfdiUuid: GLOBAL_HIDALGO, tipoOrigen: 'Venta', debe: 1998.48, haber: 0 }]).has(GLOBAL_HIDALGO)).toBe(true);
  // La póliza de HIDALGO (venta de la global) sí lo conserva.
  expect(_uuidsSoloCobroOtraSucursal([{ cfdiUuid: GLOBAL_HIDALGO, tipoOrigen: 'Venta', debe: 0, haber: 242522.56 }]).size).toBe(0);
});

test('el Excel CONTPAQ no lleva AD del UUID de otra sucursal; los renglones M1 no cambian', () => {
  const poliza = { tipo: 'I', numero: 109, fecha: '2026-11-07', concepto: 'Ingresos por Ventas Suc. CEDIS Día: 07/11/2026', ejercicio: 2026, periodo: 11 };
  const bloques = [{ tipoVenta: 'Contado', folio: 109, concepto: poliza.concepto, movs: movimientos }];
  const filas = wb => wb.worksheets[0].getSheetValues().filter(Boolean).map(r => r.slice(1));
  const antes = filas(_construirWorkbookPoliza(poliza, bloques, new Date('2026-11-07T12:00:00Z'), new Map()));
  const ahora = filas(_construirWorkbookPoliza(poliza, bloques, new Date('2026-11-07T12:00:00Z'), new Map(), [], [], null, true, [], _uuidsSoloCobroOtraSucursal(movimientos)));
  const ad = f => f.filter(r => r[0] === 'AD').map(r => r[1]);
  expect(ad(antes)).toEqual(expect.arrayContaining([GLOBAL_HIDALGO, PROPIA]));
  expect(ad(ahora)).toEqual([PROPIA]);
  expect(ahora.filter(r => r[0] !== 'AD')).toEqual(antes.filter(r => r[0] !== 'AD'));
});
