'use strict';

// Ingreso, columna H de los renglones de venta: "cliente / venta (ticket)" en
// vez de "cliente / factura" (2026-10-14, Puerto 12-oct póliza 1242: factura
// O0-261000076 de la venta O0-261000087). Columna C sigue con la factura.
jest.mock('../../../shared/models/postgres', () => ({
  AccountPlan: { findAll: jest.fn(async () => []), findOne: jest.fn(async () => null) },
  Terminal: { findAll: jest.fn(async () => []) },
  CfdiMappingRule: {}, PolizaMovimiento: {}, Poliza: {}, CentroCosto: {},
}));

const { _enriquecerConceptoConCliente: enriquecer } = require('./poliza.service');

const nombres = new Map([['U1', 'RANCHERIA PUEBLO VIEJO MIXTEPEC'], ['U2', 'ALFONSO JUAREZ GAMBOA'], ['U3', 'PUBLICO EN GENERAL'], ['U4', 'CLIENTE NC']]);
const tickets = new Map([['U1', ['O0-261000087']], ['U3', ['O0-1', 'O0-2', 'O0-3', 'O0-4']]]);
const linea = (extra) => ({ cfdiUuid: 'U1', serie: 'O0-261000076', concepto: 'LONA DE POLIETILENO…', tipoComprobante: 'I', tipoOrigen: 'Venta', reglaNombre: 'Reg 1A — Venta PUE Efectivo 16%', haber: 538.73, ...extra });

test('sin ticket en el renglón → venta del ERP; columna C igual', () => {
  const [m] = enriquecer([linea()], nombres, tickets);
  expect(m.concepto).toBe('RANCHERIA PUEBLO VIEJO MIXTEPEC / O0-261000087');
  expect(m.serie).toBe('O0-261000076');
});

test('ticket del renglón tiene prioridad', () => {
  const [m] = enriquecer([linea({ cfdiUuid: 'U2', serie: 'B0-261000076', serieVentaTicket: 'B0', folioVentaTicket: '261000201' })], nombres, tickets);
  expect(m.concepto).toBe('ALFONSO JUAREZ GAMBOA / B0-261000201');
});

test('sin ticket conocido o demasiados → la factura, como antes', () => {
  expect(enriquecer([linea({ cfdiUuid: 'U4', serie: 'A0-1' })], nombres, tickets)[0].concepto).toBe('CLIENTE NC / A0-1');
  expect(enriquecer([linea({ cfdiUuid: 'U3', serie: 'O0-90' })], nombres, tickets)[0].concepto).toBe('PUBLICO EN GENERAL / O0-90');
});

test('ajustes, marcadores, anticipos/OPA, SF y Egresos no cambian', () => {
  const casos = [
    linea({ _categoria: 'devolucion' }),
    linea({ concepto: 'DEV-054861' }),
    linea({ reglaNombre: 'Reg 22C — Recepción Anticipo por Descripción' }),
    linea({ reglaNombre: 'OPA' }),
    linea({ tipoOrigen: 'Cargo Especial' }),
    linea({ tipoComprobante: 'E' }),
  ];
  const out = enriquecer(casos, nombres, tickets).map(m => m.concepto);
  expect(out).toEqual([
    'RANCHERIA PUEBLO VIEJO MIXTEPEC / O0-261000076',
    'RANCHERIA PUEBLO VIEJO MIXTEPEC / DEV-054861',
    'RANCHERIA PUEBLO VIEJO MIXTEPEC / O0-261000076',
    'RANCHERIA PUEBLO VIEJO MIXTEPEC / O0-261000076',
    'RANCHERIA PUEBLO VIEJO MIXTEPEC / O0-261000076',
    'RANCHERIA PUEBLO VIEJO MIXTEPEC / O0-261000076',
  ]);
});

test('sin mapa de tickets (Cobranza/otros) → igual que antes', () => {
  expect(enriquecer([linea()], nombres)[0].concepto).toBe('RANCHERIA PUEBLO VIEJO MIXTEPEC / O0-261000076');
});
