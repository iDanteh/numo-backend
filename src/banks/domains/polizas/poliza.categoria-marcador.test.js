'use strict';

// Export de Contado: una NC SAT completa trae en el concepto el marcador del ERP
// ("BCT", "BON-…", "DEV-…") en vez de la descripción y sin tipoOrigen propio —
// debe seguir reconociéndose su categoría (2026-10-13, Puerto O0-261000319 y
// Ferrocarril F0-261000208: los Cargos Bonificación+IVA desaparecían del Excel).
jest.mock('../../../shared/models/postgres', () => ({
  AccountPlan: { findAll: jest.fn(async () => []), findOne: jest.fn(async () => null) },
  Terminal: { findAll: jest.fn(async () => []) },
  CfdiMappingRule: {}, PolizaMovimiento: {}, Poliza: {}, CentroCosto: {},
}));

const { _categorizarAjusteContado: categorizar } = require('./poliza.service');

const nc = (concepto, extra = {}) => ({ tipoComprobante: 'E', tipoOrigen: 'Nota de Crédito', reglaNombre: 'Reg CC-CLT-16', concepto, ...extra });

test('NC con marcador BCT → Club Tuberos', () => {
  expect(categorizar(nc('BCT'))).toBe('clubTuberos');
  expect(categorizar(nc('IVA - BCT'))).toBe('clubTuberos');
});

test('NC con marcador BON / DEV / CAC', () => {
  expect(categorizar(nc('BON-314117'))).toBe('bonificacion');
  expect(categorizar(nc('DEV-058354'))).toBe('devolucion');
  expect(categorizar(nc('CAC-079958'))).toBe('devolucion');
});

test('línea que no es NC con "DEV-…" de origen (SF usado) → sin categoría', () => {
  expect(categorizar({ tipoComprobante: 'I', tipoOrigen: 'Venta', reglaNombre: 'SF', concepto: 'DEV-055991' })).toBeNull();
});

test('como antes: descripción "Bonificacion club tuberos"', () => {
  expect(categorizar(nc('Bonificacion club tuberos'))).toBe('clubTuberos');
});
