'use strict';

// netpay-migracion.service.test.js — netpay-matching-v2 (design.md "Migration
// Classification Rule"): clasificador PURO (sin I/O) que decide, para cada registro
// legacy de NetpayMatch/NetpayReporte, su nuevo estado v2 — la migración runtime
// (scripts/migrate-netpay-v2.js) es Phase 4, este archivo solo implementa/prueba la
// regla de clasificación en sí. El discriminador es la FUENTE del monto esperado, leída
// del propio erpLink de la colección BankMovement asociada (design.md: "It can be read
// from the collection plus the erpLink on the movement").
const {
  clasificarNetpayMatch, clasificarNetpayReporte,
} = require('./netpay-migracion.service');

function mov(erpLinks = []) {
  return { erpLinks };
}

describe('clasificarNetpayMatch', () => {
  test('matcheada, con TODOS los movementIdsConfirmados con erpLink NETPAY-: confirmado_automatico', () => {
    const doc = { estatusMatch: 'matcheada', movementIdsConfirmados: ['m1', 'm2'] };
    const movimientos = new Map([
      ['m1', mov([{ erpId: 'NETPAY-T1-2026-09-10' }])],
      ['m2', mov([{ erpId: 'NETPAY-T1-2026-09-10' }])],
    ]);

    const r = clasificarNetpayMatch(doc, movimientos);

    expect(r).toEqual({
      estatusMatch: 'confirmado_automatico', motivoDiscrepancia: null, estatusLegacy: 'matcheada', bucket: 'general',
    });
  });

  test('matcheada, pero UNO de los movementIdsConfirmados perdió su erpLink NETPAY- (split parcialmente huérfano): discrepancia/vinculo_huerfano', () => {
    const doc = { estatusMatch: 'matcheada', movementIdsConfirmados: ['m1', 'm2'] };
    const movimientos = new Map([
      ['m1', mov([{ erpId: 'NETPAY-T1-2026-09-10' }])],
      ['m2', mov([])], // se desvinculó fuera de este flujo
    ]);

    const r = clasificarNetpayMatch(doc, movimientos);

    expect(r).toEqual({
      estatusMatch: 'discrepancia', motivoDiscrepancia: 'vinculo_huerfano', estatusLegacy: 'matcheada', bucket: 'general',
    });
  });

  test('matcheada, movimiento inexistente en el mapa (nunca cargado / borrado): discrepancia/vinculo_huerfano, nunca revienta', () => {
    const doc = { estatusMatch: 'matcheada', movementIdsConfirmados: ['m-fantasma'] };
    const r = clasificarNetpayMatch(doc, new Map());
    expect(r.estatusMatch).toBe('discrepancia');
    expect(r.motivoDiscrepancia).toBe('vinculo_huerfano');
  });

  test('descartada-manual: rechazado', () => {
    const doc = { estatusMatch: 'descartada-manual', movementIdsConfirmados: [] };
    const r = clasificarNetpayMatch(doc, new Map());
    expect(r).toEqual({ estatusMatch: 'rechazado', motivoDiscrepancia: null, estatusLegacy: 'descartada-manual', bucket: 'general' });
  });

  test('estado desconocido: el clasificador arroja (dry-run lo reporta como no mapeado)', () => {
    const doc = { estatusMatch: 'algo-nuevo-no-contemplado', movementIdsConfirmados: [] };
    expect(() => clasificarNetpayMatch(doc, new Map())).toThrow(/no reconocido/);
  });
});

describe('clasificarNetpayReporte', () => {
  test('confirmado, con el erpLink NETPAYRPT-<claveRastreo> exacto: resuelto_por_reporte, vinculo:erp-link', () => {
    const doc = { estatus: 'confirmado', claveRastreo: 'CLAVE-1', movementIdConfirmado: 'm1' };
    const movimiento = mov([{ erpId: 'NETPAYRPT-CLAVE-1' }]);

    const r = clasificarNetpayReporte(doc, movimiento);

    expect(r).toEqual({
      estatus: 'resuelto_por_reporte', vinculo: 'erp-link', motivoDiscrepancia: null, estatusLegacy: 'confirmado', bucket: 'general',
    });
  });

  test('confirmado, pero el erpLink NETPAYRPT- no coincide o no existe: discrepancia/vinculo_huerfano', () => {
    const doc = { estatus: 'confirmado', claveRastreo: 'CLAVE-1', movementIdConfirmado: 'm1' };
    const movimiento = mov([{ erpId: 'NETPAYRPT-OTRA-CLAVE' }]);

    const r = clasificarNetpayReporte(doc, movimiento);

    expect(r.estatus).toBe('discrepancia');
    expect(r.vinculo).toBeNull();
    expect(r.motivoDiscrepancia).toBe('vinculo_huerfano');
  });

  test('confirmado sin movimiento asociado (null/undefined): discrepancia/vinculo_huerfano, nunca revienta', () => {
    const doc = { estatus: 'confirmado', claveRastreo: 'CLAVE-1', movementIdConfirmado: null };
    const r = clasificarNetpayReporte(doc, null);
    expect(r.estatus).toBe('discrepancia');
    expect(r.motivoDiscrepancia).toBe('vinculo_huerfano');
  });

  // Ratificado en design.md "Decisions Resolved": "a report always represents a complete
  // deposit" — un legacy pendiente NUNCA se vuelve pendiente_por_marca.
  test('pendiente: SIEMPRE discrepancia/sin_candidato (nunca pendiente_por_marca)', () => {
    const doc = { estatus: 'pendiente', claveRastreo: 'CLAVE-1', movementIdConfirmado: null };
    const r = clasificarNetpayReporte(doc, null);
    expect(r.estatus).toBe('discrepancia');
    expect(r.motivoDiscrepancia).toBe('sin_candidato');
    expect(r.estatus).not.toBe('pendiente_por_marca');
  });

  test('descartado: rechazado', () => {
    const doc = { estatus: 'descartado', claveRastreo: 'CLAVE-1', movementIdConfirmado: null };
    const r = clasificarNetpayReporte(doc, null);
    expect(r).toEqual({ estatus: 'rechazado', vinculo: null, motivoDiscrepancia: null, estatusLegacy: 'descartado', bucket: 'general' });
  });

  test('estado desconocido: el clasificador arroja (dry-run lo reporta como no mapeado)', () => {
    const doc = { estatus: 'algo-nuevo-no-contemplado', claveRastreo: 'CLAVE-1' };
    expect(() => clasificarNetpayReporte(doc, null)).toThrow(/no reconocido/);
  });
});
