'use strict';

// bank.service.historialVinculacion.test.js — 2026-09-01 (pedido explícito del usuario):
// vincular/desvincular una CxC borraba erpLinks[] sin dejar ningún rastro — la
// trazabilidad se perdía apenas alguien desvinculaba. historialVinculacion (nuevo campo,
// BankMovement.model.js) persiste cada evento (vinculado/desvinculado/ajustado), aunque
// ya no esté vigente. Este archivo cubre updateErpIds() (desvincular manual, 1 CxC) y
// setErpIds() (PUT que reemplaza el arreglo completo) — mismo criterio de mocks que
// bank.service.setErpIds.test.js.
jest.mock('./BankMovement.model');
jest.mock('../../../shared/services/rbac-store');
jest.mock('../../shared/socket');

const BankMovement  = require('./BankMovement.model');
const rbacStore      = require('../../../shared/services/rbac-store');
const { emitToBanco } = require('../../shared/socket');
const bankService    = require('./bank.service');

// 2026-09-02: updateErpIds()/setErpIds() ahora corren dentro de conTransaccion() (banks/
// shared/utils/mongo-tx.js) cuando hay una baja real de por medio (para correr los
// hooks de desvinculación de forma atómica, ver bank.service.erpUnlinkHooks.test.js).
// No hace falta mockear mongoose acá: sin una conexión real, conTransaccion() detecta
// que no hay replica set y llama fn(null) directo (mismo comportamiento que prueba
// mongo-tx.test.js) — este archivo no necesita saber nada de eso.

function fakeQuery(mov) {
  const q = { session: jest.fn(() => q), then: (resolve) => resolve(mov) };
  return q;
}

function fakeMov(overrides = {}) {
  return {
    _id: 'mov-1', banco: 'BBVA', erpIds: [], erpLinks: [], identificadoPor: [],
    historialVinculacion: [], status: 'no_identificado',
    save: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  rbacStore.hasPermission = jest.fn().mockResolvedValue(true);
});

describe('updateErpIds() — desvincular manual, 1 CxC', () => {
  test('CxC estaba vinculada: push "desvinculado" con snapshot del link, userId y userNombre', async () => {
    const linkOriginal = { erpId: 'CXC-1', saldoActual: 500, total: 1000, folioFiscal: 'UUID-1' };
    const mov = fakeMov({ erpIds: ['CXC-1'], erpLinks: [linkOriginal], identificadoPor: [{ userId: 'user-1', erpId: 'CXC-1' }] });
    BankMovement.findById.mockResolvedValue(mov);

    const updated = await bankService.updateErpIds('mov-1', 'remove', 'CXC-1', { _id: 'user-1', nombre: 'Ana' });

    expect(mov.historialVinculacion).toHaveLength(1);
    const entry = mov.historialVinculacion[0];
    expect(entry.accion).toBe('desvinculado');
    expect(entry.erpId).toBe('CXC-1');
    expect(entry.origen).toBe('manual');
    expect(entry.userId).toBe('user-1');
    expect(entry.userNombre).toBe('Ana');
    expect(entry.snapshot).toEqual(linkOriginal);
    expect(updated.historialVinculacion).toBe(mov.historialVinculacion);
  });

  test('CxC ya NO estaba vinculada (reintento): no se registra nada — no hay ninguna acción real', async () => {
    const mov = fakeMov({ erpIds: [], erpLinks: [] });
    BankMovement.findById.mockResolvedValue(mov);

    await bankService.updateErpIds('mov-1', 'remove', 'CXC-1', { _id: 'user-1', nombre: 'Ana' });

    expect(mov.historialVinculacion).toHaveLength(0);
  });

  test('entradas previas del historial se conservan (append, nunca se pisan)', async () => {
    const previa = { at: new Date('2026-08-01'), accion: 'vinculado', erpId: 'CXC-VIEJA', origen: 'manual', userId: 'u0', userNombre: 'Otro', motivo: null, snapshot: null };
    const mov = fakeMov({ erpIds: ['CXC-1'], erpLinks: [{ erpId: 'CXC-1', saldoActual: 0, total: 0 }], historialVinculacion: [previa] });
    BankMovement.findById.mockResolvedValue(mov);

    await bankService.updateErpIds('mov-1', 'remove', 'CXC-1', { _id: 'user-1', nombre: 'Ana' });

    expect(mov.historialVinculacion).toHaveLength(2);
    expect(mov.historialVinculacion[0]).toBe(previa);
  });
});

describe('setErpIds() — historial de altas y bajas (PUT, reemplaza el arreglo completo)', () => {
  test('alta nueva (CxC que no estaba antes): push "vinculado" con snapshot del link nuevo', async () => {
    const mov = fakeMov();
    BankMovement.findById.mockReturnValue(fakeQuery(mov));

    await bankService.setErpIds(
      'mov-1',
      [{ erpId: 'CXC-1', saldoActual: 500 }],
      { _id: 'user-1', role: 'admin', nombre: 'Ana' },
    );

    expect(mov.historialVinculacion).toHaveLength(1);
    const entry = mov.historialVinculacion[0];
    expect(entry.accion).toBe('vinculado');
    expect(entry.erpId).toBe('CXC-1');
    expect(entry.origen).toBe('manual');
    expect(entry.userNombre).toBe('Ana');
    expect(entry.snapshot.erpId).toBe('CXC-1');
  });

  test('baja (CxC que estaba y ya no viene en el PUT): push "desvinculado" con snapshot de ANTES de reemplazar', async () => {
    const linkOriginal = { erpId: 'CXC-1', saldoActual: 500, total: 1000 };
    const mov = fakeMov({ erpIds: ['CXC-1'], erpLinks: [linkOriginal], identificadoPor: [{ userId: 'user-1', erpId: 'CXC-1' }] });
    BankMovement.findById.mockReturnValue(fakeQuery(mov));

    await bankService.setErpIds('mov-1', [], { _id: 'user-1', role: 'admin', nombre: 'Ana' });

    expect(mov.historialVinculacion).toHaveLength(1);
    const entry = mov.historialVinculacion[0];
    expect(entry.accion).toBe('desvinculado');
    expect(entry.erpId).toBe('CXC-1');
    expect(entry.snapshot).toEqual(linkOriginal);
  });

  test('sin cambios (mismas CxC antes y después): no se registra nada', async () => {
    const link = { erpId: 'CXC-1', saldoActual: 500, total: 1000 };
    const mov = fakeMov({ erpIds: ['CXC-1'], erpLinks: [link], identificadoPor: [{ userId: 'user-1', erpId: 'CXC-1' }] });
    BankMovement.findById.mockReturnValue(fakeQuery(mov));

    await bankService.setErpIds('mov-1', [{ erpId: 'CXC-1', saldoActual: 500 }], { _id: 'user-1', role: 'admin', nombre: 'Ana' });

    expect(mov.historialVinculacion).toHaveLength(0);
  });

  test('alta Y baja en el mismo PUT (una CxC sale, otra entra): 2 entradas, una de cada tipo', async () => {
    const linkSaliente = { erpId: 'CXC-VIEJA', saldoActual: 100, total: 100 };
    const mov = fakeMov({ erpIds: ['CXC-VIEJA'], erpLinks: [linkSaliente], identificadoPor: [{ userId: 'user-1', erpId: 'CXC-VIEJA' }] });
    BankMovement.findById.mockReturnValue(fakeQuery(mov));

    await bankService.setErpIds('mov-1', [{ erpId: 'CXC-NUEVA', saldoActual: 200 }], { _id: 'user-1', role: 'admin', nombre: 'Ana' });

    expect(mov.historialVinculacion).toHaveLength(2);
    const accionesPorErpId = Object.fromEntries(mov.historialVinculacion.map(e => [e.erpId, e.accion]));
    expect(accionesPorErpId['CXC-VIEJA']).toBe('desvinculado');
    expect(accionesPorErpId['CXC-NUEVA']).toBe('vinculado');
  });
});
