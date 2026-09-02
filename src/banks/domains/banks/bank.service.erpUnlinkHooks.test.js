'use strict';

// bank.service.erpUnlinkHooks.test.js — pedido explícito del usuario 2026-09-02:
// desvincular un erpId sintético CAJA-<koreId> (transferencias entre cajas) debe
// revertir la CajaTransferencia asociada en la MISMA transacción que el desvincular —
// si el hook falla, el movimiento tampoco queda desvinculado. registerErpUnlinkHook()
// deja a bank.service.js ciego a QUIÉN se registra (caja-transferencia-revert.service.js
// se registra solo al cargar erp.routes.js, ver ese archivo) — acá se prueba el
// mecanismo genérico con hooks de prueba, no el hook real.
//
// conTransaccion() (banks/shared/utils/mongo-tx.js) se mockea completo — su propia
// detección de topología / fallback a Mongo standalone YA está cubierta en
// mongo-tx.test.js, no hace falta duplicarla acá. Este archivo solo verifica que
// bank.service.js le pasa la función correcta y reacciona bien al session que le
// devuelva (real o null si conTransaccion decide ir sin transacción).
jest.mock('./BankMovement.model');
jest.mock('../../../shared/services/rbac-store');
jest.mock('../../shared/socket');
jest.mock('../../shared/utils/mongo-tx');

const { conTransaccion } = require('../../shared/utils/mongo-tx');
const BankMovement  = require('./BankMovement.model');
const rbacStore      = require('../../../shared/services/rbac-store');
const { emitToBanco } = require('../../shared/socket');
const bankService    = require('./bank.service');

const SESION_FALSA = { id: 'sesion-transaccion' };

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
  bankService._clearErpUnlinkHooksParaTests();
  rbacStore.hasPermission = jest.fn().mockResolvedValue(true);
  // Por default se comporta como si SÍ hubiera replica set: le pasa una sesión real a fn.
  conTransaccion.mockImplementation((fn) => fn(SESION_FALSA));
});

describe('updateErpIds — hooks de desvinculación', () => {
  test('se removió un link real: usa conTransaccion(), guarda con {session} y corre el hook registrado con erpId/movementId/session/user', async () => {
    const linkOriginal = { erpId: 'CAJA-abc123', saldoActual: 500 };
    const mov = fakeMov({ erpIds: ['CAJA-abc123'], erpLinks: [linkOriginal] });
    BankMovement.findById.mockResolvedValue(mov);
    const hook = jest.fn().mockResolvedValue(undefined);
    bankService.registerErpUnlinkHook(hook);

    await bankService.updateErpIds('mov-1', 'remove', 'CAJA-abc123', { _id: 'user-1', nombre: 'Ana' });

    expect(conTransaccion).toHaveBeenCalledTimes(1);
    expect(mov.save).toHaveBeenCalledWith({ session: SESION_FALSA });
    expect(hook).toHaveBeenCalledWith({ erpId: 'CAJA-abc123', movementId: 'mov-1', session: SESION_FALSA, user: { _id: 'user-1', nombre: 'Ana' } });
  });

  test('reintento sobre algo ya desvinculado (no había link que remover): NO llama a conTransaccion ni corre hooks', async () => {
    const mov = fakeMov({ erpIds: [], erpLinks: [] });
    BankMovement.findById.mockResolvedValue(mov);
    const hook = jest.fn();
    bankService.registerErpUnlinkHook(hook);

    await bankService.updateErpIds('mov-1', 'remove', 'CAJA-abc123', { _id: 'user-1' });

    expect(conTransaccion).not.toHaveBeenCalled();
    expect(mov.save).toHaveBeenCalledWith(); // save() sin argumentos — camino sin transacción
    expect(hook).not.toHaveBeenCalled();
  });

  test('el hook tira error: updateErpIds() rechaza con el mismo error (conTransaccion se encarga de abortar)', async () => {
    const mov = fakeMov({ erpIds: ['CAJA-abc123'], erpLinks: [{ erpId: 'CAJA-abc123' }] });
    BankMovement.findById.mockResolvedValue(mov);
    const errorDelHook = new Error('CajaTransferencia inconsistente');
    bankService.registerErpUnlinkHook(jest.fn().mockRejectedValue(errorDelHook));

    await expect(
      bankService.updateErpIds('mov-1', 'remove', 'CAJA-abc123', { _id: 'user-1' }),
    ).rejects.toThrow('CajaTransferencia inconsistente');
  });

  test('conTransaccion decide ir sin sesión (ej. Mongo standalone): guarda sin {session} y corre el hook igual, con session:null', async () => {
    conTransaccion.mockImplementation((fn) => fn(null));
    const mov = fakeMov({ erpIds: ['CAJA-abc123'], erpLinks: [{ erpId: 'CAJA-abc123' }] });
    BankMovement.findById.mockResolvedValue(mov);
    const hook = jest.fn().mockResolvedValue(undefined);
    bankService.registerErpUnlinkHook(hook);

    await bankService.updateErpIds('mov-1', 'remove', 'CAJA-abc123', { _id: 'user-1' });

    expect(mov.save).toHaveBeenCalledWith(undefined);
    expect(hook).toHaveBeenCalledWith(expect.objectContaining({ session: null }));
  });
});

describe('setErpIds — hooks de desvinculación', () => {
  test('solo alta (sin baja), sin session externa: NO llama a conTransaccion ni corre hooks (comportamiento previo intacto)', async () => {
    const mov = fakeMov();
    BankMovement.findById.mockReturnValue(fakeQuery(mov));
    const hook = jest.fn();
    bankService.registerErpUnlinkHook(hook);

    await bankService.setErpIds('mov-1', [{ erpId: 'CXC-1', saldoActual: 0 }], { _id: 'user-1', role: 'admin' });

    expect(conTransaccion).not.toHaveBeenCalled();
    expect(mov.save).toHaveBeenCalledWith(undefined);
    expect(hook).not.toHaveBeenCalled();
    expect(emitToBanco).toHaveBeenCalledTimes(1);
  });

  test('hay una baja real, sin session externa: usa conTransaccion(), guarda con {session} y corre el hook', async () => {
    const linkOriginal = { erpId: 'CAJA-abc123', saldoActual: 500 };
    const mov = fakeMov({ erpIds: ['CAJA-abc123'], erpLinks: [linkOriginal], identificadoPor: [{ userId: 'user-1', erpId: 'CAJA-abc123' }] });
    BankMovement.findById.mockReturnValue(fakeQuery(mov));
    const hook = jest.fn().mockResolvedValue(undefined);
    bankService.registerErpUnlinkHook(hook);

    await bankService.setErpIds('mov-1', [], { _id: 'user-1', role: 'admin' });

    expect(conTransaccion).toHaveBeenCalledTimes(1);
    expect(mov.save).toHaveBeenCalledWith({ session: SESION_FALSA });
    expect(hook).toHaveBeenCalledWith({ erpId: 'CAJA-abc123', movementId: 'mov-1', session: SESION_FALSA, user: { _id: 'user-1', role: 'admin' } });
    expect(emitToBanco).toHaveBeenCalledTimes(1); // transacción propia -> se emite igual, no está diferido
  });

  test('con session EXTERNA (caller tipo caja-transferencia-confirm.service.js): usa esa session directo, NO llama a conTransaccion', async () => {
    const mov = fakeMov({ erpIds: ['CAJA-abc123'], erpLinks: [{ erpId: 'CAJA-abc123' }], identificadoPor: [{ userId: 'user-1', erpId: 'CAJA-abc123' }] });
    BankMovement.findById.mockReturnValue(fakeQuery(mov));
    const sesionExterna = { id: 'sesion-externa' };
    const hook = jest.fn().mockResolvedValue(undefined);
    bankService.registerErpUnlinkHook(hook);

    await bankService.setErpIds('mov-1', [], { _id: 'user-1', role: 'admin' }, { session: sesionExterna });

    expect(conTransaccion).not.toHaveBeenCalled();
    expect(mov.save).toHaveBeenCalledWith({ session: sesionExterna });
    expect(hook).toHaveBeenCalledWith(expect.objectContaining({ session: sesionExterna }));
    expect(emitToBanco).not.toHaveBeenCalled(); // el caller externo emite tras SU commit
  });

  test('el hook tira error (baja real, sin session externa): setErpIds() rechaza con el mismo error', async () => {
    const mov = fakeMov({ erpIds: ['CAJA-abc123'], erpLinks: [{ erpId: 'CAJA-abc123' }], identificadoPor: [{ userId: 'user-1', erpId: 'CAJA-abc123' }] });
    BankMovement.findById.mockReturnValue(fakeQuery(mov));
    const errorDelHook = new Error('CajaTransferencia inconsistente');
    bankService.registerErpUnlinkHook(jest.fn().mockRejectedValue(errorDelHook));

    await expect(
      bankService.setErpIds('mov-1', [], { _id: 'user-1', role: 'admin' }),
    ).rejects.toThrow('CajaTransferencia inconsistente');
  });
});
