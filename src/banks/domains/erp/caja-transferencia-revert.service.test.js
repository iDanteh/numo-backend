'use strict';

// caja-transferencia-revert.service.test.js — pedido explícito del usuario 2026-09-02:
// desvincular el erpId sintético CAJA-<koreId> de un BankMovement debe revertir la
// CajaTransferencia asociada a 'pendiente' (antes quedaba estancada en 'matcheada' para
// siempre). Este archivo cubre SOLO _revertirPorDesvinculacion (la query/lógica de
// negocio) — la atomicidad con el desvincular (misma transacción Mongo, aborta si este
// hook tira) se cubre en bank.service.erpUnlinkHooks.test.js.
jest.mock('./CajaTransferencia.model');
jest.mock('../banks/bank.service', () => ({ registerErpUnlinkHook: jest.fn() }));
jest.mock('../../shared/socket', () => ({ emitToAll: jest.fn() }));

const CajaTransferencia = require('./CajaTransferencia.model');
const { registerErpUnlinkHook } = require('../banks/bank.service');
const { emitToAll } = require('../../shared/socket');
const { init, _revertirPorDesvinculacion } = require('./caja-transferencia-revert.service');

function fakeQuery(result) {
  const q = { session: jest.fn(() => q), then: (resolve) => resolve(result) };
  return q;
}

beforeEach(() => {
  jest.clearAllMocks();
});

test('init(): registra _revertirPorDesvinculacion como hook de desvinculación en bank.service.js', () => {
  init();
  expect(registerErpUnlinkHook).toHaveBeenCalledWith(_revertirPorDesvinculacion);
});

describe('_revertirPorDesvinculacion', () => {
  test('erpId real de Kore (no empieza con CAJA-): no dispara ninguna query a CajaTransferencia', async () => {
    CajaTransferencia.findOne = jest.fn();
    await _revertirPorDesvinculacion({ erpId: 'CXC-123', movementId: 'mov-1', session: null, user: null });
    expect(CajaTransferencia.findOne).not.toHaveBeenCalled();
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('erpId vacío/null: no dispara ninguna query', async () => {
    CajaTransferencia.findOne = jest.fn();
    await _revertirPorDesvinculacion({ erpId: null, movementId: 'mov-1', session: null, user: null });
    expect(CajaTransferencia.findOne).not.toHaveBeenCalled();
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('erpId CAJA-<koreId> pero no hay transferencia matcheada con ese koreId: no hace nada', async () => {
    CajaTransferencia.findOne = jest.fn().mockReturnValue(fakeQuery(null));
    CajaTransferencia.updateOne = jest.fn();

    await _revertirPorDesvinculacion({ erpId: 'CAJA-abc123', movementId: 'mov-1', session: null, user: null });

    expect(CajaTransferencia.findOne).toHaveBeenCalledWith({ koreId: 'abc123', estatusMatch: 'matcheada' });
    expect(CajaTransferencia.updateOne).not.toHaveBeenCalled();
    // Nada que revertir -> no hay señal que emitir tampoco.
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('erpId CAJA-<koreId> con transferencia matcheada: la revierte a pendiente y limpia confirmadoPor/confirmadoEn/movementIdsConfirmados', async () => {
    const transferencia = { _id: 't-1', koreId: 'abc123', estatusMatch: 'matcheada' };
    CajaTransferencia.findOne = jest.fn().mockReturnValue(fakeQuery(transferencia));
    CajaTransferencia.updateOne = jest.fn().mockResolvedValue({});

    await _revertirPorDesvinculacion({ erpId: 'CAJA-abc123', movementId: 'mov-1', session: null, user: { _id: 'user-1' } });

    expect(CajaTransferencia.updateOne).toHaveBeenCalledWith(
      { _id: 't-1' },
      { $set: {
        estatusMatch: 'pendiente', confirmadoPor: null, confirmadoEn: null, movementIdsConfirmados: [],
      } },
      { session: null },
    );
    // 2026-09-03: señal cross-banco para que la bandeja de "pendientes de ficha" se
    // autorefresque — el movimiento desvinculado deja de calificar para transferencia-caja.
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
  });

  test('con session (llamado dentro de una transacción): se usa .session(session) en el find y en el update', async () => {
    const sesionFalsa = { id: 'sesion-falsa' };
    const transferencia = { _id: 't-1', koreId: 'abc123', estatusMatch: 'matcheada' };
    const query = fakeQuery(transferencia);
    CajaTransferencia.findOne = jest.fn().mockReturnValue(query);
    CajaTransferencia.updateOne = jest.fn().mockResolvedValue({});

    await _revertirPorDesvinculacion({ erpId: 'CAJA-abc123', movementId: 'mov-1', session: sesionFalsa, user: null });

    expect(query.session).toHaveBeenCalledWith(sesionFalsa);
    expect(CajaTransferencia.updateOne).toHaveBeenCalledWith(
      expect.any(Object), expect.any(Object), { session: sesionFalsa },
    );
    // emitToAll no toma session (no es parte de la transacción Mongo) — se emite igual.
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
  });

  // Split 1:2 (pedido explícito del usuario 2026-09-02): aunque la transferencia haya
  // quedado matcheada contra 2 movimientos, desvincular UNO solo revierte la transferencia
  // COMPLETA a pendiente — sin importar que el otro movimiento siga con su erpLink
  // intacto. La función no distingue split de 1:1: siempre limpia movementIdsConfirmados
  // por completo.
  test('split 1:2: revierte igual la transferencia completa, sin importar el remanente del otro movimiento', async () => {
    const transferencia = {
      _id: 't-1', koreId: 'abc123', estatusMatch: 'matcheada',
      movementIdsConfirmados: ['mov-a', 'mov-b'],
    };
    CajaTransferencia.findOne = jest.fn().mockReturnValue(fakeQuery(transferencia));
    CajaTransferencia.updateOne = jest.fn().mockResolvedValue({});

    // Se desvincula SOLO mov-a — mov-b (no representado acá) sigue con su erpLink intacto.
    await _revertirPorDesvinculacion({ erpId: 'CAJA-abc123', movementId: 'mov-a', session: null, user: null });

    expect(CajaTransferencia.updateOne).toHaveBeenCalledWith(
      { _id: 't-1' },
      { $set: expect.objectContaining({ estatusMatch: 'pendiente', movementIdsConfirmados: [] }) },
      { session: null },
    );
    // El evento lleva el movementId del movimiento REALMENTE desvinculado (mov-a), no
    // ambos — mov-b no pasó por este hook en absoluto.
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-a' });
  });
});
