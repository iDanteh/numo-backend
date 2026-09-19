'use strict';

// netpay-match-revert.service.test.js — desvincular el erpId sintético
// NETPAY-<terminalID>-<día> de un BankMovement debe BORRAR el NetpayMatch asociado (un
// grupo "pendiente" nunca tiene documento propio, ver NetpayMatch.model.js). A diferencia
// de caja-transferencia-revert.service.js (que parsea el koreId del propio erpId), acá se
// busca por movementIdsConfirmados — sin parsear terminalID/día del string.
jest.mock('./NetpayMatch.model');
jest.mock('../banks/bank.service', () => ({ registerErpUnlinkHook: jest.fn() }));
jest.mock('../../shared/socket', () => ({ emitToAll: jest.fn() }));

const NetpayMatch = require('./NetpayMatch.model');
const { registerErpUnlinkHook } = require('../banks/bank.service');
const { emitToAll } = require('../../shared/socket');
const { init, _revertirPorDesvinculacion } = require('./netpay-match-revert.service');

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
  test('erpId que no empieza con NETPAY- (ej. CAJA- o real de Kore): no dispara ninguna query', async () => {
    NetpayMatch.findOne = jest.fn();
    await _revertirPorDesvinculacion({ erpId: 'CAJA-abc123', movementId: 'mov-1', session: null, user: null });
    expect(NetpayMatch.findOne).not.toHaveBeenCalled();
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('erpId vacío/null: no dispara ninguna query', async () => {
    NetpayMatch.findOne = jest.fn();
    await _revertirPorDesvinculacion({ erpId: null, movementId: 'mov-1', session: null, user: null });
    expect(NetpayMatch.findOne).not.toHaveBeenCalled();
  });

  test('erpId NETPAY- pero no hay match confirmado con ese movementId: no hace nada', async () => {
    NetpayMatch.findOne = jest.fn().mockReturnValue(fakeQuery(null));
    NetpayMatch.deleteOne = jest.fn();

    await _revertirPorDesvinculacion({ erpId: 'NETPAY-2840403056-2026-09-10', movementId: 'mov-1', session: null, user: null });

    expect(NetpayMatch.findOne).toHaveBeenCalledWith({ movementIdsConfirmados: 'mov-1', estatusMatch: 'matcheada' });
    expect(NetpayMatch.deleteOne).not.toHaveBeenCalled();
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('erpId NETPAY- con match confirmado: BORRA el documento (no lo pasa a pendiente)', async () => {
    const match = { _id: 'nm-1' };
    NetpayMatch.findOne = jest.fn().mockReturnValue(fakeQuery(match));
    NetpayMatch.deleteOne = jest.fn().mockResolvedValue({});

    await _revertirPorDesvinculacion({ erpId: 'NETPAY-2840403056-2026-09-10', movementId: 'mov-1', session: null, user: null });

    expect(NetpayMatch.deleteOne).toHaveBeenCalledWith({ _id: 'nm-1' }, { session: null });
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
  });

  test('con session: se usa .session(session) en el find y se propaga al delete', async () => {
    const sesionFalsa = { id: 'sesion-falsa' };
    const match = { _id: 'nm-1' };
    const query = fakeQuery(match);
    NetpayMatch.findOne = jest.fn().mockReturnValue(query);
    NetpayMatch.deleteOne = jest.fn().mockResolvedValue({});

    await _revertirPorDesvinculacion({ erpId: 'NETPAY-2840403056-2026-09-10', movementId: 'mov-1', session: sesionFalsa, user: null });

    expect(query.session).toHaveBeenCalledWith(sesionFalsa);
    expect(NetpayMatch.deleteOne).toHaveBeenCalledWith({ _id: 'nm-1' }, { session: sesionFalsa });
  });
});
