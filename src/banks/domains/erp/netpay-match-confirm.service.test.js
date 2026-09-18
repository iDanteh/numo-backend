'use strict';

// netpay-match-confirm.service.test.js — Fase D del matching Netpay↔BBVA:
// confirmarMatchNetpay()/descartarMatchNetpay() re-validan elegibilidad server-side,
// RECALCULANDO el neto en vivo contra Kore (consultarTransaccionesNetpay, mockeada acá) en
// vez de confiar en lo que trae el cliente. bank.service.js NO se mockea completo — solo
// setErpIds, ERP_TOLERANCE se toma real. netpay-match.service.js tampoco se mockea (se usan
// sus funciones reales: _diaUTC, _buscarCandidatosParaGrupo, _montosIguales) — solo sus
// dependencias (BankMovement, global-config) están mockeadas.
jest.mock('../banks/BankMovement.model');
jest.mock('./NetpayMatch.model');
jest.mock('./netpay-transacciones.service');
jest.mock('../../../shared/services/global-config.service');
jest.mock('../../shared/socket', () => ({ emitToBanco: jest.fn(), emitToAll: jest.fn() }));
jest.mock('../banks/bank.service', () => {
  const real = jest.requireActual('../banks/bank.service');
  return { setErpIds: jest.fn(), ERP_TOLERANCE: real.ERP_TOLERANCE };
});

const mockSession = {
  startTransaction: jest.fn(),
  commitTransaction: jest.fn().mockResolvedValue(undefined),
  abortTransaction: jest.fn().mockResolvedValue(undefined),
  endSession: jest.fn().mockResolvedValue(undefined),
  inTransaction: jest.fn(() => true),
};
jest.mock('mongoose', () => {
  const real = jest.requireActual('mongoose');
  return { ...real, connection: { ...real.connection, startSession: jest.fn() } };
});

const mongoose = require('mongoose');
const BankMovement = require('../banks/BankMovement.model');
const NetpayMatch = require('./NetpayMatch.model');
const globalConfigService = require('../../../shared/services/global-config.service');
const { consultarTransaccionesNetpay } = require('./netpay-transacciones.service');
const { emitToBanco, emitToAll } = require('../../shared/socket');
const { setErpIds } = require('../banks/bank.service');
const { confirmarMatchNetpay, descartarMatchNetpay } = require('./netpay-match-confirm.service');

const USER = { _id: 'user-1', nombre: 'Ana', role: 'contabilidad' };
const DIA = '2026-09-10';
const TERMINAL = '2840403056';

function fakeFind(result) {
  return { lean: jest.fn().mockResolvedValue(result) };
}

function fakeTransaccion(overrides = {}) {
  return { amount: 300, commission: 20, almacen: 'A0', terminalID: TERMINAL, transactionDate: '2026-09-10T14:00:00Z', ...overrides };
}

beforeEach(() => {
  jest.clearAllMocks();
  mongoose.connection.startSession.mockResolvedValue(mockSession);
  mockSession.inTransaction.mockReturnValue(true);
  globalConfigService.getValue.mockResolvedValue('2');
  NetpayMatch.findOne = jest.fn().mockResolvedValue(null);
  BankMovement.find = jest.fn(() => fakeFind([]));
  consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [fakeTransaccion()] }); // neto = 280
});

describe('confirmarMatchNetpay', () => {
  test('falta terminalID o dia: BadRequestError', async () => {
    await expect(confirmarMatchNetpay({ dia: DIA, movementIds: ['m1'], user: USER })).rejects.toThrow('Se requieren terminalID y dia');
    await expect(confirmarMatchNetpay({ terminalID: TERMINAL, movementIds: ['m1'], user: USER })).rejects.toThrow('Se requieren terminalID y dia');
  });

  test('movementIds vacío o con más de 2: BadRequestError', async () => {
    await expect(confirmarMatchNetpay({ terminalID: TERMINAL, dia: DIA, movementIds: [], user: USER })).rejects.toThrow('Se requiere 1 o 2 movementIds');
    await expect(confirmarMatchNetpay({ terminalID: TERMINAL, dia: DIA, movementIds: ['a', 'b', 'c'], user: USER })).rejects.toThrow('Se requiere 1 o 2 movementIds');
  });

  test('grupo ya resuelto (existe NetpayMatch): ConflictError', async () => {
    NetpayMatch.findOne = jest.fn().mockResolvedValue({ estatusMatch: 'matcheada' });
    await expect(confirmarMatchNetpay({ terminalID: TERMINAL, dia: DIA, movementIds: ['m1'], user: USER }))
      .rejects.toThrow(/ya no está pendiente/);
  });

  test('algún movementId no existe: NotFoundError', async () => {
    BankMovement.find = jest.fn().mockResolvedValue([]);
    await expect(confirmarMatchNetpay({ terminalID: TERMINAL, dia: DIA, movementIds: ['m1'], user: USER }))
      .rejects.toThrow('Uno o más movimientos bancarios');
  });

  test('movimiento que no es de BBVA: ConflictError', async () => {
    BankMovement.find = jest.fn().mockResolvedValue([{ _id: 'm1', banco: 'Banamex', erpLinks: [], deposito: 280 }]);
    await expect(confirmarMatchNetpay({ terminalID: TERMINAL, dia: DIA, movementIds: ['m1'], user: USER }))
      .rejects.toThrow(/no es de BBVA/);
  });

  test('movimiento que YA tiene erpLinks: ConflictError', async () => {
    BankMovement.find = jest.fn().mockResolvedValue([{ _id: 'm1', banco: 'BBVA', erpLinks: [{ erpId: 'CXC-1' }], deposito: 280 }]);
    await expect(confirmarMatchNetpay({ terminalID: TERMINAL, dia: DIA, movementIds: ['m1'], user: USER }))
      .rejects.toThrow(/ya tiene un ID ERP vinculado/);
  });

  // Neto recalculado en vivo = 280 (300 - 20, ver fakeTransaccion). Si la suma elegida no
  // cuadra contra ESE recálculo (no contra un valor que mande el cliente), debe fallar.
  test('suma de movimientos no coincide con el neto recalculado en vivo: ConflictError', async () => {
    BankMovement.find = jest.fn().mockResolvedValue([{ _id: 'm1', banco: 'BBVA', erpLinks: [], deposito: 999 }]);
    await expect(confirmarMatchNetpay({ terminalID: TERMINAL, dia: DIA, movementIds: ['m1'], user: USER }))
      .rejects.toThrow(/no coincide con el neto recalculado en vivo/);
  });

  test('match 1 movimiento válido: setErpIds con erpId sintético NETPAY-, crea NetpayMatch, emite sockets', async () => {
    BankMovement.find = jest.fn().mockResolvedValue([{ _id: 'm1', banco: 'BBVA', erpLinks: [], deposito: 280 }]);
    const updatedMov = { _id: 'm1', banco: 'BBVA' };
    setErpIds.mockResolvedValue(updatedMov);
    NetpayMatch.create = jest.fn().mockResolvedValue([{}]);

    const res = await confirmarMatchNetpay({ terminalID: TERMINAL, almacen: 'A0', dia: DIA, movementIds: ['m1'], user: USER });

    expect(setErpIds).toHaveBeenCalledTimes(1);
    const [movId, links] = setErpIds.mock.calls[0];
    expect(movId).toBe('m1');
    expect(links).toEqual([{
      erpId: `NETPAY-${TERMINAL}-2026-09-10`, origen: 'netpay-matching',
      saldoPagadoTotal: 280, saldoPagado: 280, total: 280,
    }]);

    expect(NetpayMatch.create).toHaveBeenCalledWith([expect.objectContaining({
      terminalID: TERMINAL, almacen: 'A0', netoEsperado: 280, estatusMatch: 'matcheada',
      movementIdsConfirmados: ['m1'],
      confirmadoPor: { userId: 'user-1', nombre: 'Ana' },
    })], { session: mockSession });

    expect(mockSession.commitTransaction).toHaveBeenCalled();
    expect(emitToBanco).toHaveBeenCalledWith('BBVA', 'bank:movement:updated', updatedMov);
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'm1' });
    expect(res.movimientos).toEqual([updatedMov]);
  });

  test('match 2 movimientos (split): setErpIds una vez por movimiento, cada uno con SU propio deposito', async () => {
    BankMovement.find = jest.fn().mockResolvedValue([
      { _id: 'm1', banco: 'BBVA', erpLinks: [], deposito: 200 },
      { _id: 'm2', banco: 'BBVA', erpLinks: [], deposito: 80 },
    ]);
    setErpIds.mockResolvedValueOnce({ _id: 'm1', banco: 'BBVA' }).mockResolvedValueOnce({ _id: 'm2', banco: 'BBVA' });
    NetpayMatch.create = jest.fn().mockResolvedValue([{}]);

    await confirmarMatchNetpay({ terminalID: TERMINAL, dia: DIA, movementIds: ['m1', 'm2'], user: USER });

    expect(setErpIds).toHaveBeenCalledTimes(2);
    expect(setErpIds.mock.calls[0][1][0]).toMatchObject({ saldoPagadoTotal: 200, total: 200 });
    expect(setErpIds.mock.calls[1][1][0]).toMatchObject({ saldoPagadoTotal: 80, total: 80 });
    expect(emitToAll).toHaveBeenCalledTimes(2);
  });

  test('Mongo sin soporte de transacciones (standalone, code 20): cae al camino sin sesión', async () => {
    BankMovement.find = jest.fn().mockResolvedValue([{ _id: 'm1', banco: 'BBVA', erpLinks: [], deposito: 280 }]);
    NetpayMatch.create = jest.fn().mockResolvedValue([{}]);
    const err = new Error('Transaction numbers are only allowed on a replica set member or mongos');
    err.code = 20;
    setErpIds.mockRejectedValueOnce(err).mockResolvedValueOnce({ _id: 'm1', banco: 'BBVA' });

    const res = await confirmarMatchNetpay({ terminalID: TERMINAL, dia: DIA, movementIds: ['m1'], user: USER });

    expect(setErpIds).toHaveBeenCalledTimes(2);
    expect(setErpIds.mock.calls[1][3].session).toBeNull();
    expect(mockSession.abortTransaction).toHaveBeenCalled();
    expect(res.movimientos).toEqual([{ _id: 'm1', banco: 'BBVA' }]);
  });
});

describe('descartarMatchNetpay', () => {
  test('falta terminalID o dia: BadRequestError', async () => {
    await expect(descartarMatchNetpay({ dia: DIA, user: USER })).rejects.toThrow('Se requieren terminalID y dia');
  });

  test('grupo ya resuelto: ConflictError', async () => {
    NetpayMatch.findOne = jest.fn().mockResolvedValue({ estatusMatch: 'descartada-manual' });
    await expect(descartarMatchNetpay({ terminalID: TERMINAL, dia: DIA, user: USER })).rejects.toThrow(/ya no está pendiente/);
  });

  test('con candidato(s) disponibles: ConflictError, no descarta', async () => {
    BankMovement.find = jest.fn(() => fakeFind([{ _id: 'm1', banco: 'BBVA', deposito: 280, fecha: new Date('2026-09-10T00:00:00Z') }]));
    NetpayMatch.create = jest.fn();

    await expect(descartarMatchNetpay({ terminalID: TERMINAL, dia: DIA, user: USER }))
      .rejects.toThrow(/ya tiene candidato\(s\)/);
    expect(NetpayMatch.create).not.toHaveBeenCalled();
  });

  test('sin candidatos: crea NetpayMatch descartada-manual', async () => {
    BankMovement.find = jest.fn(() => fakeFind([]));
    NetpayMatch.create = jest.fn().mockResolvedValue({});

    const res = await descartarMatchNetpay({ terminalID: TERMINAL, almacen: 'A0', dia: DIA, user: USER });

    expect(NetpayMatch.create).toHaveBeenCalledWith(expect.objectContaining({
      terminalID: TERMINAL, almacen: 'A0', netoEsperado: 280, estatusMatch: 'descartada-manual',
      descartadoManualmentePor: { userId: 'user-1', nombre: 'Ana' },
    }));
    expect(res.estatusMatch).toBe('descartada-manual');
  });
});
