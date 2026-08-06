'use strict';

// mongo-tx.test.js — conTransaccion() puerto genérico de la detección de
// topología + fallback standalone en bank-autorizaciones.service.js:161-197
// (ejecutarBulkConTransaccion), pero para cualquier función async, no solo
// BankMovement.bulkWrite. Mockea mongoose completo: no requiere una conexión
// real a Mongo, solo verifica que conTransaccion() decida bien según la
// topología reportada y maneje commit/abort/fallback correctamente.
jest.mock('mongoose', () => ({
  connection: {
    client: { topology: { description: { type: 'Single' } } },
    startSession: jest.fn(),
  },
}));

const mongoose = require('mongoose');
const { conTransaccion } = require('./mongo-tx');

function setTopology(type) {
  mongoose.connection.client.topology.description.type = type;
}

function buildFakeSession() {
  return {
    startTransaction:  jest.fn(),
    commitTransaction: jest.fn().mockResolvedValue(undefined),
    abortTransaction:  jest.fn().mockResolvedValue(undefined),
    endSession:        jest.fn().mockResolvedValue(undefined),
    inTransaction:     jest.fn().mockReturnValue(true),
  };
}

describe('conTransaccion', () => {
  beforeEach(() => {
    mongoose.connection.startSession.mockReset();
  });

  test('topología standalone (Single): NO abre sesión, invoca fn(null) directo', async () => {
    setTopology('Single');
    const fn = jest.fn().mockResolvedValue('resultado-standalone');

    const resultado = await conTransaccion(fn);

    expect(resultado).toBe('resultado-standalone');
    expect(fn).toHaveBeenCalledTimes(1);
    expect(fn).toHaveBeenCalledWith(null);
    expect(mongoose.connection.startSession).not.toHaveBeenCalled();
  });

  test('replica set: abre sesión, corre fn(session) y comitea', async () => {
    setTopology('ReplicaSetWithPrimary');
    const session = buildFakeSession();
    mongoose.connection.startSession.mockResolvedValue(session);
    const fn = jest.fn().mockResolvedValue('resultado-tx');

    const resultado = await conTransaccion(fn);

    expect(resultado).toBe('resultado-tx');
    expect(session.startTransaction).toHaveBeenCalledTimes(1);
    expect(fn).toHaveBeenCalledWith(session);
    expect(session.commitTransaction).toHaveBeenCalledTimes(1);
    expect(session.abortTransaction).not.toHaveBeenCalled();
    expect(session.endSession).toHaveBeenCalledTimes(1);
  });

  test('replica set pero el error indica falta de soporte de transacciones: aborta y cae a fn(null)', async () => {
    setTopology('ReplicaSetNoPrimary');
    const session = buildFakeSession();
    mongoose.connection.startSession.mockResolvedValue(session);
    const err = new Error('Transaction numbers are only allowed on a replica set member or mongos');
    const fn = jest.fn()
      .mockRejectedValueOnce(err)
      .mockResolvedValueOnce('resultado-fallback');

    const resultado = await conTransaccion(fn);

    expect(resultado).toBe('resultado-fallback');
    expect(fn).toHaveBeenCalledTimes(2);
    expect(fn).toHaveBeenNthCalledWith(1, session);
    expect(fn).toHaveBeenNthCalledWith(2, null);
    expect(session.abortTransaction).toHaveBeenCalledTimes(1);
    expect(session.endSession).toHaveBeenCalledTimes(1);
  });

  test('replica set con error genérico (no de soporte de transacciones): aborta y relanza, SIN fallback', async () => {
    setTopology('Sharded');
    const session = buildFakeSession();
    mongoose.connection.startSession.mockResolvedValue(session);
    const err = new Error('duplicate key error');
    const fn = jest.fn().mockRejectedValue(err);

    await expect(conTransaccion(fn)).rejects.toThrow('duplicate key error');

    expect(fn).toHaveBeenCalledTimes(1);
    expect(session.abortTransaction).toHaveBeenCalledTimes(1);
    expect(session.endSession).toHaveBeenCalledTimes(1);
  });
});
