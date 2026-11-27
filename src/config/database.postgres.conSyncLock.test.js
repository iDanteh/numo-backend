'use strict';

// database.postgres.conSyncLock.test.js — cubre SOLO _conSyncLock() (el advisory lock nuevo
// alrededor de syncModels(), ver docstring en database.postgres.js). El resto del archivo
// (sequelize, connectPostgres/disconnectPostgres reales) no tiene suite propia — fuera de
// alcance acá, no se toca.

const mockClient = {
  connect: jest.fn(),
  query: jest.fn(),
  end: jest.fn(),
};

jest.mock('pg', () => ({
  Client: jest.fn(() => mockClient),
}));

const { _conSyncLock } = require('./database.postgres');

describe('_conSyncLock', () => {
  beforeEach(() => jest.clearAllMocks());

  it('toma el lock, corre fn(), libera el lock y cierra la conexión — en ese orden', async () => {
    const orden = [];
    mockClient.connect.mockImplementation(async () => orden.push('connect'));
    mockClient.query.mockImplementation(async (sql) => {
      orden.push(sql.includes('pg_advisory_lock') ? 'lock' : 'unlock');
    });
    mockClient.end.mockImplementation(async () => orden.push('end'));
    const fn = jest.fn(async () => orden.push('fn'));

    await _conSyncLock(fn);

    expect(orden).toEqual(['connect', 'lock', 'fn', 'unlock', 'end']);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('libera el lock y cierra la conexión aunque fn() falle — nunca deja el lock colgado', async () => {
    mockClient.connect.mockResolvedValue();
    mockClient.query.mockResolvedValue();
    mockClient.end.mockResolvedValue();
    const fn = jest.fn(async () => { throw new Error('syncModels explotó'); });

    await expect(_conSyncLock(fn)).rejects.toThrow('syncModels explotó');

    expect(mockClient.query).toHaveBeenCalledWith('SELECT pg_advisory_unlock($1)', [expect.any(Number)]);
    expect(mockClient.end).toHaveBeenCalledTimes(1);
  });

  it('si pg_advisory_unlock falla, igual cierra la conexión (no deja el proceso colgado)', async () => {
    mockClient.connect.mockResolvedValue();
    mockClient.query.mockImplementation(async (sql) => {
      if (sql.includes('unlock')) throw new Error('conexión ya cerrada del otro lado');
    });
    mockClient.end.mockResolvedValue();
    const fn = jest.fn().mockResolvedValue();

    await expect(_conSyncLock(fn)).resolves.toBeUndefined();
    expect(mockClient.end).toHaveBeenCalledTimes(1);
  });
});
