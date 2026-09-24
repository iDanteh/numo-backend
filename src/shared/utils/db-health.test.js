'use strict';

jest.mock('mongoose', () => ({ connection: { readyState: 0 } }));
jest.mock('../../config/database.postgres', () => ({ sequelize: { authenticate: jest.fn() } }));

const mongoose = require('mongoose');
const { sequelize } = require('../../config/database.postgres');
const { checkMongoOk, checkPostgresOk } = require('./db-health');

describe('db-health', () => {
  test('checkMongoOk: true solo con readyState 1 (connected)', () => {
    mongoose.connection.readyState = 1;
    expect(checkMongoOk()).toBe(true);

    mongoose.connection.readyState = 0;
    expect(checkMongoOk()).toBe(false);
  });

  test('checkPostgresOk: true cuando sequelize.authenticate() resuelve', async () => {
    sequelize.authenticate.mockResolvedValueOnce(undefined);
    expect(await checkPostgresOk()).toBe(true);
  });

  test('checkPostgresOk: false cuando sequelize.authenticate() rechaza (no propaga la excepción)', async () => {
    sequelize.authenticate.mockRejectedValueOnce(new Error('conexión rechazada'));
    expect(await checkPostgresOk()).toBe(false);
  });
});
