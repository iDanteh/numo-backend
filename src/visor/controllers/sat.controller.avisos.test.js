'use strict';

// GET /api/sat/credenciales/avisos — el banner de credenciales SAT solo lo
// reciben admin y contabilidad (2026-10-19, pedido del usuario).

jest.mock('../jobs/satSyncJob', () => ({}));
jest.mock('../services/periodoFiscal.service', () => ({}));
jest.mock('../sat/credenciales', () => ({}));
jest.mock('../sat/rateLimiter', () => ({}));
jest.mock('../models/SatJobCheckpoint', () => ({}));
jest.mock('../sat/avisoVencimiento', () => ({
  ROLES_CON_AVISO: ['admin', 'contabilidad'],
  avisosVigentes: jest.fn(async () => [{ rfc: 'CCO011113663', estado: 'por_vencer' }]),
}));

const { avisosVigentes } = require('../sat/avisoVencimiento');
const { getAvisosCredenciales } = require('./sat.controller');

const llamar = async (role) => {
  const res = {};
  await new Promise((resolve, reject) => {
    res.json = jest.fn(resolve);
    getAvisosCredenciales({ user: role === undefined ? undefined : { role } }, res, reject);
  });
  return res.json.mock.calls[0][0];
};

beforeEach(() => jest.clearAllMocks());

test.each(['admin', 'contabilidad'])('%s recibe los avisos', async (role) => {
  expect(await llamar(role)).toEqual({ avisos: [{ rfc: 'CCO011113663', estado: 'por_vencer' }] });
});

test.each(['cobranza', 'tienda', 'visor', undefined])('%s no recibe nada (ni se consulta)', async (role) => {
  expect(await llamar(role)).toEqual({ avisos: [] });
  expect(avisosVigentes).not.toHaveBeenCalled();
});
