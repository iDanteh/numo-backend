'use strict';

// loyalty-kore-cliente.routes.test.js — solo gate de auth + wiring (mismo criterio que
// loyalty-kore-venta.routes.test.js): la lógica real vive y se prueba en
// loyalty-kore-cliente.service.test.js.
jest.mock('../../../shared/middleware/kore-api-key-auth', () => ({
  verifyKoreApiKey: (req, _res, next) => next(),
}));
jest.mock('./loyalty-kore-cliente.service', () => ({
  procesarClienteKore: jest.fn(),
}));

const express = require('express');
const request = require('supertest');
const router = require('./loyalty-kore-cliente.routes');
const clienteService = require('./loyalty-kore-cliente.service');

function freshApp() {
  const app = express();
  app.use(express.json());
  app.use('/', router);
  app.use((err, _req, res, _next) => res.status(err.statusCode || 500).json({ error: err.message }));
  return app;
}

beforeEach(() => jest.clearAllMocks());

describe('POST /clientes', () => {
  test('no requiere sesión Numo (solo verifyKoreApiKey, mockeado como no-op) y pasa el body al service', async () => {
    const body = { evento: 'alta', koreClienteId: 'K1', telefono: '5551234567', nombre: 'Juan' };
    clienteService.procesarClienteKore.mockResolvedValue({ ok: true, evento: 'alta', cardId: 1, creada: true });

    const res = await request(freshApp()).post('/clientes').send(body);

    expect(res.status).toBe(200);
    expect(clienteService.procesarClienteKore).toHaveBeenCalledWith(expect.objectContaining(body));
    expect(res.body).toEqual({ ok: true, evento: 'alta', cardId: 1, creada: true });
  });

  test('propaga el statusCode del error del service (ej. 400) en vez de 500 genérico', async () => {
    clienteService.procesarClienteKore.mockRejectedValue(Object.assign(new Error('evento inválido'), { statusCode: 400 }));

    const res = await request(freshApp()).post('/clientes').send({ evento: 'x' });

    expect(res.status).toBe(400);
  });
});
