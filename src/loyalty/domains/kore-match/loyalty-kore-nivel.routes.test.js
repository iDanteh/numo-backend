'use strict';

// loyalty-kore-nivel.routes.test.js — solo gate de auth + wiring (mismo criterio que
// loyalty-kore-cliente.routes.test.js): la lógica real vive y se prueba en
// loyalty-kore-nivel.service.test.js.
jest.mock('../../../shared/middleware/kore-api-key-auth', () => ({
  verifyKoreApiKey: (req, _res, next) => next(),
}));
jest.mock('./loyalty-kore-nivel.service', () => ({
  procesarNivelKore: jest.fn(),
}));

const express = require('express');
const request = require('supertest');
const router = require('./loyalty-kore-nivel.routes');
const nivelService = require('./loyalty-kore-nivel.service');

function freshApp() {
  const app = express();
  app.use(express.json());
  app.use('/', router);
  app.use((err, _req, res, _next) => res.status(err.statusCode || 500).json({ error: err.message }));
  return app;
}

beforeEach(() => jest.clearAllMocks());

describe('POST /niveles', () => {
  test('no requiere sesión Numo (solo verifyKoreApiKey, mockeado como no-op) y pasa el body al service', async () => {
    const body = { evento: 'alta', koreId: 'n1', nombre: 'Bronce', porcentaje: 1.5 };
    nivelService.procesarNivelKore.mockResolvedValue({ ok: true, evento: 'alta', nivelId: 1, creado: true });

    const res = await request(freshApp()).post('/niveles').send(body);

    expect(res.status).toBe(200);
    expect(nivelService.procesarNivelKore).toHaveBeenCalledWith(expect.objectContaining(body));
    expect(res.body).toEqual({ ok: true, evento: 'alta', nivelId: 1, creado: true });
  });

  test('propaga el statusCode del error del service (ej. 400) en vez de 500 genérico', async () => {
    nivelService.procesarNivelKore.mockRejectedValue(Object.assign(new Error('evento inválido'), { statusCode: 400 }));

    const res = await request(freshApp()).post('/niveles').send({ evento: 'x' });

    expect(res.status).toBe(400);
  });
});
