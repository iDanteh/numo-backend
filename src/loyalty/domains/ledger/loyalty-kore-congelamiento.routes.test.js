'use strict';

// loyalty-kore-congelamiento.routes.test.js — request simulado end-to-end contra la app Express
// real (supertest), solo gate de auth + wiring (mismo criterio que loyalty-kore-venta.routes.test.js):
// la lógica real de negocio ya se prueba contra modelos mockeados en
// loyalty-kore-congelamiento.service.test.js. Acá el servicio también se mockea — este archivo NUNCA
// toca Postgres real, solo verifica que el HTTP (auth, body, status codes) esté bien cableado.
jest.mock('../../../shared/middleware/kore-api-key-auth', () => ({
  verifyKoreApiKey: (req, res, next) => {
    if (req.headers['x-api-key'] !== 'test-key') {
      return res.status(401).json({ error: 'X-Api-Key inválida o faltante.' });
    }
    return next();
  },
}));
jest.mock('./loyalty-kore-congelamiento.service', () => ({
  procesarCongelamientoKore: jest.fn(),
}));

const express = require('express');
const request = require('supertest');
const router = require('./loyalty-kore-congelamiento.routes');
const congelamientoService = require('./loyalty-kore-congelamiento.service');

function buildApp() {
  const app = express();
  app.use(express.json());
  app.use('/', router);
  app.use((err, _req, res, _next) => res.status(err.statusCode || 500).json({ error: err.message }));
  return app;
}

describe('POST /congelamiento', () => {
  afterEach(() => jest.clearAllMocks());

  it('401 sin X-Api-Key — nunca llega al servicio', async () => {
    const app = buildApp();
    const res = await request(app)
      .post('/congelamiento')
      .send({ evento: 'cancelacion_puntos' });

    expect(res.status).toBe(401);
    expect(congelamientoService.procesarCongelamientoKore).not.toHaveBeenCalled();
  });

  it('request simulado real — cancelacion_puntos, 200 con el body que resuelve el servicio', async () => {
    congelamientoService.procesarCongelamientoKore.mockResolvedValue({
      ok: true, evento: 'cancelacion_puntos', cardId: 'card-1', puntosCancelados: 350, saldoResultante: 0,
    });

    const app = buildApp();
    const payload = {
      evento: 'cancelacion_puntos',
      koreClienteId: 'KORE-CLI-00981',
      koreEventoId: 'KORE-EVT-00123',
      telefono: '9511771923',
      motivo: 'Sin ventas en 6 meses',
    };
    const res = await request(app)
      .post('/congelamiento')
      .set('X-Api-Key', 'test-key')
      .send(payload);

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      ok: true, evento: 'cancelacion_puntos', cardId: 'card-1', puntosCancelados: 350, saldoResultante: 0,
    });
    expect(congelamientoService.procesarCongelamientoKore).toHaveBeenCalledWith(payload);
  });

  it('request simulado real — desactivacion_tarjeta, 200 con el body que resuelve el servicio', async () => {
    congelamientoService.procesarCongelamientoKore.mockResolvedValue({
      ok: true, evento: 'desactivacion_tarjeta', cardId: 'card-1',
    });

    const app = buildApp();
    const payload = {
      evento: 'desactivacion_tarjeta',
      koreClienteId: 'KORE-CLI-00981',
      koreEventoId: 'KORE-EVT-00456',
      telefono: '9511771923',
    };
    const res = await request(app)
      .post('/congelamiento')
      .set('X-Api-Key', 'test-key')
      .send(payload);

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true, evento: 'desactivacion_tarjeta', cardId: 'card-1' });
  });

  it('request simulado real — payload inválido (evento desconocido), el error del servicio se propaga como HTTP', async () => {
    const err = new Error('evento inválido: "lo_que_sea". Debe ser uno de: cancelacion_puntos, desactivacion_tarjeta.');
    err.statusCode = 400;
    congelamientoService.procesarCongelamientoKore.mockRejectedValue(err);

    const app = buildApp();
    const res = await request(app)
      .post('/congelamiento')
      .set('X-Api-Key', 'test-key')
      .send({ evento: 'lo_que_sea' });

    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/evento inválido/);
  });

  it('request simulado real — teléfono que no coincide (409) se propaga como HTTP', async () => {
    const err = new Error('El teléfono recibido ("9510000000") no coincide con el registrado para koreClienteId="KORE-CLI-00981" ("9511771923") — se rechaza el evento para evitar tocar la tarjeta equivocada.');
    err.statusCode = 409;
    congelamientoService.procesarCongelamientoKore.mockRejectedValue(err);

    const app = buildApp();
    const res = await request(app)
      .post('/congelamiento')
      .set('X-Api-Key', 'test-key')
      .send({
        evento: 'cancelacion_puntos', koreClienteId: 'KORE-CLI-00981', koreEventoId: 'KORE-EVT-1', telefono: '9510000000',
      });

    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/no coincide/);
  });
});
