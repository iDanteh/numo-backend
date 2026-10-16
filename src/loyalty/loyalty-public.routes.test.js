'use strict';

// loyalty-public.routes.test.js — PR4/8 sistema-lealtad. Usa un `express-rate-limit`
// REAL (no mockeado), mismo criterio que el rate limiter de `cfdis.js`. Cada
// test que depende del estado del limiter construye su PROPIA app con
// `jest.resetModules()` + un `require()` fresco de `./loyalty-public.routes` —
// el router (y por lo tanto sus dos `rateLimit(...)`, con su propia
// `MemoryStore`) se instancia una sola vez al cargar el módulo, así que sin
// esto todos los tests de este archivo compartirían el mismo contador por IP.
// Verifica: NO hay `authenticate` en la cadena (D9), el método correcto es
// POST (GET → 404), el límite por minuto corta con 429 antes de correr
// cualquier lógica de negocio, y las fallas de negocio son 422 con el `code`
// que `error-handler.js` ahora propaga desde `err.code`.
jest.mock('./domains/public/loyalty-public.service');

const express = require('express');
const request = require('supertest');
const errorHandler = require('../shared/middleware/error-handler');

/** Re-requiere el router (y su service mockeado) en un registro de módulos
 * limpio, para que cada test tenga su propia `MemoryStore` de rate limiting. */
function freshApp() {
  jest.resetModules();
  const publicService = require('./domains/public/loyalty-public.service');
  const loyaltyPublicRoutes = require('./loyalty-public.routes');

  const app = express();
  app.use(express.json());
  app.use('/api/loyalty/public', loyaltyPublicRoutes);
  app.use(errorHandler);

  return { app, publicService, loyaltyPublicRoutes };
}

test('no hay authenticate en la cadena: sin Authorization igual llega al service', async () => {
  const { app, publicService } = freshApp();
  publicService.consultaPublica.mockResolvedValue({ nombre: 'Ana', nivel: 'Plata', saldoPuntos: '10.00' });

  const res = await request(app).post('/api/loyalty/public/consulta').send({ telefono: '5512345678', nip: '1234' });

  expect(res.status).toBe(200);
  expect(publicService.consultaPublica).toHaveBeenCalled();
});

test('GET /consulta no existe (solo POST) — D8: un GET filtraría el NIP a los logs de tráfico vía querystring', async () => {
  const { app } = freshApp();
  const res = await request(app).get('/api/loyalty/public/consulta').query({ telefono: '5512345678', nip: '1234' });
  expect(res.status).toBe(404);
});

test('falla de negocio (422 LOYALTY_LOOKUP_FAILED) se propaga con el mismo código', async () => {
  const { app, publicService } = freshApp();
  const err = Object.assign(new Error('No se encontró ninguna tarjeta con ese teléfono y NIP.'), {
    statusCode: 422, code: 'LOYALTY_LOOKUP_FAILED',
  });
  publicService.consultaPublica.mockRejectedValue(err);

  const res = await request(app).post('/api/loyalty/public/consulta').send({ telefono: '5512345678', nip: '0000' });

  expect(res.status).toBe(422);
  expect(res.body.code).toBe('LOYALTY_LOOKUP_FAILED');
});

describe('rate limiting dedicado (distinto del limiter global de /api/)', () => {
  test('el 6to request en la misma ventana de 1 min → 429, antes de llamar al service', async () => {
    const { app, publicService } = freshApp();
    publicService.consultaPublica.mockResolvedValue({ nombre: 'Ana', nivel: 'Plata', saldoPuntos: '10.00' });

    for (let i = 0; i < 5; i++) {
      // eslint-disable-next-line no-await-in-loop
      const r = await request(app).post('/api/loyalty/public/consulta').send({ telefono: '5512345678', nip: '1234' });
      expect(r.status).toBe(200);
    }

    const sexto = await request(app).post('/api/loyalty/public/consulta').send({ telefono: '5512345678', nip: '1234' });
    expect(sexto.status).toBe(429);
    expect(sexto.body.code).toBe('RATE_LIMIT_EXCEEDED');
    expect(publicService.consultaPublica).toHaveBeenCalledTimes(5);
  });

  test('el límite de una app NO contamina a otra app fresca (MemoryStore aislada por test)', async () => {
    const primera = freshApp();
    primera.publicService.consultaPublica.mockResolvedValue({ nombre: 'Ana', nivel: 'Plata', saldoPuntos: '1.00' });
    for (let i = 0; i < 5; i++) {
      // eslint-disable-next-line no-await-in-loop
      await request(primera.app).post('/api/loyalty/public/consulta').send({ telefono: '5512345678', nip: '1234' });
    }
    const bloqueadaEnPrimera = await request(primera.app).post('/api/loyalty/public/consulta').send({ telefono: '5512345678', nip: '1234' });
    expect(bloqueadaEnPrimera.status).toBe(429);

    const segunda = freshApp();
    segunda.publicService.consultaPublica.mockResolvedValue({ nombre: 'Ana', nivel: 'Plata', saldoPuntos: '1.00' });
    const primeraEnSegunda = await request(segunda.app).post('/api/loyalty/public/consulta').send({ telefono: '5512345678', nip: '1234' });
    expect(primeraEnSegunda.status).toBe(200);
  });
});

test('crearLimitersConsulta() exporta una factory (MemoryStore fresca por llamada, no singleton)', () => {
  const { loyaltyPublicRoutes } = freshApp();
  const { crearLimitersConsulta } = loyaltyPublicRoutes;
  expect(typeof crearLimitersConsulta).toBe('function');
  const par1 = crearLimitersConsulta();
  const par2 = crearLimitersConsulta();
  expect(Array.isArray(par1)).toBe(true);
  expect(par1).toHaveLength(2);
  expect(par1[0]).not.toBe(par2[0]);
});
