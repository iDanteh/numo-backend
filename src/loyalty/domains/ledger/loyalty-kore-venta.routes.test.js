'use strict';

// loyalty-kore-venta.routes.test.js — solo gate de auth + wiring (mismo criterio que
// loyalty.routes.test.js/collection-request.routes.test.js): la lógica real vive y se
// prueba en loyalty-kore-venta.service.test.js.
jest.mock('../../../shared/middleware/kore-api-key-auth', () => ({
  verifyKoreApiKey: (req, _res, next) => next(),
}));
jest.mock('../../../shared/middleware/auth', () => ({
  authenticate: (req, _res, next) => { req.user = { dbId: '1', nombre: 'Tester' }; next(); },
  permit: (...perms) => (req, res, next) => {
    const granted = JSON.parse(req.headers['x-test-permissions'] || '[]');
    if (!perms.every((p) => granted.includes(p))) {
      return res.status(403).json({ error: 'Permisos insuficientes para esta acción.' });
    }
    next();
  },
}));
jest.mock('./loyalty-kore-venta.service', () => ({
  consultarSaldoKore: jest.fn(),
  procesarVentaKore: jest.fn(),
  procesarLoteVentasKore: jest.fn(),
}));
jest.mock('./loyalty-venta-fallida.service', () => ({
  listarVentasFallidas: jest.fn(),
  marcarVentaFallidaResuelta: jest.fn(),
}));

const express = require('express');
const request = require('supertest');
const router = require('./loyalty-kore-venta.routes');
const ventaService = require('./loyalty-kore-venta.service');
const ventaFallidaService = require('./loyalty-venta-fallida.service');
const { PERMISSIONS } = require('../../../shared/config/rbac');

function perms(...list) { return { 'x-test-permissions': JSON.stringify(list) }; }

function freshApp() {
  const app = express();
  app.use(express.json());
  app.use('/', router);
  app.use((err, _req, res, _next) => res.status(err.statusCode || 500).json({ error: err.message }));
  return app;
}

beforeEach(() => jest.clearAllMocks());

describe('GET /saldo', () => {
  test('no requiere sesión Numo (solo verifyKoreApiKey, mockeado como no-op)', async () => {
    ventaService.consultarSaldoKore.mockResolvedValue({ tieneTarjeta: true, saldoDisponible: 100, nivel: 'Plata' });
    const res = await request(freshApp()).get('/saldo').query({ telefono: '5512345678' });
    expect(res.status).toBe(200);
    expect(ventaService.consultarSaldoKore).toHaveBeenCalledWith({ telefono: '5512345678', koreClienteId: undefined });
  });
});

describe('POST /venta', () => {
  test('pasa el body completo al service y responde lo que devuelva', async () => {
    const body = { serieFolio: 'A0-1', telefono: '5512345678' };
    ventaService.procesarVentaKore.mockResolvedValue({ serieFolio: 'A0-1', status: 'aplicado' });
    const res = await request(freshApp()).post('/venta').send(body);
    expect(res.status).toBe(200);
    expect(ventaService.procesarVentaKore).toHaveBeenCalledWith(expect.objectContaining(body));
    expect(res.body).toEqual({ serieFolio: 'A0-1', status: 'aplicado' });
  });
});

describe('POST /ventas (lote)', () => {
  test('no requiere sesión Numo (solo verifyKoreApiKey, mockeado como no-op) y pasa el arreglo al service', async () => {
    const ventas = [{ serieFolio: 'A0-1' }, { serieFolio: 'A0-2' }];
    ventaService.procesarLoteVentasKore.mockResolvedValue({
      resultados: [
        { serieFolio: 'A0-1', success: true, status: 'aplicado' },
        { serieFolio: 'A0-2', success: false, error: 'algo falló', statusCode: 400 },
      ],
    });
    const res = await request(freshApp()).post('/ventas').send({ ventas });
    expect(res.status).toBe(200);
    expect(ventaService.procesarLoteVentasKore).toHaveBeenCalledWith(ventas);
    expect(res.body.resultados).toHaveLength(2);
  });
});

describe('GET /ventas-fallidas — requiere loyalty:config, sesión Numo', () => {
  test('403 sin el permiso', async () => {
    const res = await request(freshApp()).get('/ventas-fallidas');
    expect(res.status).toBe(403);
  });

  test('200 con el permiso', async () => {
    ventaFallidaService.listarVentasFallidas.mockResolvedValue([]);
    const res = await request(freshApp()).get('/ventas-fallidas').set(perms(PERMISSIONS.LOYALTY_CONFIG));
    expect(res.status).toBe(200);
  });
});

describe('PATCH /ventas-fallidas/:id/resolver', () => {
  test('403 sin loyalty:config', async () => {
    const res = await request(freshApp()).patch('/ventas-fallidas/1/resolver');
    expect(res.status).toBe(403);
  });

  test('200 con el permiso, pasa req.user al service', async () => {
    ventaFallidaService.marcarVentaFallidaResuelta.mockResolvedValue({ id: 1, resuelto: true });
    const res = await request(freshApp()).patch('/ventas-fallidas/1/resolver').set(perms(PERMISSIONS.LOYALTY_CONFIG));
    expect(res.status).toBe(200);
    expect(ventaFallidaService.marcarVentaFallidaResuelta).toHaveBeenCalledWith('1', expect.objectContaining({ dbId: '1' }));
  });
});
