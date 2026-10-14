'use strict';

// Rutas de consulta de CFDIs (2026-10-14): piden visor:read, respetan las
// empresas asignadas al usuario y ya no existe import-erp-api (el navegador
// mandaba la URL a la que se conectaba el servidor).
jest.mock('../../shared/middleware/auth', () => ({
  authenticate: (req, _res, next) => {
    req.user = { _id: 'u1', role: 'test', extraPermissions: [], empresaRfcs: JSON.parse(req.headers['x-test-empresas'] || '[]') };
    next();
  },
  permit: (...perms) => (req, res, next) => (
    perms.every(p => JSON.parse(req.headers['x-test-permissions'] || '[]').includes(p))
      ? next()
      : res.status(403).json({ error: 'Permisos insuficientes para esta acción.' })
  ),
}));
jest.mock('../models/CFDI', () => ({ findById: jest.fn() }));
jest.mock('../services/cfdiSubtotalRepair', () => ({ repararSubtotalDesdeXml: jest.fn() }));

const express = require('express');
const request = require('supertest');
const CFDI    = require('../models/CFDI');
const router  = require('./cfdis');

const PROPIA = 'AAA010101AAA';
const app = express();
app.use(express.json());
app.use('/api/cfdis', router);

const conLectura = { 'x-test-permissions': JSON.stringify(['visor:read']) };
const restringido = { ...conLectura, 'x-test-empresas': JSON.stringify([PROPIA]) };

beforeEach(() => {
  CFDI.findById.mockImplementation(() => ({
    lean: async () => ({ _id: 'c1', emisor: { rfc: 'BBB020202BBB' }, receptor: { rfc: 'XAXX010101000' } }),
  }));
});

test('sin visor:read no se consultan CFDIs', async () => {
  const r = await request(app).get('/api/cfdis/c1');
  expect(r.status).toBe(403);
});

test('sin empresas asignadas se ve el CFDI como antes', async () => {
  const r = await request(app).get('/api/cfdis/c1').set(conLectura);
  expect(r.status).toBe(200);
});

test('con empresas asignadas, un CFDI de otra empresa no existe para él', async () => {
  const r = await request(app).get('/api/cfdis/c1').set(restringido);
  expect(r.status).toBe(404);
});

test('con empresas asignadas, pedir el listado de otra empresa → 403', async () => {
  const r = await request(app).get('/api/cfdis?rfcEmisor=BBB020202BBB').set(restringido);
  expect(r.status).toBe(403);
});

test('import-erp-api ya no existe', async () => {
  const r = await request(app).post('/api/cfdis/import-erp-api')
    .set({ 'x-test-permissions': JSON.stringify(['visor:write']) })
    .send({ ejercicio: 2026, periodo: 10, erpUrl: 'http://127.0.0.1:27017' });
  expect(r.status).toBe(404);
});
