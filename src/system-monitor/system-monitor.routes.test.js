'use strict';

// system-monitor.routes.test.js — solo gate de permiso + wiring, mismo criterio que
// bank.routes.test.js: el service (getSnapshot) se mockea completo, la lógica real
// de agregación vive y se prueba en system-monitor.service.test.js.
jest.mock('../shared/middleware/auth', () => ({
  authenticate: (req, _res, next) => {
    req.user = { _id: 'user-test', role: 'test-role', extraPermissions: [] };
    next();
  },
  permit: (...perms) => (req, res, next) => {
    const granted = JSON.parse(req.headers['x-test-permissions'] || '[]');
    const ok = perms.every((p) => granted.includes(p));
    if (!ok) {
      return res.status(403).json({ error: 'Permisos insuficientes para esta acción.', required: perms });
    }
    next();
  },
}));

jest.mock('./system-monitor.service', () => ({
  getSnapshot: jest.fn(),
}));
jest.mock('./system-monitor-historial.service', () => ({
  getHistorial: jest.fn(),
}));

const express = require('express');
const request = require('supertest');
const router = require('./system-monitor.routes');
const { getSnapshot } = require('./system-monitor.service');
const { getHistorial } = require('./system-monitor-historial.service');
const { PERMISSIONS } = require('../shared/config/rbac');

describe('GET /snapshot', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    app = express();
    app.use('/', router);
  });

  test('responde 403 sin system:monitor:read', async () => {
    const res = await request(app).get('/snapshot').set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(res.body.required).toEqual([PERMISSIONS.SYSTEM_MONITOR_READ]);
    expect(getSnapshot).not.toHaveBeenCalled();
  });

  test('responde 200 con system:monitor:read y devuelve el snapshot del service', async () => {
    getSnapshot.mockResolvedValue({ estadoGeneral: 'normal', requestsPorMinuto: 12 });

    const res = await request(app)
      .get('/snapshot')
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.SYSTEM_MONITOR_READ]));

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ estadoGeneral: 'normal', requestsPorMinuto: 12 });
  });
});

describe('GET /historial', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    app = express();
    app.use('/', router);
  });

  test('responde 403 sin system:monitor:read', async () => {
    const res = await request(app).get('/historial').set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(getHistorial).not.toHaveBeenCalled();
  });

  test('responde 200 y pasa fechaInicio/fechaFin tal cual al service', async () => {
    getHistorial.mockResolvedValue([{ fecha: '2026-09-24', requestsPorMinuto: 10 }]);

    const res = await request(app)
      .get('/historial')
      .query({ fechaInicio: '2026-09-20', fechaFin: '2026-09-24' })
      .set('x-test-permissions', JSON.stringify([PERMISSIONS.SYSTEM_MONITOR_READ]));

    expect(res.status).toBe(200);
    expect(getHistorial).toHaveBeenCalledWith({ fechaInicio: '2026-09-20', fechaFin: '2026-09-24' });
    expect(res.body).toEqual([{ fecha: '2026-09-24', requestsPorMinuto: 10 }]);
  });

  test('sin query params: los pasa como undefined (el service decide el default)', async () => {
    getHistorial.mockResolvedValue([]);

    await request(app).get('/historial').set('x-test-permissions', JSON.stringify([PERMISSIONS.SYSTEM_MONITOR_READ]));

    expect(getHistorial).toHaveBeenCalledWith({ fechaInicio: undefined, fechaFin: undefined });
  });
});
