'use strict';

// collection-request.routes.test.js — primer test de este router (no existía ninguno).
// Cobertura mínima del contrato HTTP de _resolveScopeUserId() (2026-09-07, filtro
// admin-only de contador(es) para el panel de indicadores) — hasta ahora solo estaba
// verificado por lectura de código (hallazgo 1 de la revisión de confiabilidad
// independiente). Mismo criterio de "mockear los límites de I/O" que
// collection-request.identificar.test.js (los otros tests de este dominio que sí requieren
// el service real): CollectionRequest.model, BankMovement.model, bank.service,
// kore-caja.service, erp.routes, mongo-tx, socket y logger se mockean para que requerir
// collection-request.service.js (dependencia del router) no dispare I/O real ni conexiones
// a Mongo/Kore a nivel de módulo. collection-request-indicadores.service.js se mockea
// aparte, completo (solo se prueba el gate de permiso + el paso de scopeUserId, no la
// agregación — esa lógica ya tiene su propio test,
// collection-request-indicadores.service.test.js) — mismo criterio que bank.routes.test.js
// mockeando bank-indicadores.service.js.
jest.mock('./CollectionRequest.model');
jest.mock('../banks/BankMovement.model');
jest.mock('../banks/bank.service');
jest.mock('../erp/kore-caja.service');
jest.mock('../erp/erp.routes');
jest.mock('../../shared/utils/mongo-tx');
jest.mock('../../shared/socket');
jest.mock('../../shared/utils/logger');
jest.mock('./collection-request-indicadores.service', () => ({
  getIndicadoresSolicitudesCobro:  jest.fn(),
  getDistribucionSolicitudesCobro: jest.fn(),
  listContadoresConSolicitudesIdentificadas: jest.fn(),
}));

// `mockReqUser` (prefijo "mock" a propósito, no es un capricho de nombre): variable
// mutable referenciada dentro del factory de jest.mock('.../auth.real') para poder variar
// req.user.role/._id por test — es el idioma estándar de Jest para inyectar estado
// dinámico en un factory hoisteado (babel-plugin-jest-hoist solo permite referenciar,
// desde dentro de un factory de jest.mock, identificadores que empiecen con "mock").
// Ningún test previo de este proyecto necesitaba admin vs. no-admin en el mismo router
// (bank.routes.test.js/erp-reversion.routes.test.js siempre usan un role fijo
// 'test-role', variando solo el permiso vía header x-test-permissions) — acá SÍ hace
// falta variar el rol, porque _resolveScopeUserId() depende de req.user.role/_id, no solo
// del permiso.
let mockReqUser = { _id: 'user-test', role: 'test-role', extraPermissions: [] };

jest.mock('../../shared/middleware/auth.real', () => ({
  authenticate: (req, _res, next) => {
    req.user = mockReqUser;
    next();
  },
  permit: (...perms) => (req, res, next) => {
    const granted = JSON.parse(req.headers['x-test-permissions'] || '[]');
    const ok = perms.every(p => granted.includes(p));
    if (!ok) {
      return res.status(403).json({ error: 'Permisos insuficientes para esta acción.', required: perms });
    }
    next();
  },
}));

const express = require('express');
const request = require('supertest');
const router  = require('./collection-request.routes');
const indicadoresService = require('./collection-request-indicadores.service');
const { PERMISSIONS } = require('../../../shared/config/rbac');

const ALLOWED = JSON.stringify([PERMISSIONS.COLLECTIONS_READ]);

describe('_resolveScopeUserId() vía GET /indicadores y GET /indicadores/distribucion (2026-09-07)', () => {
  let app;

  beforeEach(() => {
    jest.clearAllMocks();
    mockReqUser = { _id: 'user-test', role: 'test-role', extraPermissions: [] };
    indicadoresService.getIndicadoresSolicitudesCobro.mockResolvedValue({});
    indicadoresService.getDistribucionSolicitudesCobro.mockResolvedValue({});
    indicadoresService.listContadoresConSolicitudesIdentificadas.mockResolvedValue([]);
    app = express();
    app.use(express.json());
    app.use('/', router);
  });

  describe('GET /indicadores', () => {
    test('responde 403 sin collections:read (nunca llega a llamar al service)', async () => {
      const res = await request(app).get('/indicadores').set('x-test-permissions', JSON.stringify([]));

      expect(res.status).toBe(403);
      expect(res.body.required).toEqual([PERMISSIONS.COLLECTIONS_READ]);
      expect(indicadoresService.getIndicadoresSolicitudesCobro).not.toHaveBeenCalled();
    });

    test('admin con ?userIds=id1,id2 -> el service se llama con scopeUserId: [\'id1\',\'id2\']', async () => {
      mockReqUser = { _id: 'admin-1', role: 'admin', extraPermissions: [] };

      const res = await request(app)
        .get('/indicadores')
        .query({ userIds: 'id1,id2' })
        .set('x-test-permissions', ALLOWED);

      expect(res.status).toBe(200);
      expect(indicadoresService.getIndicadoresSolicitudesCobro).toHaveBeenCalledTimes(1);
      const args = indicadoresService.getIndicadoresSolicitudesCobro.mock.calls[0][0];
      expect(args.scopeUserId).toEqual(['id1', 'id2']);
    });

    test('admin sin ?userIds -> el service se llama con scopeUserId: undefined (todo el equipo)', async () => {
      mockReqUser = { _id: 'admin-1', role: 'admin', extraPermissions: [] };

      await request(app).get('/indicadores').set('x-test-permissions', ALLOWED);

      const args = indicadoresService.getIndicadoresSolicitudesCobro.mock.calls[0][0];
      expect(args.scopeUserId).toBeUndefined();
    });

    test('no-admin con ?userIds=algo -> se ignora, el service se llama con scopeUserId: req.user._id (su propio id), NO con el query param', async () => {
      mockReqUser = { _id: 'contador-1', role: 'contabilidad', extraPermissions: [] };

      await request(app)
        .get('/indicadores')
        .query({ userIds: 'otro-id-que-no-es-el-suyo' })
        .set('x-test-permissions', ALLOWED);

      const args = indicadoresService.getIndicadoresSolicitudesCobro.mock.calls[0][0];
      expect(args.scopeUserId).toBe('contador-1');
    });

    test('?userIds= vacío cae a undefined (no un array vacío)', async () => {
      mockReqUser = { _id: 'admin-1', role: 'admin', extraPermissions: [] };

      await request(app)
        .get('/indicadores')
        .query({ userIds: '' })
        .set('x-test-permissions', ALLOWED);

      const args = indicadoresService.getIndicadoresSolicitudesCobro.mock.calls[0][0];
      expect(args.scopeUserId).toBeUndefined();
    });

    test('?userIds= solo comas/espacios cae a undefined (no un array vacío)', async () => {
      mockReqUser = { _id: 'admin-1', role: 'admin', extraPermissions: [] };

      await request(app)
        .get('/indicadores')
        .query({ userIds: ' , ,  ,' })
        .set('x-test-permissions', ALLOWED);

      const args = indicadoresService.getIndicadoresSolicitudesCobro.mock.calls[0][0];
      expect(args.scopeUserId).toBeUndefined();
    });
  });

  describe('GET /indicadores/distribucion — mismo _resolveScopeUserId compartido con /indicadores', () => {
    test('responde 403 sin collections:read', async () => {
      const res = await request(app)
        .get('/indicadores/distribucion')
        .set('x-test-permissions', JSON.stringify([]));

      expect(res.status).toBe(403);
      expect(indicadoresService.getDistribucionSolicitudesCobro).not.toHaveBeenCalled();
    });

    test('admin con ?userIds=id1,id2 -> el service se llama con scopeUserId: [\'id1\',\'id2\']', async () => {
      mockReqUser = { _id: 'admin-1', role: 'admin', extraPermissions: [] };

      await request(app)
        .get('/indicadores/distribucion')
        .query({ userIds: 'id1,id2' })
        .set('x-test-permissions', ALLOWED);

      const args = indicadoresService.getDistribucionSolicitudesCobro.mock.calls[0][0];
      expect(args.scopeUserId).toEqual(['id1', 'id2']);
    });

    test('admin sin ?userIds -> el service se llama con scopeUserId: undefined', async () => {
      mockReqUser = { _id: 'admin-1', role: 'admin', extraPermissions: [] };

      await request(app).get('/indicadores/distribucion').set('x-test-permissions', ALLOWED);

      const args = indicadoresService.getDistribucionSolicitudesCobro.mock.calls[0][0];
      expect(args.scopeUserId).toBeUndefined();
    });

    test('no-admin con ?userIds=algo -> se ignora, el service se llama con scopeUserId: req.user._id (su propio id)', async () => {
      mockReqUser = { _id: 'contador-1', role: 'cobranza', extraPermissions: [] };

      await request(app)
        .get('/indicadores/distribucion')
        .query({ userIds: 'otro-id-que-no-es-el-suyo' })
        .set('x-test-permissions', ALLOWED);

      const args = indicadoresService.getDistribucionSolicitudesCobro.mock.calls[0][0];
      expect(args.scopeUserId).toBe('contador-1');
    });

    test('?userIds= vacío o solo comas/espacios cae a undefined (no un array vacío)', async () => {
      mockReqUser = { _id: 'admin-1', role: 'admin', extraPermissions: [] };

      await request(app)
        .get('/indicadores/distribucion')
        .query({ userIds: ' , , ' })
        .set('x-test-permissions', ALLOWED);

      const args = indicadoresService.getDistribucionSolicitudesCobro.mock.calls[0][0];
      expect(args.scopeUserId).toBeUndefined();
    });
  });

  describe('GET /indicadores/contadores (2026-09-07, fix real: bug 1 — el <select> del filtro ofrecía usuarios que nunca resolvieron nada)', () => {
    test('responde 403 sin collections:read (nunca llega a llamar al service)', async () => {
      const res = await request(app).get('/indicadores/contadores').set('x-test-permissions', JSON.stringify([]));

      expect(res.status).toBe(403);
      expect(indicadoresService.listContadoresConSolicitudesIdentificadas).not.toHaveBeenCalled();
    });

    test('con collections:read responde { userIds } tal cual lo devuelve el service, mismo permiso que /indicadores (no es admin-only)', async () => {
      mockReqUser = { _id: 'contador-1', role: 'contabilidad', extraPermissions: [] };
      indicadoresService.listContadoresConSolicitudesIdentificadas.mockResolvedValue(['u1', 'u2']);

      const res = await request(app).get('/indicadores/contadores').set('x-test-permissions', ALLOWED);

      expect(res.status).toBe(200);
      expect(res.body).toEqual({ userIds: ['u1', 'u2'] });
      expect(indicadoresService.listContadoresConSolicitudesIdentificadas).toHaveBeenCalledTimes(1);
    });
  });
});
