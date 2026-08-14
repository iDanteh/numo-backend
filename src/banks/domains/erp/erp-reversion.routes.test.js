'use strict';

// erp-reversion.routes.test.js — webhook server-to-server que consume Kore para avisar que
// revirtió/canceló una CxC ya vinculada a un depósito bancario, más la bandeja de auditoría
// (GET, solo lectura) que usa la UI con sesión Numo normal.
//
// Mismo criterio de "mockear los límites de I/O" que erp.routes.test.js /
// bank.service.setErpIds.test.js: se mockea auth.real (authenticate/permit vía headers de
// prueba), BankMovement.model y ErpReversion.model (Mongoose) y shared/socket
// (emitToBanco). aplicarLogicaErp (bank.service.js) corre REAL — es lógica pura, igual que
// hace bank.service.setErpIds.test.js con setErpIds.
jest.mock('../../shared/middleware/auth.real', () => ({
  authenticate: (req, _res, next) => {
    req.user = { _id: 'user-test', email: 'test@example.com', role: 'test-role', extraPermissions: [] };
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

jest.mock('../banks/BankMovement.model');
jest.mock('./ErpReversion.model');
jest.mock('../../shared/socket');

const express      = require('express');
const request      = require('supertest');
const router       = require('./erp-reversion.routes');
const errorHandler = require('../../shared/middleware/error-handler');
const BankMovement = require('../banks/BankMovement.model');
const ErpReversion = require('./ErpReversion.model');
const { emitToBanco } = require('../../shared/socket');
const { PERMISSIONS } = require('../../../shared/config/rbac');

const API_KEY = 'test-kore-api-key';

function buildApp() {
  const app = express();
  app.use(express.json());
  app.use('/api/erp/cxc-reversiones', router);
  app.use(errorHandler);
  return app;
}

function fakeMov(overrides = {}) {
  return {
    _id: 'mov-1', banco: 'BBVA', erpIds: [], erpLinks: [], identificadoPor: [],
    status: 'no_identificado', deposito: null, retiro: null, saldoErp: null, uuidXML: null,
    save: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

const ALLOWED = JSON.stringify([PERMISSIONS.BANKS_ERP_REVERSIONES]);

let app;

beforeEach(() => {
  jest.clearAllMocks();
  process.env.KORE_API_KEY = API_KEY;
  app = buildApp();
});

afterAll(() => {
  delete process.env.KORE_API_KEY;
});

describe('POST /api/erp/cxc-reversiones — autenticación por API key', () => {
  test('401 sin X-Api-Key', async () => {
    const res = await request(app).post('/api/erp/cxc-reversiones').send({ erpId: 'CXC-1' });

    expect(res.status).toBe(401);
    expect(res.body).toEqual({ error: 'API key inválida o ausente.' });
  });

  test('401 con X-Api-Key incorrecta', async () => {
    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', 'llave-equivocada')
      .send({ erpId: 'CXC-1' });

    expect(res.status).toBe(401);
    expect(res.body).toEqual({ error: 'API key inválida o ausente.' });
  });

  test('503 si KORE_API_KEY no está configurada en el servidor', async () => {
    delete process.env.KORE_API_KEY;

    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', 'cualquier-cosa')
      .send({ erpId: 'CXC-1' });

    expect(res.status).toBe(503);
    expect(res.body).toEqual({ error: 'Integración con Kore no configurada en el servidor' });
  });
});

describe('POST /api/erp/cxc-reversiones — validación de body', () => {
  test('400 sin erpId', async () => {
    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({});

    expect(res.status).toBe(400);
    expect(res.body).toEqual({ error: 'Se requiere erpId.' });
  });

  test('400 sin serieExterna', async () => {
    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-1', folioExterno: '100' });

    expect(res.status).toBe(400);
    expect(res.body).toEqual({ error: 'Se requiere serieExterna.' });
  });

  test('400 sin folioExterno', async () => {
    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-1', serieExterna: 'A0' });

    expect(res.status).toBe(400);
    expect(res.body).toEqual({ error: 'Se requiere folioExterno.' });
  });

  test('400 con referencia mal formada', async () => {
    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100', referencia: 'abc' });

    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/referencia/i);
  });
});

describe('POST /api/erp/cxc-reversiones — procesa la reversión', () => {
  test('200 con un movimiento afectado: erpIds/erpLinks/status se recalculan', async () => {
    const mov = fakeMov({
      erpIds:   ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 500, total: 500, serie: 'A0', folioExterno: '100' }],
      deposito: 500,
      status:   'identificado',
    });
    BankMovement.find.mockResolvedValue([mov]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-1' });

    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true, movimientosAfectados: 1, yaEstabaDesvinculada: false });
    expect(BankMovement.find).toHaveBeenCalledWith({ erpIds: 'CXC-1' });
    expect(mov.erpIds).toEqual([]);
    expect(mov.erpLinks).toEqual([]);
    expect(mov.status).toBe('no_identificado'); // ya no hay ningún link que cubra el depósito
    expect(mov.save).toHaveBeenCalledTimes(1);
    expect(emitToBanco).toHaveBeenCalledWith('BBVA', 'bank:movement:updated', expect.objectContaining({
      erpIds: [], erpLinks: [], status: 'no_identificado',
    }));
  });

  test('200 con 2 movimientos afectados por el mismo erpId', async () => {
    const link = { erpId: 'CXC-1', saldoActual: 200, total: 200 };
    const mov1 = fakeMov({ _id: 'mov-1', erpIds: ['CXC-1'], erpLinks: [{ ...link }], deposito: 200 });
    const mov2 = fakeMov({ _id: 'mov-2', erpIds: ['CXC-1'], erpLinks: [{ ...link }], deposito: 200 });
    BankMovement.find.mockResolvedValue([mov1, mov2]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-2' });

    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '200' });

    expect(res.status).toBe(200);
    expect(res.body.movimientosAfectados).toBe(2);
    expect(mov1.save).toHaveBeenCalledTimes(1);
    expect(mov2.save).toHaveBeenCalledTimes(1);
    expect(mov1.erpIds).toEqual([]);
    expect(mov2.erpIds).toEqual([]);
  });

  test('200 yaEstabaDesvinculada:true cuando no se encuentra ningún movimiento, sin persistir ErpReversion', async () => {
    BankMovement.find.mockResolvedValue([]);

    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-inexistente', serieExterna: 'A0', folioExterno: '300' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true, movimientosAfectados: 0, yaEstabaDesvinculada: true });
    // 2026-08-10: repetir el mismo evento (o mandar un erpId nunca vinculado) ya no debe
    // dejar un registro nuevo en la bandeja — nada que auditar, nada que "Revertir" pudiera
    // restaurar.
    expect(ErpReversion.create).not.toHaveBeenCalled();
  });

  test('desvinculación (webhook Kore, sin sesión real): NO limpia primeraIdentificacionAt aunque el movimiento vuelva a no_identificado', async () => {
    const previo = new Date('2026-01-01T00:00:00.000Z');
    const mov = fakeMov({
      erpIds:   ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 500, total: 500, serie: 'A0', folioExterno: '100' }],
      deposito: 500,
      status:   'identificado',
      primeraIdentificacionAt: previo,
      primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' },
    });
    BankMovement.find.mockResolvedValue([mov]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-5' });

    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100' });

    expect(res.status).toBe(200);
    expect(mov.status).toBe('no_identificado');
    // Inmutable: nunca se limpia al desvincular, aunque el webhook no tenga usuario real.
    expect(mov.primeraIdentificacionAt).toEqual(previo);
    expect(mov.primeraIdentificacionPor).toEqual({ userId: 'user-1', nombre: 'Ana' });
  });

  test('detecta serieFolioMismatch cuando el link vinculado no coincide con la serie/folio que manda Kore', async () => {
    const mov = fakeMov({
      erpIds:   ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 0, total: 0, serie: 'A0', folioExterno: '100' }],
      deposito: 0,
    });
    BankMovement.find.mockResolvedValue([mov]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-4' });

    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-1', serieExterna: 'B0', folioExterno: '999' });

    expect(res.status).toBe(200);
    expect(ErpReversion.create).toHaveBeenCalledWith(expect.objectContaining({ serieFolioMismatch: true }));
  });
});

describe('GET /api/erp/cxc-reversiones', () => {
  test('403 sin permiso banks:erp:reversiones', async () => {
    const res = await request(app)
      .get('/api/erp/cxc-reversiones')
      .set('x-test-permissions', JSON.stringify([]));

    expect(res.status).toBe(403);
    expect(res.body.required).toEqual([PERMISSIONS.BANKS_ERP_REVERSIONES]);
  });

  test('200: pasa q como filtro $or de erpId/serieExterna/folioExterno (case-insensitive)', async () => {
    const mockQuery = {
      sort:  jest.fn().mockReturnThis(),
      skip:  jest.fn().mockReturnThis(),
      limit: jest.fn().mockResolvedValue([]),
    };
    ErpReversion.find.mockReturnValue(mockQuery);
    ErpReversion.countDocuments.mockResolvedValue(0);

    const res = await request(app)
      .get('/api/erp/cxc-reversiones')
      .query({ q: 'H0' })
      .set('x-test-permissions', ALLOWED);

    expect(res.status).toBe(200);
    expect(ErpReversion.find).toHaveBeenCalledTimes(1);
    const filtroUsado = ErpReversion.find.mock.calls[0][0];
    expect(filtroUsado.$or).toEqual([
      { erpId:        expect.any(RegExp) },
      { serieExterna: expect.any(RegExp) },
      { folioExterno: expect.any(RegExp) },
    ]);
    expect(filtroUsado.$or[0].erpId.source).toBe('H0');
    expect(filtroUsado.$or[0].erpId.flags).toContain('i');
    expect(filtroUsado.$or[1].serieExterna.source).toBe('H0');
    expect(filtroUsado.$or[2].folioExterno.source).toBe('H0');
  });

  test('escapa caracteres especiales de regex en q (sin inyección de patrón)', async () => {
    const mockQuery = {
      sort:  jest.fn().mockReturnThis(),
      skip:  jest.fn().mockReturnThis(),
      limit: jest.fn().mockResolvedValue([]),
    };
    ErpReversion.find.mockReturnValue(mockQuery);
    ErpReversion.countDocuments.mockResolvedValue(0);

    const res = await request(app)
      .get('/api/erp/cxc-reversiones')
      .query({ q: 'A.(1)' })
      .set('x-test-permissions', ALLOWED);

    expect(res.status).toBe(200);
    const filtroUsado = ErpReversion.find.mock.calls[0][0];
    // Los caracteres especiales quedan escapados: el regex busca el texto LITERAL "A.(1)",
    // no lo que significarían como metacaracteres de regex sin escapar.
    expect(filtroUsado.$or[0].erpId.source).toBe('A\\.\\(1\\)');
  });
});
