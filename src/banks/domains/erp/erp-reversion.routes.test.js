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
// 2026-08-20: erp-reversion.service.js ahora reconsulta a Kore EN VIVO reusando los
// helpers que erp.routes.js re-expone en el router (mismo patrón ya usado por
// collection-request.service.js) — se mockea el módulo completo, un límite de I/O real
// (llamadas HTTP a Kore), no una implementación a probar aquí (ya tiene sus propios tests
// en erp.routes.test.js).
jest.mock('./erp.routes', () => ({
  _rangoDesdeFollo:               jest.fn(),
  _sincronizarConRetry:           jest.fn(),
  _erpIdIdentificadoPorHumano:    jest.fn(),
  _montoSaldoLinkPorMovimiento:   jest.fn(),
  _montoSaldoLinkPorAutorizacion: jest.fn(),
  _backfillFormasPagoYFolioFiscal: jest.fn(),
  _movimientosKoreDesde:          jest.fn(),
  _retencionVigente:              jest.fn(),
}));

const express      = require('express');
const request      = require('supertest');
const router       = require('./erp-reversion.routes');
const errorHandler = require('../../shared/middleware/error-handler');
const BankMovement = require('../banks/BankMovement.model');
const ErpReversion = require('./ErpReversion.model');
const erpRoutes    = require('./erp.routes');
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
  // 2026-08-20: el handler ahora loguea el payload crudo de Kore por consola (pedido
  // explícito del usuario, para poder diagnosticar reversiones que "corren pero mal") —
  // se silencia acá para no ensuciar la salida de test, igual que logger.warn más abajo.
  jest.spyOn(console, 'log').mockImplementation(() => {});
  // Default: sin datos frescos de Kore (mismo comportamiento que "no se pudo reconsultar")
  // — así los tests preexistentes (que prueban el desvinculado completo, el comportamiento
  // de respaldo) no necesitan mockear toda la cadena de reconsulta. Los tests nuevos de
  // "ajuste parcial" sobreescriben esto explícitamente.
  erpRoutes._rangoDesdeFollo.mockReturnValue(null);
});

afterEach(() => {
  console.log.mockRestore();
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

describe('POST /api/erp/cxc-reversiones — log del payload crudo (2026-08-20)', () => {
  test('loguea el body completo ANTES de validar, incluso si el request se rechaza', async () => {
    await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-1' }); // sin serieExterna/folioExterno -> 400

    expect(console.log).toHaveBeenCalledWith(
      '[erp-reversion] payload recibido de Kore →',
      JSON.stringify({ erpId: 'CXC-1' }),
    );
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

describe('POST /api/erp/cxc-reversiones — fix real 2026-08-20: reconsulta a Kore en vivo, no desvincula abonos que siguen vigentes', () => {
  function mockRangoYSync(raw0) {
    erpRoutes._rangoDesdeFollo.mockReturnValue({ fechaDesde: '2026-08-01', fechaHasta: '2026-08-31' });
    erpRoutes._sincronizarConRetry.mockResolvedValue({ raw: [raw0] });
  }

  test('CxC pagada en 2 depósitos DISTINTOS: solo se desvincula el que ya no tiene aporte, el otro queda intacto (solo corregido)', async () => {
    const movConAbonoVigente  = fakeMov({
      _id: 'mov-A', erpIds: ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 100, total: 300, saldoErpAportado: 100, serie: 'A0', folioExterno: '100' }],
      deposito: 100,
    });
    const movYaSinAporte = fakeMov({
      _id: 'mov-B', erpIds: ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 100, total: 300, saldoErpAportado: 200, serie: 'A0', folioExterno: '100' }],
      deposito: 200,
    });
    BankMovement.find.mockResolvedValue([movConAbonoVigente, movYaSinAporte]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-6' });

    mockRangoYSync({ saldoActual: 100, movimientos: [] }); // contenido real no importa, se mockean los cálculos derivados
    erpRoutes._erpIdIdentificadoPorHumano.mockReturnValue(true);
    // El movimiento reverted (mov-B) ya no tiene NADA atribuible; mov-A sigue con $100 vigentes.
    erpRoutes._montoSaldoLinkPorMovimiento.mockImplementation((raw0, mov) => (mov._id === 'mov-A' ? 100 : 0));
    erpRoutes._backfillFormasPagoYFolioFiscal.mockReturnValue({ saldoPagadoTotal: 100, saldoPagado: 100, folioFiscal: null });
    erpRoutes._movimientosKoreDesde.mockReturnValue([]);
    erpRoutes._retencionVigente.mockReturnValue({ tieneRetencion: false, montoRetenido: null });

    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100', referencia: '6a876c193bfaed00011c9216', monto: 200 });

    expect(res.status).toBe(200);
    // mov-A: NO se desvincula — sigue teniendo su erpLink, solo con el número corregido.
    expect(movConAbonoVigente.erpIds).toEqual(['CXC-1']);
    expect(movConAbonoVigente.erpLinks).toHaveLength(1);
    expect(movConAbonoVigente.erpLinks[0].saldoErpAportado).toBe(100);
    // mov-B: sin nada atribuible -> se desvincula por completo, como antes.
    expect(movYaSinAporte.erpIds).toEqual([]);
    expect(movYaSinAporte.erpLinks).toEqual([]);

    const payload = ErpReversion.create.mock.calls[0][0];
    expect(payload.movimientosAfectados).toEqual(expect.arrayContaining([
      expect.objectContaining({ movementId: 'mov-A', tipo: 'ajustado' }),
      expect.objectContaining({ movementId: 'mov-B', tipo: 'desvinculado' }),
    ]));
  });

  test('CxC con 3 abonos parciales en el MISMO depósito, se revierte 1: resta el importe, NO desvincula el depósito completo', async () => {
    const mov = fakeMov({
      _id: 'mov-1', erpIds: ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 9700, total: 10000, saldoErpAportado: 300, serie: 'A0', folioExterno: '100' }],
      deposito: 300,
    });
    BankMovement.find.mockResolvedValue([mov]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-7' });

    mockRangoYSync({ saldoActual: 9700, movimientos: [] });
    erpRoutes._erpIdIdentificadoPorHumano.mockReturnValue(true);
    // Antes de la reversión el link tenía $300 (3 abonos de $100); tras la reversión de 1,
    // la reconsulta a Kore devuelve $200 atribuibles a este mismo movimiento.
    erpRoutes._montoSaldoLinkPorMovimiento.mockReturnValue(200);
    erpRoutes._backfillFormasPagoYFolioFiscal.mockReturnValue({ saldoPagadoTotal: 200, saldoPagado: 200, folioFiscal: null });
    erpRoutes._movimientosKoreDesde.mockReturnValue([]);
    erpRoutes._retencionVigente.mockReturnValue({ tieneRetencion: false, montoRetenido: null });

    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100', referencia: '6a876c193bfaed00011c9216', monto: 100 });

    expect(res.status).toBe(200);
    // El link SIGUE existiendo (no se desvincula el depósito completo) — solo bajó el aporte.
    expect(mov.erpIds).toEqual(['CXC-1']);
    expect(mov.erpLinks).toHaveLength(1);
    expect(mov.erpLinks[0].saldoErpAportado).toBe(200);
    expect(mov.save).toHaveBeenCalledTimes(1);
    expect(ErpReversion.create).toHaveBeenCalledWith(expect.objectContaining({
      movimientosAfectados: [expect.objectContaining({
        movementId: 'mov-1', tipo: 'ajustado',
        erpLinkAjustado: expect.objectContaining({
          antes:   expect.objectContaining({ saldoErpAportado: 300 }),
          despues: expect.objectContaining({ saldoErpAportado: 200 }),
        }),
      })],
    }));
  });

  test('si no se puede reconsultar a Kore (excepción), cae al comportamiento de respaldo: desvincula por completo', async () => {
    const mov = fakeMov({
      erpIds: ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 100, total: 300, serie: 'A0', folioExterno: '100' }],
      deposito: 100,
    });
    BankMovement.find.mockResolvedValue([mov]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-8' });

    erpRoutes._rangoDesdeFollo.mockReturnValue({ fechaDesde: '2026-08-01', fechaHasta: '2026-08-31' });
    erpRoutes._sincronizarConRetry.mockRejectedValue(new Error('Kore no responde'));

    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100' });

    expect(res.status).toBe(200);
    expect(mov.erpIds).toEqual([]);
    expect(mov.erpLinks).toEqual([]);
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
