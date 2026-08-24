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
  // 2026-08-21: erp-reversion.service.js ya no llama a _montoSaldoLinkPorMovimiento
  // directamente (bug de atribución cruzada entre movimientos, ver erp.routes.test.js) —
  // usa _aportesPorErpIdCronologico, que resuelve TODOS los movimientos humanos de una CxC
  // en una sola pasada. Se deja igual mockeada por si algún test viejo la referencia.
  _montoSaldoLinkPorMovimiento:   jest.fn(),
  _aportesPorErpIdCronologico:    jest.fn(() => new Map()),
  _esFormaPagoBancariaKore:       jest.fn(),
  _montoSaldoLinkPorAutorizacion: jest.fn(),
  _backfillFormasPagoYFolioFiscal: jest.fn(),
  _movimientosKoreDesde:          jest.fn(),
  _retencionVigente:              jest.fn(),
}));

const express      = require('express');
const request      = require('supertest');
const router       = require('./erp-reversion.routes');
const { procesarReversionKore } = require('./erp-reversion.service');
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

describe('procesarReversionKore — procesa la reversión', () => {
  // 2026-08-21: estos tests llaman al service directamente (no vía supertest) — desde el fix
  // de "responder ya, procesar en background" (ver describe 'ack rápido' más abajo), el
  // request HTTP ya NO espera a que esto termine, así que no hay forma de observar estos
  // efectos secundarios esperando la respuesta del POST. Se prueba la lógica de negocio acá,
  // y el contrato HTTP (responde rápido, con el conteo correcto) por separado.
  test('un movimiento afectado: erpIds/erpLinks/status se recalculan', async () => {
    const mov = fakeMov({
      erpIds:   ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 500, total: 500, serie: 'A0', folioExterno: '100' }],
      deposito: 500,
      status:   'identificado',
    });
    BankMovement.find.mockResolvedValue([mov]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-1' });

    const result = await procesarReversionKore({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100' });

    expect(result).toEqual({ reversionId: 'rev-1', movimientosAfectados: 1, yaEstabaDesvinculada: false });
    expect(BankMovement.find).toHaveBeenCalledWith({ erpIds: 'CXC-1' });
    expect(mov.erpIds).toEqual([]);
    expect(mov.erpLinks).toEqual([]);
    expect(mov.status).toBe('no_identificado'); // ya no hay ningún link que cubra el depósito
    expect(mov.save).toHaveBeenCalledTimes(1);
    expect(emitToBanco).toHaveBeenCalledWith('BBVA', 'bank:movement:updated', expect.objectContaining({
      erpIds: [], erpLinks: [], status: 'no_identificado',
    }));
  });

  test('2 movimientos afectados por el mismo erpId', async () => {
    const link = { erpId: 'CXC-1', saldoActual: 200, total: 200 };
    const mov1 = fakeMov({ _id: 'mov-1', erpIds: ['CXC-1'], erpLinks: [{ ...link }], deposito: 200 });
    const mov2 = fakeMov({ _id: 'mov-2', erpIds: ['CXC-1'], erpLinks: [{ ...link }], deposito: 200 });
    BankMovement.find.mockResolvedValue([mov1, mov2]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-2' });

    const result = await procesarReversionKore({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '200' });

    expect(result.movimientosAfectados).toBe(2);
    expect(mov1.save).toHaveBeenCalledTimes(1);
    expect(mov2.save).toHaveBeenCalledTimes(1);
    expect(mov1.erpIds).toEqual([]);
    expect(mov2.erpIds).toEqual([]);
  });

  test('yaEstabaDesvinculada:true cuando no se encuentra ningún movimiento, sin persistir ErpReversion', async () => {
    BankMovement.find.mockResolvedValue([]);

    const result = await procesarReversionKore({ erpId: 'CXC-inexistente', serieExterna: 'A0', folioExterno: '300' });

    expect(result).toEqual({ reversionId: null, movimientosAfectados: 0, yaEstabaDesvinculada: true });
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

    await procesarReversionKore({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100' });

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

    await procesarReversionKore({ erpId: 'CXC-1', serieExterna: 'B0', folioExterno: '999' });

    expect(ErpReversion.create).toHaveBeenCalledWith(expect.objectContaining({ serieFolioMismatch: true }));
  });
});

describe('procesarReversionKore — fix real 2026-08-20: reconsulta a Kore en vivo, no desvincula abonos que siguen vigentes', () => {
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
    // Índices en el orden de BankMovement.find: 0=movConAbonoVigente(mov-A), 1=movYaSinAporte(mov-B).
    erpRoutes._aportesPorErpIdCronologico.mockReturnValue(new Map([[0, 100]]));
    erpRoutes._backfillFormasPagoYFolioFiscal.mockReturnValue({ saldoPagadoTotal: 100, saldoPagado: 100, folioFiscal: null });
    erpRoutes._movimientosKoreDesde.mockReturnValue([]);
    erpRoutes._retencionVigente.mockReturnValue({ tieneRetencion: false, montoRetenido: null });

    await procesarReversionKore({
      erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100', referencia: '6a876c193bfaed00011c9216', monto: 200,
    });

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
    erpRoutes._aportesPorErpIdCronologico.mockReturnValue(new Map([[0, 200]]));
    erpRoutes._backfillFormasPagoYFolioFiscal.mockReturnValue({ saldoPagadoTotal: 200, saldoPagado: 200, folioFiscal: null });
    erpRoutes._movimientosKoreDesde.mockReturnValue([]);
    erpRoutes._retencionVigente.mockReturnValue({ tieneRetencion: false, montoRetenido: null });

    await procesarReversionKore({
      erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100', referencia: '6a876c193bfaed00011c9216', monto: 100,
    });

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

    await procesarReversionKore({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100' });

    expect(mov.erpIds).toEqual([]);
    expect(mov.erpLinks).toEqual([]);
  });
});

describe('procesarReversionKore — red de seguridad de atribución (2026-08-21, caso real folioExterno 260800164)', () => {
  test('si la suma calculada NO reconcilia contra lo que Kore dice pagado, no se toca ningún link', async () => {
    const movA = fakeMov({
      _id: 'mov-A', erpIds: ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 196.62, total: 346.62, saldoErpAportado: 100, serie: 'A0', folioExterno: '164' }],
      deposito: 100,
    });
    const movB = fakeMov({
      _id: 'mov-B', erpIds: ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 196.62, total: 346.62, saldoErpAportado: 50, serie: 'A0', folioExterno: '164' }],
      deposito: 50,
    });
    BankMovement.find.mockResolvedValue([movA, movB]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-sin-tocar' });
    erpRoutes._rangoDesdeFollo.mockReturnValue({ fechaDesde: '2026-08-01', fechaHasta: '2026-08-31' });
    // La entrada con `fecha` exacta hace que se confirme en el primer intento — sin esto, el
    // retry de 15/30/45s (real, sin fake timers en este test) haría que el test tardara ~90s.
    erpRoutes._sincronizarConRetry.mockResolvedValue({
      raw: [{ total: 346.62, saldoActual: 196.62, movimientos: [{ fecha: '2026-08-21T16:14:20.116981Z', total: 50 }] }],
    });
    erpRoutes._erpIdIdentificadoPorHumano.mockReturnValue(true);
    // El bug real (previo a la función cronológica compartida): ambos movimientos daban 0
    // aunque Kore diga que hay $150 pagados entre los dos (100+50) — se mockea acá el
    // resultado de _aportesPorErpIdCronologico como un mapa VACÍO para probar que el chequeo
    // de reconciliación sigue funcionando como red de seguridad ante CUALQUIER escenario
    // donde el cálculo no reconcilie, sin depender de que ese bug puntual siga existiendo
    // (ya resuelto y cubierto aparte en erp.routes.test.js).
    erpRoutes._aportesPorErpIdCronologico.mockReturnValue(new Map());

    await procesarReversionKore({
      erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '164', fecha: '2026-08-21T16:14:20.116981Z',
    });

    // Nada se tocó: ni erpIds/erpLinks cambiaron, ni se llamó a save().
    expect(movA.erpIds).toEqual(['CXC-1']);
    expect(movB.erpIds).toEqual(['CXC-1']);
    expect(movA.save).not.toHaveBeenCalled();
    expect(movB.save).not.toHaveBeenCalled();
    expect(ErpReversion.create).toHaveBeenCalledWith(expect.objectContaining({
      atribucionConfiable: false,
      movimientosAfectados: [
        expect.objectContaining({ movementId: 'mov-A', tipo: 'sin_tocar' }),
        expect.objectContaining({ movementId: 'mov-B', tipo: 'sin_tocar' }),
      ],
    }));
  });

  test('si la suma calculada SÍ reconcilia, sigue aplicando normalmente (sin falsos positivos)', async () => {
    const mov = fakeMov({
      _id: 'mov-1', erpIds: ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 100, total: 300, saldoErpAportado: 300, serie: 'A0', folioExterno: '100' }],
      deposito: 100,
    });
    BankMovement.find.mockResolvedValue([mov]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-ok' });
    erpRoutes._rangoDesdeFollo.mockReturnValue({ fechaDesde: '2026-08-01', fechaHasta: '2026-08-31' });
    erpRoutes._sincronizarConRetry.mockResolvedValue({
      raw: [{ total: 300, saldoActual: 100, movimientos: [{ fecha: '2026-08-01T00:00:00Z', total: 100 }] }],
    });
    erpRoutes._erpIdIdentificadoPorHumano.mockReturnValue(true);
    erpRoutes._aportesPorErpIdCronologico.mockReturnValue(new Map([[0, 200]])); // reconcilia: 300-100=200
    erpRoutes._backfillFormasPagoYFolioFiscal.mockReturnValue({ saldoPagadoTotal: 200, saldoPagado: 200, folioFiscal: null });
    erpRoutes._movimientosKoreDesde.mockReturnValue([]);
    erpRoutes._retencionVigente.mockReturnValue({ tieneRetencion: false, montoRetenido: null });

    await procesarReversionKore({
      erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100', fecha: '2026-08-01T00:00:00Z',
    });

    expect(mov.save).toHaveBeenCalledTimes(1);
    expect(mov.erpLinks[0].saldoErpAportado).toBe(200);
    expect(ErpReversion.create).toHaveBeenCalledWith(expect.objectContaining({ atribucionConfiable: true }));
  });
});

describe('POST /api/erp/cxc-reversiones — ack rápido (2026-08-21, fix real: Kore reportaba "No se pudo revertir el movimiento")', () => {
  // Reportado por el usuario: al agregar el retry con backoff (hasta 90s), Kore empezó a
  // fallar SU PROPIA reversión con "No se pudo revertir el movimiento" — evidencia de que
  // Kore llama a este webhook de forma síncrona como parte de su propia transacción, y su
  // cliente HTTP hacía timeout esperando nuestra respuesta. Fix: responder de inmediato con
  // el conteo (ya conocido sin reconsultar Kore) y procesar el resto en segundo plano.
  test('responde de inmediato con el conteo, SIN esperar la reconsulta a Kore ni el retry', async () => {
    const mov = fakeMov({
      erpIds: ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 100, total: 300, serie: 'A0', folioExterno: '100' }],
      deposito: 100,
    });
    BankMovement.find.mockResolvedValue([mov]);
    BankMovement.countDocuments.mockResolvedValue(1);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-ack' });
    erpRoutes._rangoDesdeFollo.mockReturnValue({ fechaDesde: '2026-08-01', fechaHasta: '2026-08-31' });
    // Nunca resuelve — si la respuesta HTTP dependiera de esto, el test colgaría hasta el
    // timeout de Jest en vez de responder rápido.
    erpRoutes._sincronizarConRetry.mockReturnValue(new Promise(() => {}));

    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100', fecha: '2026-08-21T00:00:00Z' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true, movimientosAfectados: 1, yaEstabaDesvinculada: false });
    expect(BankMovement.countDocuments).toHaveBeenCalledWith({ erpIds: 'CXC-1' });
  });

  test('yaEstabaDesvinculada:true en el ack rápido cuando el conteo es 0', async () => {
    BankMovement.countDocuments.mockResolvedValue(0);
    BankMovement.find.mockResolvedValue([]);

    const res = await request(app)
      .post('/api/erp/cxc-reversiones')
      .set('X-Api-Key', API_KEY)
      .send({ erpId: 'CXC-inexistente', serieExterna: 'A0', folioExterno: '300' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true, movimientosAfectados: 0, yaEstabaDesvinculada: true });
  });
});

describe('procesarReversionKore — confirmación con reintentos incrementales (2026-08-21, caso real folioExterno 260800152)', () => {
  // Se llama al service directamente (no vía supertest) para poder usar fake timers sin
  // mezclarlos con el ciclo de vida de un socket HTTP real — mismo criterio que
  // collection-request-erp-links.test.js (jest.useFakeTimers, sin supertest de por medio).
  afterEach(() => {
    jest.useRealTimers();
  });

  test('Kore ya refleja la reversión en el primer intento: un solo llamado, sin esperar nada', async () => {
    const fechaReversion = '2026-08-20T21:50:52.560635Z';
    const mov = fakeMov({
      _id: 'mov-1', erpIds: ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 200, total: 300, saldoErpAportado: 200, serie: 'A0', folioExterno: '100' }],
      deposito: 100,
    });
    BankMovement.find.mockResolvedValue([mov]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-9' });
    erpRoutes._rangoDesdeFollo.mockReturnValue({ fechaDesde: '2026-08-01', fechaHasta: '2026-08-31' });
    erpRoutes._sincronizarConRetry.mockResolvedValue({
      raw: [{ saldoActual: 200, movimientos: [{ fecha: fechaReversion, total: 100 }] }],
    });
    erpRoutes._erpIdIdentificadoPorHumano.mockReturnValue(true);
    erpRoutes._aportesPorErpIdCronologico.mockReturnValue(new Map([[0, 200]]));
    erpRoutes._backfillFormasPagoYFolioFiscal.mockReturnValue({ saldoPagadoTotal: 200, saldoPagado: 200, folioFiscal: null });
    erpRoutes._movimientosKoreDesde.mockReturnValue([{ fecha: fechaReversion, total: 100 }]);
    erpRoutes._retencionVigente.mockReturnValue({ tieneRetencion: false, montoRetenido: null });

    const result = await procesarReversionKore({
      erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100', fecha: fechaReversion,
    });

    expect(result.movimientosAfectados).toBe(1);
    expect(erpRoutes._sincronizarConRetry).toHaveBeenCalledTimes(1);
    expect(ErpReversion.create).toHaveBeenCalledWith(expect.objectContaining({ confirmadaEnKore: true }));
  });

  test('Kore todavía no refleja la reversión: reintenta a los 15s y confirma en el segundo intento', async () => {
    jest.useFakeTimers();
    const fechaReversion = '2026-08-20T21:50:52.560635Z';
    const mov = fakeMov({
      _id: 'mov-1', erpIds: ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 100, total: 300, saldoErpAportado: 300, serie: 'A0', folioExterno: '100' }],
      deposito: 100,
    });
    BankMovement.find.mockResolvedValue([mov]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-10' });
    erpRoutes._rangoDesdeFollo.mockReturnValue({ fechaDesde: '2026-08-01', fechaHasta: '2026-08-31' });
    erpRoutes._sincronizarConRetry
      .mockResolvedValueOnce({ raw: [{ saldoActual: 100, movimientos: [] }] }) // 1er intento: todavía no refleja el reverso
      .mockResolvedValueOnce({ raw: [{ saldoActual: 200, movimientos: [{ fecha: fechaReversion, total: 100 }] }] }); // 2do intento: ya lo refleja
    erpRoutes._erpIdIdentificadoPorHumano.mockReturnValue(true);
    erpRoutes._aportesPorErpIdCronologico.mockReturnValue(new Map([[0, 200]]));
    erpRoutes._backfillFormasPagoYFolioFiscal.mockReturnValue({ saldoPagadoTotal: 200, saldoPagado: 200, folioFiscal: null });
    erpRoutes._movimientosKoreDesde.mockReturnValue([{ fecha: fechaReversion, total: 100 }]);
    erpRoutes._retencionVigente.mockReturnValue({ tieneRetencion: false, montoRetenido: null });

    const promise = procesarReversionKore({
      erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100', fecha: fechaReversion,
    });
    await jest.advanceTimersByTimeAsync(15_000);
    const result = await promise;

    expect(erpRoutes._sincronizarConRetry).toHaveBeenCalledTimes(2);
    expect(result.movimientosAfectados).toBe(1);
    expect(mov.erpLinks[0].saldoErpAportado).toBe(200); // ya con el número corregido, no el stale de 300
    expect(ErpReversion.create).toHaveBeenCalledWith(expect.objectContaining({ confirmadaEnKore: true }));
  });

  test('Kore nunca refleja la reversión tras agotar los 3 reintentos (15/30/45s): sigue con los datos más recientes disponibles', async () => {
    jest.useFakeTimers();
    const fechaReversion = '2026-08-20T21:50:52.560635Z';
    const mov = fakeMov({
      _id: 'mov-1', erpIds: ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 100, total: 300, saldoErpAportado: 300, serie: 'A0', folioExterno: '100' }],
      deposito: 100,
    });
    BankMovement.find.mockResolvedValue([mov]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-11' });
    erpRoutes._rangoDesdeFollo.mockReturnValue({ fechaDesde: '2026-08-01', fechaHasta: '2026-08-31' });
    erpRoutes._sincronizarConRetry.mockResolvedValue({ raw: [{ saldoActual: 100, movimientos: [] }] }); // nunca refleja el reverso
    erpRoutes._erpIdIdentificadoPorHumano.mockReturnValue(true);
    erpRoutes._aportesPorErpIdCronologico.mockReturnValue(new Map([[0, 100]])); // stale: el mismo valor de "antes", como en el caso real
    erpRoutes._backfillFormasPagoYFolioFiscal.mockReturnValue({ saldoPagadoTotal: 100, saldoPagado: 100, folioFiscal: null });
    erpRoutes._movimientosKoreDesde.mockReturnValue([]);
    erpRoutes._retencionVigente.mockReturnValue({ tieneRetencion: false, montoRetenido: null });

    const promise = procesarReversionKore({
      erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100', fecha: fechaReversion,
    });
    await jest.advanceTimersByTimeAsync(15_000);
    await jest.advanceTimersByTimeAsync(30_000);
    await jest.advanceTimersByTimeAsync(45_000);
    const result = await promise;

    expect(erpRoutes._sincronizarConRetry).toHaveBeenCalledTimes(4); // 1 inicial + 3 reintentos
    expect(result.movimientosAfectados).toBe(1);
    expect(mov.erpLinks[0].saldoErpAportado).toBe(100); // sigue con el dato stale, mejor que nada (comportamiento de respaldo ya existente)
    // 2026-08-21: NO se marca como confirmada — no hay forma de distinguir "Kore todavía no
    // lo aplicó" de "Kore falló y nunca lo va a aplicar" (caso real: folioExterno 260800164,
    // el reverso JAMÁS apareció en el historial de Kore, ni minutos después de agotar esto).
    expect(ErpReversion.create).toHaveBeenCalledWith(expect.objectContaining({ confirmadaEnKore: false }));
  });

  test('caso real 2026-08-21 (folioExterno 260800164): Kore avisó una reversión que nunca aplicó de su lado — el movimiento queda intacto y sin confirmar, no se le baja nada de más', async () => {
    jest.useFakeTimers();
    const fechaReversion = '2026-08-21T15:13:37.150011Z';
    // El historial de Kore NUNCA trae una entrada REV para esta fecha — no es una demora,
    // Kore simplemente no aplicó la reversión (confirmado por el usuario contra Kore real).
    const movKoreIntacto = { saldoActual: 146.62, movimientos: [
      { fecha: '2026-08-21T14:59:51.206396Z', total: 346.62 },
      { fecha: '2026-08-21T15:09:13.048446Z', total: -100 },
      { fecha: '2026-08-21T15:12:19.709737Z', total: -100 },
    ] };
    const mov1 = fakeMov({ _id: 'mov-1', erpIds: ['CXC-1'], erpLinks: [{ erpId: 'CXC-1', saldoActual: 246.62, total: 346.62, serie: 'A0', folioExterno: '164' }], deposito: 100 });
    const mov2 = fakeMov({ _id: 'mov-2', erpIds: ['CXC-1'], erpLinks: [{ erpId: 'CXC-1', saldoActual: 146.62, total: 346.62, serie: 'A0', folioExterno: '164' }], deposito: 100 });
    BankMovement.find.mockResolvedValue([mov1, mov2]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-13' });
    erpRoutes._rangoDesdeFollo.mockReturnValue({ fechaDesde: '2026-08-01', fechaHasta: '2026-08-31' });
    erpRoutes._sincronizarConRetry.mockResolvedValue({ raw: [movKoreIntacto] });
    erpRoutes._erpIdIdentificadoPorHumano.mockReturnValue(true);
    erpRoutes._aportesPorErpIdCronologico.mockReturnValue(new Map([[0, 100], [1, 100]])); // el aporte real, intacto — no bajó nada
    erpRoutes._backfillFormasPagoYFolioFiscal.mockReturnValue({ saldoPagadoTotal: 100, saldoPagado: 100, folioFiscal: null });
    erpRoutes._movimientosKoreDesde.mockReturnValue(movKoreIntacto.movimientos);
    erpRoutes._retencionVigente.mockReturnValue({ tieneRetencion: false, montoRetenido: null });

    const promise = procesarReversionKore({
      erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '164', fecha: fechaReversion,
      referencia: '6a886ad36f8ee0000106a67e',
    });
    await jest.advanceTimersByTimeAsync(15_000);
    await jest.advanceTimersByTimeAsync(30_000);
    await jest.advanceTimersByTimeAsync(45_000);
    await promise;

    // Ninguno de los 2 movimientos se desvincula ni pierde aporte — el reverso nunca se
    // aplicó del lado de Kore, así que el dato correcto ES que nada cambió.
    expect(mov1.erpLinks[0].saldoErpAportado).toBe(100);
    expect(mov2.erpLinks[0].saldoErpAportado).toBe(100);
    // Pero SÍ queda marcado como sin confirmar, para que alguien le dé seguimiento con Kore
    // en vez de asumir que la reversión ya quedó resuelta.
    expect(ErpReversion.create).toHaveBeenCalledWith(expect.objectContaining({ confirmadaEnKore: false }));
  });

  test('sin fecha en el payload de Kore, un solo intento (no reintenta a ciegas sin nada que confirmar)', async () => {
    const mov = fakeMov({
      _id: 'mov-1', erpIds: ['CXC-1'],
      erpLinks: [{ erpId: 'CXC-1', saldoActual: 100, total: 300, saldoErpAportado: 100, serie: 'A0', folioExterno: '100' }],
      deposito: 100,
    });
    BankMovement.find.mockResolvedValue([mov]);
    ErpReversion.create.mockResolvedValue({ _id: 'rev-12' });
    erpRoutes._rangoDesdeFollo.mockReturnValue({ fechaDesde: '2026-08-01', fechaHasta: '2026-08-31' });
    erpRoutes._sincronizarConRetry.mockResolvedValue({ raw: [{ saldoActual: 100, movimientos: [] }] });
    erpRoutes._erpIdIdentificadoPorHumano.mockReturnValue(true);
    erpRoutes._aportesPorErpIdCronologico.mockReturnValue(new Map([[0, 100]]));
    erpRoutes._backfillFormasPagoYFolioFiscal.mockReturnValue({ saldoPagadoTotal: 100, saldoPagado: 100, folioFiscal: null });
    erpRoutes._movimientosKoreDesde.mockReturnValue([]);
    erpRoutes._retencionVigente.mockReturnValue({ tieneRetencion: false, montoRetenido: null });

    await procesarReversionKore({ erpId: 'CXC-1', serieExterna: 'A0', folioExterno: '100' }); // sin `fecha`

    expect(erpRoutes._sincronizarConRetry).toHaveBeenCalledTimes(1);
    expect(ErpReversion.create).toHaveBeenCalledWith(expect.objectContaining({ confirmadaEnKore: false }));
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
