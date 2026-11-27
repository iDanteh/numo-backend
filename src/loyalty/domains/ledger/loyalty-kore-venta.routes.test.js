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
jest.mock('./loyalty-kore-venta-retiro.service', () => ({
  procesarVentaRetiroKore: jest.fn(),
  listarVentasRetiradasConDeposito: jest.fn(),
}));
jest.mock('./loyalty-venta-fallida.service', () => ({
  listarVentasFallidas: jest.fn(),
  marcarVentaFallidaResuelta: jest.fn(),
}));
jest.mock('./loyalty-kore-canje.service', () => ({
  solicitarCodigoCanje: jest.fn(),
  validarCodigoCanje: jest.fn(),
}));
// Auditoría de /venta y /ventas (2026-11-17) — mockeado para no pegarle a Postgres real desde
// estos tests de wiring (la lógica real de filtros se prueba en
// loyalty-kore-venta-log.service.test.js). Resuelve por default para no dejar promesas colgadas.
jest.mock('./loyalty-kore-venta-log.service', () => ({
  registrarVentaLog: jest.fn().mockResolvedValue(undefined),
  listarVentasLog: jest.fn(),
}));
// Mismo criterio que loyalty-public.routes.test.js: `express-rate-limit` sigue siendo REAL (no
// mockeado), pero el store sí se mockea — un `FakeRedisStore` que implementa el contrato real
// (increment/decrement/resetKey/init) para que el conteo de 429 del limiter por teléfono
// (`limiterSolicitarCanje`) sea idéntico al que afirma el describe de abajo. `redis-client` se
// mockea para no disparar su connect() real (no hay Redis en CI/local para estos tests).
jest.mock('../../../shared/utils/redis-client', () => ({ sendCommand: jest.fn() }));
jest.mock('rate-limit-redis', () => {
  function FakeRedisStore(options) {
    this.options = options;
    this.hits = new Map();
    this.windowMs = 0;
    this.init = (opts) => { this.windowMs = opts.windowMs; };
    this.increment = async (key) => {
      const now = Date.now();
      let entry = this.hits.get(key);
      if (!entry || entry.resetTime.getTime() <= now) {
        entry = { totalHits: 0, resetTime: new Date(now + this.windowMs) };
      }
      entry.totalHits += 1;
      this.hits.set(key, entry);
      return { totalHits: entry.totalHits, resetTime: entry.resetTime };
    };
    this.decrement = async (key) => {
      const entry = this.hits.get(key);
      if (entry && entry.totalHits > 0) entry.totalHits -= 1;
    };
    this.resetKey = async (key) => { this.hits.delete(key); };
  }
  return { RedisStore: jest.fn(FakeRedisStore) };
});

const express = require('express');
const request = require('supertest');
const router = require('./loyalty-kore-venta.routes');
const ventaService = require('./loyalty-kore-venta.service');
const ventaRetiroService = require('./loyalty-kore-venta-retiro.service');
const ventaFallidaService = require('./loyalty-venta-fallida.service');
const canjeCodigoService = require('./loyalty-kore-canje.service');
const ventaLogService = require('./loyalty-kore-venta-log.service');
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

  // Auditoría (2026-11-17, pedido explícito del usuario): cada request real se registra con su
  // resultado — nunca cambia la respuesta real del webhook (ver loyalty-kore-venta-log.service.js).
  test('registra el log con resultado "exito" cuando el service responde bien', async () => {
    const body = { serieFolio: 'A0-1', telefono: '5512345678' };
    ventaService.procesarVentaKore.mockResolvedValue({ status: 'aplicado' });
    await request(freshApp()).post('/venta').send(body);
    await new Promise((r) => setImmediate(r)); // el registro es fire-and-forget (.catch, no await)
    expect(ventaLogService.registrarVentaLog).toHaveBeenCalledWith(
      expect.objectContaining({ endpoint: 'venta', resultado: 'exito', payload: expect.objectContaining(body) }),
    );
  });

  test('registra el log con resultado "error" y el detalle cuando el service revienta, y sigue respondiendo el error real', async () => {
    const body = { serieFolio: 'A0-1', telefono: '5512345678' };
    const err = Object.assign(new Error('La venta no existe en Kore.'), { statusCode: 400 });
    ventaService.procesarVentaKore.mockRejectedValue(err);

    const res = await request(freshApp()).post('/venta').send(body);
    await new Promise((r) => setImmediate(r));

    expect(res.status).toBe(400);
    expect(ventaLogService.registrarVentaLog).toHaveBeenCalledWith(
      expect.objectContaining({ endpoint: 'venta', resultado: 'error', detalle: 'La venta no existe en Kore.' }),
    );
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

  test('registra el log con endpoint "ventas" y resultado "exito"', async () => {
    const ventas = [{ serieFolio: 'A0-1' }];
    ventaService.procesarLoteVentasKore.mockResolvedValue({ resultados: [] });
    await request(freshApp()).post('/ventas').send({ ventas });
    await new Promise((r) => setImmediate(r));
    expect(ventaLogService.registrarVentaLog).toHaveBeenCalledWith(
      expect.objectContaining({ endpoint: 'ventas', resultado: 'exito' }),
    );
  });
});

describe('POST /venta-retiro', () => {
  test('no requiere sesión Numo (solo verifyKoreApiKey, mockeado como no-op) y pasa serieFolio/motivoRetiro al service', async () => {
    ventaRetiroService.procesarVentaRetiroKore.mockResolvedValue({ ok: true, revertido: true });
    const res = await request(freshApp()).post('/venta-retiro').send({ serieFolio: 'A0-1', motivoRetiro: 'cliente canceló' });
    expect(res.status).toBe(200);
    expect(ventaRetiroService.procesarVentaRetiroKore).toHaveBeenCalledWith({ serieFolio: 'A0-1', motivoRetiro: 'cliente canceló' });
    expect(res.body).toEqual({ ok: true, revertido: true });
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

describe('POST /canje/solicitar-codigo', () => {
  test('no requiere sesión Numo (solo verifyKoreApiKey, mockeado como no-op) y pasa el teléfono al service', async () => {
    canjeCodigoService.solicitarCodigoCanje.mockResolvedValue({ mensaje: 'Código enviado' });
    const res = await request(freshApp()).post('/canje/solicitar-codigo').send({ telefono: '5512345678' });
    expect(res.status).toBe(200);
    expect(canjeCodigoService.solicitarCodigoCanje).toHaveBeenCalledWith('5512345678');
    expect(res.body).toEqual({ mensaje: 'Código enviado' });
  });
});

describe('POST /canje/solicitar-codigo — límite por teléfono (2026-11-13, hardening)', () => {
  test('4ta solicitud para el mismo teléfono dentro de la ventana → 429, sin llegar al service', async () => {
    canjeCodigoService.solicitarCodigoCanje.mockResolvedValue({ mensaje: 'Código enviado' });
    const telefono = '5500009999'; // teléfono propio de este describe — no comparte contador con el test de arriba
    const app = freshApp();

    for (let i = 0; i < 3; i++) {
      const ok = await request(app).post('/canje/solicitar-codigo').send({ telefono });
      expect(ok.status).toBe(200);
    }

    const bloqueado = await request(app).post('/canje/solicitar-codigo').send({ telefono });
    expect(bloqueado.status).toBe(429);
    expect(bloqueado.body).toMatchObject({ code: 'RATE_LIMIT_EXCEEDED' });
    expect(canjeCodigoService.solicitarCodigoCanje).toHaveBeenCalledTimes(3);
  });

  test('teléfonos distintos tienen presupuestos independientes', async () => {
    canjeCodigoService.solicitarCodigoCanje.mockResolvedValue({ mensaje: 'Código enviado' });
    const app = freshApp();

    for (let i = 0; i < 3; i++) {
      await request(app).post('/canje/solicitar-codigo').send({ telefono: '5500001111' });
    }
    const otroTelefono = await request(app).post('/canje/solicitar-codigo').send({ telefono: '5500002222' });

    expect(otroTelefono.status).toBe(200);
  });
});

describe('POST /canje/validar-codigo', () => {
  test('no requiere sesión Numo (solo verifyKoreApiKey, mockeado como no-op) y pasa teléfono/código al service', async () => {
    canjeCodigoService.validarCodigoCanje.mockResolvedValue({ valido: true });
    const res = await request(freshApp()).post('/canje/validar-codigo').send({ telefono: '5512345678', codigo: '123456' });
    expect(res.status).toBe(200);
    expect(canjeCodigoService.validarCodigoCanje).toHaveBeenCalledWith('5512345678', '123456');
    expect(res.body).toEqual({ valido: true });
  });
});

describe('GET /ventas-log — requiere loyalty:auditoria, sesión Numo', () => {
  test('403 sin el permiso', async () => {
    const res = await request(freshApp()).get('/ventas-log');
    expect(res.status).toBe(403);
  });

  test('200 con el permiso, pasa los filtros de query al service', async () => {
    ventaLogService.listarVentasLog.mockResolvedValue({ total: 0, logs: [] });
    const res = await request(freshApp()).get('/ventas-log')
      .query({ resultado: 'error', endpoint: 'venta', desde: '2026-11-01' })
      .set(perms(PERMISSIONS.LOYALTY_AUDITORIA));
    expect(res.status).toBe(200);
    expect(ventaLogService.listarVentasLog).toHaveBeenCalledWith(expect.objectContaining({
      resultado: 'error', endpoint: 'venta', desde: '2026-11-01',
    }));
  });
});

describe('GET /ventas-retiradas-con-deposito — requiere loyalty:config, sesión Numo', () => {
  test('403 sin el permiso', async () => {
    const res = await request(freshApp()).get('/ventas-retiradas-con-deposito');
    expect(res.status).toBe(403);
  });

  test('200 con el permiso', async () => {
    ventaRetiroService.listarVentasRetiradasConDeposito.mockResolvedValue([]);
    const res = await request(freshApp()).get('/ventas-retiradas-con-deposito').set(perms(PERMISSIONS.LOYALTY_CONFIG));
    expect(res.status).toBe(200);
    expect(res.body).toEqual([]);
  });
});
