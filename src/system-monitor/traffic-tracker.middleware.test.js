'use strict';

jest.mock('../shared/utils/logger', () => ({ logger: { error: jest.fn() } }));
jest.mock('./system-monitor-historial.service', () => ({ guardarError: jest.fn() }));

const EventEmitter = require('events');
const express = require('express');
const request = require('supertest');
const { trafficTracker, getEstadoCrudo, _resetParaTests } = require('./traffic-tracker.middleware');
const { logger } = require('../shared/utils/logger');
const historialSvc = require('./system-monitor-historial.service');

// Deja el microtask queue drenar — guardarError() se llama sin await (fire-and-forget)
// desde el handler de 'finish', así que su .then/.catch corre en un microtask aparte.
async function flushMicrotasks() {
  await Promise.resolve();
  await Promise.resolve();
}

function buildApp() {
  const app = express();
  app.use(trafficTracker);
  app.get('/ok', (req, res) => res.status(200).json({ ok: true }));
  app.get('/redirect', (req, res) => res.status(302).end());
  app.get('/bad', (req, res) => res.status(400).json({ error: 'bad' }));
  app.get('/conflict', (req, res) => res.status(409).json({ error: 'conflict' }));
  app.get('/unprocessable', (req, res) => res.status(422).json({ error: 'unprocessable' }));
  app.get('/forbidden', (req, res) => res.status(403).json({ error: 'forbidden' }));
  app.get('/notfound', (req, res) => res.status(404).json({ error: 'notfound' }));
  app.get('/boom', (req, res) => res.status(500).json({ error: 'boom' }));
  return app;
}

describe('trafficTracker middleware', () => {
  beforeEach(() => {
    _resetParaTests();
    historialSvc.guardarError.mockReset().mockResolvedValue(undefined);
  });

  test('cuenta una request 2xx en los buckets de segundo y minuto', async () => {
    const app = buildApp();
    await request(app).get('/ok');

    const estado = getEstadoCrudo();
    expect(estado.segundos).toHaveLength(1);
    expect(estado.segundos[0]).toMatchObject({ total: 1, c2xx: 1, c3xx: 0, c4xx: 0, c5xx: 0 });
    expect(estado.minutos).toHaveLength(1);
    expect(estado.minutos[0]).toMatchObject({ total: 1, c2xx: 1 });
  });

  test('clasifica correctamente 3xx/4xx/5xx en el mismo bucket', async () => {
    const app = buildApp();
    await request(app).get('/redirect');
    await request(app).get('/bad');
    await request(app).get('/boom');

    const estado = getEstadoCrudo();
    expect(estado.minutos[0]).toMatchObject({ total: 3, c2xx: 0, c3xx: 1, c4xx: 1, c5xx: 1 });
  });

  test('enCurso vuelve a 0 después de que todas las requests terminan', async () => {
    const app = buildApp();
    await Promise.all([
      request(app).get('/ok'),
      request(app).get('/ok'),
      request(app).get('/bad'),
    ]);

    expect(getEstadoCrudo().enCurso).toBe(0);
  });

  test('guarda los 5xx en erroresRecientes con método/path/status', async () => {
    const app = buildApp();
    await request(app).get('/boom');

    const estado = getEstadoCrudo();
    expect(estado.erroresRecientes).toHaveLength(1);
    expect(estado.erroresRecientes[0]).toMatchObject({ metodo: 'GET', path: '/boom', status: 500 });
  });

  test('un 5xx dispara guardarError() fire-and-forget con ts (Date)/metodo/path/status', async () => {
    const app = buildApp();
    await request(app).get('/boom');
    await flushMicrotasks();

    expect(historialSvc.guardarError).toHaveBeenCalledTimes(1);
    const doc = historialSvc.guardarError.mock.calls[0][0];
    expect(doc.ts).toBeInstanceOf(Date);
    expect(doc.metodo).toBe('GET');
    expect(doc.path).toBe('/boom');
    expect(doc.status).toBe(500);
  });

  test('un 2xx/3xx NO llama a guardarError', async () => {
    const app = buildApp();
    await request(app).get('/ok');
    await request(app).get('/redirect');
    await flushMicrotasks();

    expect(historialSvc.guardarError).not.toHaveBeenCalled();
  });

  test('403/404 (ruido normal de uso, no negocio) NO llaman a guardarError', async () => {
    const app = buildApp();
    await request(app).get('/forbidden');
    await request(app).get('/notfound');
    await flushMicrotasks();

    expect(historialSvc.guardarError).not.toHaveBeenCalled();
  });

  test.each([
    ['/bad', 400],
    ['/conflict', 409],
    ['/unprocessable', 422],
  ])('un error de negocio (%s, %i) SÍ llama a guardarError', async (path, status) => {
    const app = buildApp();
    await request(app).get(path);
    await flushMicrotasks();

    expect(historialSvc.guardarError).toHaveBeenCalledTimes(1);
    expect(historialSvc.guardarError.mock.calls[0][0]).toMatchObject({ metodo: 'GET', path, status });
  });

  test('si guardarError() rechaza (Mongo caído), solo se loguea — no propaga ni afecta la respuesta ya enviada', async () => {
    historialSvc.guardarError.mockRejectedValueOnce(new Error('mongo caído'));
    const app = buildApp();

    const res = await request(app).get('/boom');
    expect(res.status).toBe(500); // la respuesta ya se mandó, no se ve afectada

    await flushMicrotasks();
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('[system-monitor] Error persistiendo error en Mongo'),
      'mongo caído',
    );
  });

  test('erroresRecientes se recorta a los últimos 20 (descarta los más viejos)', async () => {
    const app = buildApp();
    for (let i = 0; i < 25; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await request(app).get('/boom');
    }

    expect(getEstadoCrudo().erroresRecientes).toHaveLength(20);
  });

  test('un slot obsoleto (misma posición del ring buffer, otro segundo) se resetea solo', async () => {
    const app = buildApp();
    const original = Date.now;
    try {
      // Fija "ahora" en un instante exacto, hace una request, y luego salta
      // exactamente 60 segundos (misma posición del ring buffer de 60 slots) —
      // el slot debe reflejar SOLO la request nueva, no arrastrar la vieja.
      const base = 1_700_000_000_000; // instante fijo cualquiera
      Date.now = () => base;
      await request(app).get('/ok');

      Date.now = () => base + 60_000;
      await request(app).get('/bad');

      const estado = getEstadoCrudo(base + 60_000);
      expect(estado.segundos).toHaveLength(1);
      expect(estado.segundos[0]).toMatchObject({ total: 1, c2xx: 0, c4xx: 1 });
    } finally {
      Date.now = original;
    }
  });

  test('getEstadoCrudo(ahoraMs) descarta slots fuera de la ventana de 60 unidades', async () => {
    const app = buildApp();
    const original = Date.now;
    try {
      const base = 1_700_000_000_000;
      Date.now = () => base;
      await request(app).get('/ok');

      // 61 minutos después: el bucket de minuto de la request queda fuera de la
      // ventana de 60 minutos válidos desde ese "ahora" (a los 5min seguiría
      // adentro — la ventana es de 60, no de "recién pasó").
      const estado = getEstadoCrudo(base + 61 * 60_000);
      expect(estado.minutos).toHaveLength(0);
    } finally {
      Date.now = original;
    }
  });

  test('un slot de MINUTO obsoleto (misma posición del ring buffer, 60min después) se resetea solo', async () => {
    const app = buildApp();
    const original = Date.now;
    try {
      const base = 1_700_000_000_000;
      Date.now = () => base;
      await request(app).get('/ok');

      Date.now = () => base + 60 * 60_000; // misma posición del ring buffer de 60 slots
      await request(app).get('/bad');

      const estado = getEstadoCrudo(base + 60 * 60_000);
      expect(estado.minutos).toHaveLength(1);
      expect(estado.minutos[0]).toMatchObject({ total: 1, c2xx: 0, c4xx: 1 });
    } finally {
      Date.now = original;
    }
  });

  // ── Resiliencia: un error interno al registrar el 'finish' nunca debe propagar ──
  test('un error interno al registrar el finish no propaga (la request de negocio no se ve afectada)', () => {
    _resetParaTests();
    const res = new EventEmitter();
    res.statusCode = 500;
    const req = {
      method: 'GET',
      // Simula una excepción real dentro del callback (ej. algo inesperado leyendo
      // el request) — el acceso a la propiedad explota recién cuando el callback
      // de 'finish' la lee, no antes.
      get originalUrl() { throw new Error('boom interno'); },
    };

    expect(() => {
      trafficTracker(req, res, () => {});
      res.emit('finish');
    }).not.toThrow();

    expect(logger.error).toHaveBeenCalled();
  });

  // ── enCurso: guard finish/close ──────────────────────────────────────────────
  test('enCurso se decrementa UNA sola vez aunque "finish" y "close" disparen ambos (camino feliz)', () => {
    _resetParaTests();
    const res = new EventEmitter();
    res.statusCode = 200;
    const req = { method: 'GET', originalUrl: '/x' };

    trafficTracker(req, res, () => {});
    expect(getEstadoCrudo().enCurso).toBe(1);

    res.emit('finish');
    res.emit('close');
    expect(getEstadoCrudo().enCurso).toBe(0);
  });

  test('enCurso se decrementa por "close" cuando "finish" nunca dispara (conexión abortada)', () => {
    _resetParaTests();
    const res = new EventEmitter();
    res.statusCode = 200;
    const req = { method: 'GET', originalUrl: '/x' };

    trafficTracker(req, res, () => {});
    expect(getEstadoCrudo().enCurso).toBe(1);

    res.emit('close'); // sin 'finish' — request cancelada a mitad de camino
    expect(getEstadoCrudo().enCurso).toBe(0);
  });
});
