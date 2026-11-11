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
jest.mock('./domains/public/loyalty-recovery.service');

// Store en Redis de los 4 limiters dedicados (solicitar/confirmar/ver/email,
// consultoría de desempeño 2026-11-11: cluster nativo + Redis) — mockea
// `rate-limit-redis` y `./shared/utils/redis-client` de la misma forma que el
// resto de este archivo mockea los services: `express-rate-limit` en sí sigue
// siendo REAL (no mockeado, mismo criterio que ya usaba este archivo), así
// que el `store` que le pasamos SÍ debe implementar el contrato real
// (`increment`/`decrement`/`resetKey`, más `init(options)` opcional donde
// express-rate-limit le inyecta `windowMs`) para que el conteo de 429 sea
// idéntico al que ya afirman los tests de abajo — un mock "tonto" (solo
// jest.fn() vacíos) rompería todas las aserciones de "el Nvo request → 429".
// `./shared/utils/redis-client` se mockea para no disparar su side-effect de
// connect() real (no hay Redis en CI/local para estos tests).
jest.mock('../shared/utils/redis-client', () => ({ sendCommand: jest.fn() }));
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
  // jest.fn(impl) sigue siendo `new`-able — permite afirmar sobre
  // `RedisStore.mock.calls`/`.mock.instances` (una instancia nueva, con su
  // propio Map, por cada `new RedisStore(...)` — ver _redisStore() en
  // loyalty-public.routes.js: NUNCA un store compartido entre limiters).
  return { RedisStore: jest.fn(FakeRedisStore) };
});

const express = require('express');
const request = require('supertest');
const errorHandler = require('../shared/middleware/error-handler');

/** Re-requiere el router (y sus services mockeados) en un registro de módulos
 * limpio, para que cada test tenga su propia `MemoryStore` de rate limiting. */
function freshApp() {
  jest.resetModules();
  const publicService = require('./domains/public/loyalty-public.service');
  const recoveryService = require('./domains/public/loyalty-recovery.service');
  const { RedisStore } = require('rate-limit-redis');
  const loyaltyPublicRoutes = require('./loyalty-public.routes');

  const app = express();
  app.use(express.json());
  app.use('/api/loyalty/public', loyaltyPublicRoutes);
  app.use(errorHandler);

  return { app, publicService, recoveryService, loyaltyPublicRoutes, RedisStore };
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

describe('solicitud de código (paso 1, compartido por consulta) — mismo router público, sin authenticate', () => {
  test('POST /recuperacion/solicitar llega al service sin Authorization', async () => {
    const { app, recoveryService } = freshApp();
    recoveryService.solicitarCodigo.mockResolvedValue({ mensaje: 'Si el teléfono está registrado…' });

    const res = await request(app).post('/api/loyalty/public/recuperacion/solicitar').send({ telefono: '5512345678' });

    expect(res.status).toBe(200);
    expect(recoveryService.solicitarCodigo).toHaveBeenCalledWith('5512345678', expect.objectContaining({ ip: expect.anything() }));
  });

  test('GET /recuperacion/solicitar no existe (solo POST)', async () => {
    const { app } = freshApp();
    const res = await request(app).get('/api/loyalty/public/recuperacion/solicitar').query({ telefono: '5512345678' });
    expect(res.status).toBe(404);
  });

  test('rate limit de /recuperacion/solicitar es independiente del de /consulta', async () => {
    const { app, publicService, recoveryService } = freshApp();
    publicService.consultaPublica.mockResolvedValue({ nombre: 'Ana', nivel: 'Plata', saldoPuntos: '10.00' });
    recoveryService.solicitarCodigo.mockResolvedValue({ mensaje: 'ok' });

    for (let i = 0; i < 5; i++) {
      // eslint-disable-next-line no-await-in-loop
      await request(app).post('/api/loyalty/public/consulta').send({ telefono: '5512345678', nip: '1234' });
    }
    const consultaBloqueada = await request(app).post('/api/loyalty/public/consulta').send({ telefono: '5512345678', nip: '1234' });
    expect(consultaBloqueada.status).toBe(429);

    // /recuperacion/solicitar tiene su propio limiter (3/15min) — no debería
    // verse afectado por haber agotado el de /consulta.
    const solicitar = await request(app).post('/api/loyalty/public/recuperacion/solicitar').send({ telefono: '5512345678' });
    expect(solicitar.status).toBe(200);
  });

  test('el 4to request a /recuperacion/solicitar en 15min → 429 (límite más estricto por "enviar" algo)', async () => {
    const { app, recoveryService } = freshApp();
    recoveryService.solicitarCodigo.mockResolvedValue({ mensaje: 'ok' });

    for (let i = 0; i < 3; i++) {
      // eslint-disable-next-line no-await-in-loop
      const r = await request(app).post('/api/loyalty/public/recuperacion/solicitar').send({ telefono: '5512345678' });
      expect(r.status).toBe(200);
    }
    const cuarto = await request(app).post('/api/loyalty/public/recuperacion/solicitar').send({ telefono: '5512345678' });
    expect(cuarto.status).toBe(429);
    expect(recoveryService.solicitarCodigo).toHaveBeenCalledTimes(3);
  });
});

describe('consulta de puntos vía código (2026-10-26, reemplaza al NIP en /consulta)', () => {
  test('POST /consulta/confirmar llega al service sin Authorization y devuelve el payload mínimo', async () => {
    const { app, recoveryService } = freshApp();
    recoveryService.confirmarCodigoConsulta.mockResolvedValue({ nombre: 'Ana', nivel: 'Plata', saldoPuntos: 1234.5 });

    const res = await request(app).post('/api/loyalty/public/consulta/confirmar')
      .send({ telefono: '5512345678', codigo: '654321' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ nombre: 'Ana', nivel: 'Plata', saldoPuntos: 1234.5 });
    expect(recoveryService.confirmarCodigoConsulta).toHaveBeenCalledWith('5512345678', '654321', expect.objectContaining({ ip: expect.anything() }));
  });

  test('GET /consulta/confirmar no existe (solo POST, D8: el código nunca en una URL)', async () => {
    const { app } = freshApp();
    const res = await request(app).get('/api/loyalty/public/consulta/confirmar').query({ telefono: '5512345678', codigo: '654321' });
    expect(res.status).toBe(404);
  });

  test('falla de negocio (422 LOYALTY_RECOVERY_FAILED) se propaga con el mismo código', async () => {
    const { app, recoveryService } = freshApp();
    const err = Object.assign(new Error('El código no es válido o expiró.'), {
      statusCode: 422, code: 'LOYALTY_RECOVERY_FAILED',
    });
    recoveryService.confirmarCodigoConsulta.mockRejectedValue(err);

    const res = await request(app).post('/api/loyalty/public/consulta/confirmar')
      .send({ telefono: '5512345678', codigo: '000000' });

    expect(res.status).toBe(422);
    expect(res.body.code).toBe('LOYALTY_RECOVERY_FAILED');
  });

  test('el 11vo request en la misma ventana de 15min → 429, antes de llamar al service', async () => {
    const { app, recoveryService } = freshApp();
    recoveryService.confirmarCodigoConsulta.mockResolvedValue({ nombre: 'Ana', nivel: 'Plata', saldoPuntos: 1 });

    for (let i = 0; i < 10; i++) {
      // eslint-disable-next-line no-await-in-loop
      const r = await request(app).post('/api/loyalty/public/consulta/confirmar').send({ telefono: '5512345678', codigo: '654321' });
      expect(r.status).toBe(200);
    }
    const onceavo = await request(app).post('/api/loyalty/public/consulta/confirmar').send({ telefono: '5512345678', codigo: '654321' });
    expect(onceavo.status).toBe(429);
    expect(recoveryService.confirmarCodigoConsulta).toHaveBeenCalledTimes(10);
  });
});

describe('sesión de consulta post-código (2026-11-09) — "ver de nuevo" + email de contacto', () => {
  test('POST /consulta/ver llega al service sin Authorization y devuelve el payload con usosRestantes', async () => {
    const { app, recoveryService } = freshApp();
    recoveryService.verConsultaSesion.mockResolvedValue({
      nombre: 'Ana', nivel: 'Plata', saldoPuntos: 1234.5, movimientos: [], usosRestantes: 2,
    });

    const res = await request(app).post('/api/loyalty/public/consulta/ver')
      .send({ telefono: '5512345678', token: 'token-real' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ nombre: 'Ana', nivel: 'Plata', saldoPuntos: 1234.5, movimientos: [], usosRestantes: 2 });
    expect(recoveryService.verConsultaSesion).toHaveBeenCalledWith('5512345678', 'token-real', expect.objectContaining({ ip: expect.anything() }));
  });

  test('GET /consulta/ver no existe (solo POST, D8: el token nunca en una URL)', async () => {
    const { app } = freshApp();
    const res = await request(app).get('/api/loyalty/public/consulta/ver').query({ telefono: '5512345678', token: 'token-real' });
    expect(res.status).toBe(404);
  });

  test('falla de negocio (422 LOYALTY_RECOVERY_FAILED) se propaga con el mismo código', async () => {
    const { app, recoveryService } = freshApp();
    const err = Object.assign(new Error('El código no es válido o expiró.'), {
      statusCode: 422, code: 'LOYALTY_RECOVERY_FAILED',
    });
    recoveryService.verConsultaSesion.mockRejectedValue(err);

    const res = await request(app).post('/api/loyalty/public/consulta/ver')
      .send({ telefono: '5512345678', token: 'token-malo' });

    expect(res.status).toBe(422);
    expect(res.body.code).toBe('LOYALTY_RECOVERY_FAILED');
  });

  test('el 11vo request a /consulta/ver en 15min → 429, antes de llamar al service', async () => {
    const { app, recoveryService } = freshApp();
    recoveryService.verConsultaSesion.mockResolvedValue({ nombre: 'Ana', nivel: 'Plata', saldoPuntos: 1, movimientos: [], usosRestantes: 2 });

    for (let i = 0; i < 10; i++) {
      // eslint-disable-next-line no-await-in-loop
      const r = await request(app).post('/api/loyalty/public/consulta/ver').send({ telefono: '5512345678', token: 'token-real' });
      expect(r.status).toBe(200);
    }
    const onceavo = await request(app).post('/api/loyalty/public/consulta/ver').send({ telefono: '5512345678', token: 'token-real' });
    expect(onceavo.status).toBe(429);
    expect(recoveryService.verConsultaSesion).toHaveBeenCalledTimes(10);
  });

  test('rate limit de /consulta/ver es independiente del de /consulta/confirmar', async () => {
    const { app, recoveryService } = freshApp();
    recoveryService.verConsultaSesion.mockResolvedValue({ nombre: 'Ana', nivel: 'Plata', saldoPuntos: 1, movimientos: [], usosRestantes: 2 });
    recoveryService.confirmarCodigoConsulta.mockResolvedValue({ nombre: 'Ana', nivel: 'Plata', saldoPuntos: 1 });

    for (let i = 0; i < 10; i++) {
      // eslint-disable-next-line no-await-in-loop
      await request(app).post('/api/loyalty/public/consulta/ver').send({ telefono: '5512345678', token: 'token-real' });
    }
    const verBloqueado = await request(app).post('/api/loyalty/public/consulta/ver').send({ telefono: '5512345678', token: 'token-real' });
    expect(verBloqueado.status).toBe(429);

    const confirmar = await request(app).post('/api/loyalty/public/consulta/confirmar').send({ telefono: '5512345678', codigo: '654321' });
    expect(confirmar.status).toBe(200);
  });

  test('POST /consulta/email llega al service sin Authorization y devuelve el mensaje', async () => {
    const { app, recoveryService } = freshApp();
    recoveryService.guardarEmailConsulta.mockResolvedValue({ mensaje: 'Listo, guardamos tu correo.' });

    const res = await request(app).post('/api/loyalty/public/consulta/email')
      .send({ telefono: '5512345678', token: 'token-real', email: 'cliente@correo.com' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ mensaje: 'Listo, guardamos tu correo.' });
    expect(recoveryService.guardarEmailConsulta).toHaveBeenCalledWith(
      '5512345678', 'token-real', 'cliente@correo.com', expect.objectContaining({ ip: expect.anything() }),
    );
  });

  test('GET /consulta/email no existe (solo POST)', async () => {
    const { app } = freshApp();
    const res = await request(app).get('/api/loyalty/public/consulta/email').query({ telefono: '5512345678' });
    expect(res.status).toBe(404);
  });

  test('falla de negocio (422 LOYALTY_EMAIL_INVALIDO, distinto de LOYALTY_RECOVERY_FAILED) se propaga con su propio código', async () => {
    const { app, recoveryService } = freshApp();
    const err = Object.assign(new Error('Ingresá un correo electrónico válido.'), {
      statusCode: 422, code: 'LOYALTY_EMAIL_INVALIDO',
    });
    recoveryService.guardarEmailConsulta.mockRejectedValue(err);

    const res = await request(app).post('/api/loyalty/public/consulta/email')
      .send({ telefono: '5512345678', token: 'token-real', email: 'no-es-un-correo' });

    expect(res.status).toBe(422);
    expect(res.body.code).toBe('LOYALTY_EMAIL_INVALIDO');
  });

  test('crearLimitersSesion() exporta una factory (MemoryStore fresca por llamada, no singleton)', () => {
    const { loyaltyPublicRoutes } = freshApp();
    const { crearLimitersSesion } = loyaltyPublicRoutes;
    expect(typeof crearLimitersSesion).toBe('function');
    const par1 = crearLimitersSesion();
    const par2 = crearLimitersSesion();
    expect(par1.ver).not.toBe(par2.ver);
    expect(par1.email).not.toBe(par2.email);
  });
});

describe('store en Redis de los 4 limiters dedicados (cluster nativo + Redis, 2026-11-11)', () => {
  test('cargar el router construye 4 RedisStore — uno por limiter, cada uno con su propio prefix (nunca un store compartido)', () => {
    const { RedisStore } = freshApp();

    // app.js también usa RedisStore (limiter global, prefix 'rl:global:'), pero ese vive
    // en otro módulo (app.js no se requiere en este test) — acá solo cargan los 4 de
    // loyalty-public.routes.js: solicitar/confirmar (crearLimitersRecuperacion, ya
    // instanciados a nivel de módulo) + ver/email (crearLimitersSesion, ídem).
    expect(RedisStore).toHaveBeenCalledTimes(4);

    const prefixes = RedisStore.mock.calls.map(([options]) => options.prefix);
    expect(new Set(prefixes).size).toBe(4); // los 4 prefixes son distintos entre sí
    expect(prefixes).toEqual(expect.arrayContaining([
      'rl:loyalty-solicitar:', 'rl:loyalty-confirmar:', 'rl:loyalty-ver:', 'rl:loyalty-email:',
    ]));

    // Cada llamada recibe un `sendCommand` propio que delega al redisClient compartido —
    // no literalmente la misma función entre limiters, pero sí la misma firma/contrato.
    RedisStore.mock.calls.forEach(([options]) => {
      expect(typeof options.sendCommand).toBe('function');
    });

    // 4 instancias distintas (cada `new RedisStore(...)` arma su propio Map interno) —
    // es justo lo que evita que, p. ej., /recuperacion/solicitar y /recuperacion/confirmar
    // (o /consulta/ver y /consulta/email) pisen el contador del otro en Redis.
    const instancias = new Set(RedisStore.mock.instances);
    expect(instancias.size).toBe(4);
  });
});
