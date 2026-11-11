'use strict';

/**
 * loyalty/loyalty-public.routes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — router PÚBLICO/anónimo, montado en `/api/loyalty/public`
 * (D9, ver design.md). Primer endpoint de este backend que no exige ningún
 * token y devuelve datos personales — por eso vive en su propio router, SIN
 * `authenticate` en ninguna ruta, nunca importado junto al router de staff.
 *
 * Es un POST, no un GET (D8): `traffic-tracker.middleware.js` persiste
 * `req.originalUrl` (que para un GET incluiría la querystring) a Mongo para
 * toda respuesta 400/409/422/5xx — un GET con `?telefono&nip` filtraría el NIP
 * a esos logs. Con POST, solo el path se loguea, nunca el body.
 *
 * Rate limiting dedicado (specs/loyalty-public-lookup "Dedicated Rate
 * Limiting"): dos `express-rate-limit` apilados por IP (5/min, 30/h), ADEMÁS
 * del limiter global `/api/` ya montado en app.js — nunca lo reemplazan.
 * `crearLimitersConsulta()` es una factory (mismo patrón que `cfdis.js:17-28`)
 * para que cada `require()` de este router en tests obtenga una `MemoryStore`
 * nueva, sin contaminar contadores entre casos.
 */
const express = require('express');
const rateLimit = require('express-rate-limit');
const { RedisStore } = require('rate-limit-redis');
const redisClient = require('../shared/utils/redis-client');
const { asyncHandler } = require('../shared/middleware/error-handler');
const publicService = require('./domains/public/loyalty-public.service');
const recoveryService = require('./domains/public/loyalty-recovery.service');

function crearLimitersConsulta() {
  const handler = (_req, res) => res.status(429).json({
    success: false,
    error: 'Demasiadas consultas. Intenta de nuevo más tarde.',
    code: 'RATE_LIMIT_EXCEEDED',
    retryAfter: 60,
  });
  const base = { standardHeaders: true, legacyHeaders: false, handler };
  return [
    rateLimit({ ...base, windowMs: 60 * 1000, max: 5 }),
    rateLimit({ ...base, windowMs: 60 * 60 * 1000, max: 30 }),
  ];
}

// Recuperación de NIP en 2 pasos (D6-D8, mismo threat model que /consulta).
// "Solicitar" es más estricto que "confirmar" porque es el paso que "envía"
// algo (simulado por ahora, ver loyalty-recovery.service.js) — limitar cuánto
// puede spamear un mismo IP, independiente del lockout por tarjeta que ya
// cubre fuerza bruta del código en /confirmar.
//
// STRESS_TEST_*_MAX (2026-11-09, ampliado 2026-11-10): override SOLO para la réplica local de
// stress-test (ver stress-test/docker-compose.stress.yml) — todo el tráfico de k6 sale de una
// sola IP, así que CUALQUIERA de estos límites reales (pensados para distinguir IPs distintas)
// lo taparía casi de inmediato. Mismo candado que TEST_AUTH_SECRET/codigoDev: nunca se activa en
// producción, y en cualquier otro ambiente requiere setear la variable a propósito.
// Bug real encontrado 2026-11-10: solo `solicitar` tenía este override — `confirmar`/`ver`
// seguían fijos en 10/15min, así que con >10 confirmaciones en toda la corrida (triv ial con
// varios VUs) la inmensa mayoría de los flujos fallaba con 429 a partir del 11vo — un techo
// plano de "10 en total", no una señal real de capacidad del servidor. Invalida los resultados
// de fallas de las corridas anteriores a este fix.
function _maxConStress(envVar, defaultMax) {
  return (process.env.NODE_ENV !== 'production' && Number(process.env[envVar])) || defaultMax;
}

// Store en Redis para que el presupuesto de cada limiter sea real cross-worker bajo
// cluster nativo (src/cluster.js) — sin esto, cada worker llevaría su propia cuenta en
// memoria y el límite efectivo sería `max * workers`. UN `RedisStore` nuevo POR limiter
// (nunca uno compartido en `base`, pese a que `base` sí se comparte entre los dos
// limiters de cada factory de abajo): `rate-limit-redis` arma la key de Redis como
// `prefix + key` y `key` por default es solo la IP — si `solicitar`/`confirmar` (o
// `ver`/`email`) compartieran la MISMA instancia con el MISMO prefix, un request a
// cualquiera de los dos incrementaría el mismo contador en Redis y sus presupuestos
// (3/15min vs. 10/15min, o 10/15min vs. 20/15min) dejarían de ser independientes.
function _redisStore(prefix) {
  return new RedisStore({ prefix, sendCommand: (...args) => redisClient.sendCommand(args) });
}

function crearLimitersRecuperacion() {
  const handler = (_req, res) => res.status(429).json({
    success: false,
    error: 'Demasiados intentos. Intenta de nuevo más tarde.',
    code: 'RATE_LIMIT_EXCEEDED',
    retryAfter: 60,
  });
  // `passOnStoreError: true` (hallazgo real 2026-11-11, ver mismo criterio en
  // app.js): un fallo del store de Redis degrada a "sin límite" en vez de
  // bloquear la request — Redis caído nunca debe tumbar el tráfico real.
  const base = {
    standardHeaders: true, legacyHeaders: false, handler, passOnStoreError: true,
  };
  return {
    solicitar: rateLimit({ ...base, windowMs: 15 * 60 * 1000, max: _maxConStress('STRESS_TEST_SOLICITAR_MAX', 3), store: _redisStore('rl:loyalty-solicitar:') }),
    confirmar: rateLimit({ ...base, windowMs: 15 * 60 * 1000, max: _maxConStress('STRESS_TEST_CONFIRMAR_MAX', 10), store: _redisStore('rl:loyalty-confirmar:') }),
  };
}

// Sesión de consulta post-código (2026-11-09): "ver de nuevo" y "guardar email". Misma factory
// que crearLimitersRecuperacion — presupuesto propio, no comparten contador con
// /consulta/confirmar. `ver` reusa el mismo presupuesto que confirmar (10/15min): es el mismo
// vector de fuerza bruta (adivinar un secreto de 15 min de vida por teléfono), solo que acá el
// secreto es el token de sesión en vez del código OTP. `email` es más laxo (20/15min): no es un
// vector de fuerza bruta por sí solo (ya exige una sesión vigente, que ya pasó por el limiter de
// `confirmar` para existir), solo evita spam de formato.
function crearLimitersSesion() {
  const handler = (_req, res) => res.status(429).json({
    success: false,
    error: 'Demasiados intentos. Intenta de nuevo más tarde.',
    code: 'RATE_LIMIT_EXCEEDED',
    retryAfter: 60,
  });
  // `passOnStoreError: true` — ver el mismo comentario en crearLimitersRecuperacion().
  const base = {
    standardHeaders: true, legacyHeaders: false, handler, passOnStoreError: true,
  };
  return {
    ver:   rateLimit({ ...base, windowMs: 15 * 60 * 1000, max: _maxConStress('STRESS_TEST_VER_MAX', 10), store: _redisStore('rl:loyalty-ver:') }),
    email: rateLimit({ ...base, windowMs: 15 * 60 * 1000, max: _maxConStress('STRESS_TEST_EMAIL_MAX', 20), store: _redisStore('rl:loyalty-email:') }),
  };
}

const router = express.Router();
const limitersRecuperacion = crearLimitersRecuperacion();
const limitersSesion = crearLimitersSesion();

router.post('/consulta', ...crearLimitersConsulta(), asyncHandler(async (req, res) => {
  const { telefono, nip } = req.body ?? {};
  const resultado = await publicService.consultaPublica(telefono, nip, { ip: req.ip });
  res.json(resultado);
}));

// POST, nunca GET (D8: traffic-tracker.middleware.js persiste req.originalUrl
// en respuestas de error — un GET con querystring filtraría el código/NIP).
router.post('/recuperacion/solicitar', limitersRecuperacion.solicitar, asyncHandler(async (req, res) => {
  const { telefono } = req.body ?? {};
  const resultado = await recoveryService.solicitarCodigo(telefono, { ip: req.ip });
  res.json(resultado);
}));

// Consulta de puntos vía código de verificación (2026-10-26, reemplaza al NIP
// estático en /consulta — ver memoria project_lealtad_consulta_publica_otp).
// "Solicitar" reusa exactamente /recuperacion/solicitar (mismo código, mismo
// mensaje genérico); acá solo cambia qué se hace al confirmarlo. La ruta
// pública `/recuperacion/confirmar` (flujo "recuperar NIP olvidado", único
// otro consumidor de este mismo limiter) se eliminó el mismo día — quedó
// huérfana una vez que el NIP dejó de ser necesario para el cliente final.
router.post('/consulta/confirmar', limitersRecuperacion.confirmar, asyncHandler(async (req, res) => {
  const { telefono, codigo } = req.body ?? {};
  const resultado = await recoveryService.confirmarCodigoConsulta(telefono, codigo, { ip: req.ip });
  res.json(resultado);
}));

// Sesión de consulta post-código (2026-11-09, ver memoria project_lealtad_consulta_publica_otp):
// "ver de nuevo" el saldo con el token emitido por /consulta/confirmar, sin pedir un código
// nuevo — hasta 3 veces por sesión (ver loyalty-recovery.service.js#verConsultaSesion). POST,
// nunca GET (D8, mismo criterio que el resto de este router: un GET filtraría el token a los
// logs de tráfico vía querystring).
router.post('/consulta/ver', limitersSesion.ver, asyncHandler(async (req, res) => {
  const { telefono, token } = req.body ?? {};
  const resultado = await recoveryService.verConsultaSesion(telefono, token, { ip: req.ip });
  res.json(resultado);
}));

// Deja un correo de contacto futuro (2026-11-09) — requiere la misma sesión vigente que
// /consulta/ver, pero no consume un uso de vista (ver guardarEmailConsulta). Opcional: el
// cliente puede ignorar este campo.
router.post('/consulta/email', limitersSesion.email, asyncHandler(async (req, res) => {
  const { telefono, token, email } = req.body ?? {};
  const resultado = await recoveryService.guardarEmailConsulta(telefono, token, email, { ip: req.ip });
  res.json(resultado);
}));

module.exports = router;
module.exports.crearLimitersConsulta = crearLimitersConsulta;
module.exports.crearLimitersRecuperacion = crearLimitersRecuperacion;
module.exports.crearLimitersSesion = crearLimitersSesion;
