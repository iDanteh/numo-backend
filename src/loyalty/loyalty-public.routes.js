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
function crearLimitersRecuperacion() {
  const handler = (_req, res) => res.status(429).json({
    success: false,
    error: 'Demasiados intentos. Intenta de nuevo más tarde.',
    code: 'RATE_LIMIT_EXCEEDED',
    retryAfter: 60,
  });
  const base = { standardHeaders: true, legacyHeaders: false, handler };
  return {
    solicitar: rateLimit({ ...base, windowMs: 15 * 60 * 1000, max: 3 }),
    confirmar: rateLimit({ ...base, windowMs: 15 * 60 * 1000, max: 10 }),
  };
}

const router = express.Router();
const limitersRecuperacion = crearLimitersRecuperacion();

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

module.exports = router;
module.exports.crearLimitersConsulta = crearLimitersConsulta;
module.exports.crearLimitersRecuperacion = crearLimitersRecuperacion;
