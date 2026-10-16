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

const router = express.Router();

router.post('/consulta', ...crearLimitersConsulta(), asyncHandler(async (req, res) => {
  const { telefono, nip } = req.body ?? {};
  const resultado = await publicService.consultaPublica(telefono, nip, { ip: req.ip });
  res.json(resultado);
}));

module.exports = router;
module.exports.crearLimitersConsulta = crearLimitersConsulta;
