'use strict';

/**
 * shared/routes/dev-auth.routes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * POST /api/dev/test-token — SOLO DESARROLLO/PRUEBAS.
 *
 * Emite un token HS256 (firmado con TEST_AUTH_SECRET) que authenticate()
 * (shared/middleware/auth.js) acepta como atajo en vez de exigir un JWT RS256
 * real de Auth0 — pensado para probar endpoints en Insomnia/Postman sin pasar
 * por el login completo, y para compartir un token ya con el rol correcto con
 * otras personas que necesiten probar lo mismo.
 *
 * Este archivo solo se registra en app.js si NODE_ENV != production Y existe
 * TEST_AUTH_SECRET — nunca queda expuesto en producción (ver el `if` en app.js).
 * Además, esta ruta en sí exige el mismo secreto en el header X-Test-Auth-Key,
 * para que no cualquiera en un entorno compartido pueda emitirse un token con
 * el rol que quiera con solo conocer la URL.
 */

const express = require('express');
const crypto  = require('crypto');
const jwt     = require('jsonwebtoken');
const { asyncHandler }       = require('../middleware/error-handler');
const { NOMBRE_CLAIM, EMAIL_CLAIM } = require('../middleware/auth');
const userSvc = require('../../banks/domains/users/user.service');

const router = express.Router();

const DEFAULT_SUB    = 'test|shared-tester';
const DEFAULT_NOMBRE = 'Usuario de Pruebas';
const DEFAULT_EMAIL  = 'test-tester@numo.local';
const TOKEN_TTL       = '12h';

function requireTestAuthKey(req, res, next) {
  const expected = process.env.TEST_AUTH_SECRET || '';
  const received = req.get('X-Test-Auth-Key') || '';
  const a = Buffer.from(received);
  const b = Buffer.from(expected);
  const valid = expected.length > 0 && a.length === b.length && crypto.timingSafeEqual(a, b);
  if (!valid) return res.status(401).json({ error: 'X-Test-Auth-Key inválida o faltante' });
  next();
}

// POST /api/dev/test-token
// Body opcional: { auth0Sub, nombre, email, role }
// - auth0Sub/nombre/email por default apuntan a un mismo "usuario de pruebas"
//   compartido, para que distintas personas reusen la misma identidad/rol sin
//   crear un usuario nuevo cada vez.
// - role (opcional): si se manda y el usuario no lo tiene ya, se le asigna de
//   una vez (mismo mecanismo que PATCH /api/users/:id/role) — así el token
//   queda listo para probar ese rol sin pasos manuales adicionales.
router.post('/', requireTestAuthKey, asyncHandler(async (req, res) => {
  const auth0Sub = String(req.body.auth0Sub || DEFAULT_SUB).trim();
  const nombre   = String(req.body.nombre   || DEFAULT_NOMBRE).trim();
  const email    = String(req.body.email    || DEFAULT_EMAIL).trim();
  const role     = req.body.role ? String(req.body.role).trim() : null;

  const userDoc = await userSvc.findOrCreate({ auth0Sub, nombre, email });
  if (role && role !== userDoc.role) {
    await userSvc.updateRole(userDoc.id, role);
  }

  const token = jwt.sign(
    { sub: auth0Sub, [NOMBRE_CLAIM]: nombre, [EMAIL_CLAIM]: email },
    process.env.TEST_AUTH_SECRET,
    { algorithm: 'HS256', expiresIn: TOKEN_TTL },
  );

  res.json({
    token,
    tokenType: 'Bearer',
    expiresIn: TOKEN_TTL,
    user: { auth0Sub, nombre, email, role: role || userDoc.role },
    aviso: 'Token de PRUEBA (HS256) — solo funciona mientras el backend corra con NODE_ENV != production y TEST_AUTH_SECRET configurado. No usar en producción.',
  });
}));

module.exports = router;
