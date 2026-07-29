'use strict';

/**
 * shared/middleware/auth.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Middleware de autenticación y autorización compartido entre todos los módulos.
 *
 * authenticate:
 *   Valida el JWT RS256 emitido por Auth0.
 *   Si el usuario no existe en PostgreSQL lo crea con rol 'tienda'.
 *   Puebla req.user = { _id, dbId, nombre, email, role }.
 *
 * permit(...permissions):
 *   Verifica que req.user.role tenga TODOS los permisos indicados.
 *   Usa la tabla de permisos de src/shared/config/rbac.js.
 *   Para agregar/modificar permisos de un rol → editar solo rbac.js.
 *
 * Variables de entorno requeridas:
 *   AUTH0_DOMAIN   — dominio del tenant, ej: myapp.us.auth0.com
 *   AUTH0_AUDIENCE — API identifier registrado en Auth0
 */

const { auth }    = require('express-oauth2-jwt-bearer');
const jwt         = require('jsonwebtoken');
const config      = require('../../config/env');
const userSvc     = require('../../banks/domains/users/user.service');
const rbacStore   = require('../services/rbac-store');
const { logger }  = require('../utils/logger');

const NOMBRE_CLAIM = 'https://cfdi-comparator/nombre';
const EMAIL_CLAIM  = 'https://cfdi-comparator/email';

const jwtCheck = auth({
  issuerBaseURL:   `https://${process.env.AUTH0_DOMAIN}/`,
  audience:        process.env.AUTH0_AUDIENCE,
  tokenSigningAlg: 'RS256',
});

// Atajo de SOLO pruebas: acepta tokens HS256 firmados con TEST_AUTH_SECRET en
// vez de exigir un JWT RS256 real de Auth0. Doble candado — nunca se activa en
// producción aunque alguien deje la variable puesta por error, y nunca se
// activa sin la variable aunque el entorno no sea producción. Ver
// shared/routes/dev-auth.routes.js (emite estos tokens) y POST /api/dev/test-token.
const TEST_AUTH_ENABLED = config.env !== 'production' && !!process.env.TEST_AUTH_SECRET;
if (TEST_AUTH_ENABLED) {
  logger.warn(`[auth] TEST_AUTH_SECRET activo (NODE_ENV=${config.env}) — /api/dev/test-token y sus tokens HS256 están habilitados`);
}

/**
 * Puebla req.user a partir de los claims ya validados (de Auth0 o del atajo
 * de pruebas) consultando/creando el usuario correspondiente en PostgreSQL.
 */
async function resolveUser(payload, req, res, next) {
  try {
    const userDoc = await userSvc.findOrCreate({
      auth0Sub: payload.sub,
      nombre:   payload[NOMBRE_CLAIM] ?? '',
      email:    payload[EMAIL_CLAIM]  ?? payload.email ?? '',
    });

    if (!userDoc.isActive) {
      return res.status(403).json({ error: 'Usuario desactivado. Contacta al administrador.' });
    }

    req.user = {
      _id:    payload.sub,                   // auth0 sub (string)
      dbId:   String(userDoc.id),            // PG integer id como string
      nombre: userDoc.nombre || payload[NOMBRE_CLAIM] || '',
      email:  userDoc.email  || payload[EMAIL_CLAIM]  || '',
      role:   userDoc.role,
      // Empresas fijas asignadas directo al usuario (puede tener varias) —
      // [] = sin restricción, puede elegir cualquier empresa.
      empresaRfcs: userDoc.empresaRfcs ?? [],
      // Permisos extra asignados directo a este usuario, además de los que ya
      // le da su rol — se leen aquí porque el registro ya se consulta en cada
      // request (sin round-trip extra). Puramente aditivo, ver rbac-store.js.
      extraPermissions: userDoc.extraPermissions ?? [],
    };

    next();
  } catch (dbErr) {
    logger.error(`[auth] Error resolviendo usuario en DB: ${dbErr.message}`);
    return res.status(500).json({ error: 'Error interno de autenticación' });
  }
}

/**
 * Valida el JWT y puebla req.user con datos desde PostgreSQL.
 * Si el usuario está desactivado devuelve 403.
 */
const authenticate = (req, res, next) => {
  if (TEST_AUTH_ENABLED) {
    const authHeader = req.get('Authorization') || '';
    const bearer     = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (bearer) {
      try {
        const payload = jwt.verify(bearer, process.env.TEST_AUTH_SECRET, { algorithms: ['HS256'] });
        return resolveUser(payload, req, res, next);
      } catch (_testErr) {
        // No es un token de prueba válido (o es un JWT real RS256 de Auth0) —
        // sigue el flujo normal de abajo.
      }
    }
  }

  jwtCheck(req, res, (err) => {
    if (err) {
      logger.debug(`[auth] jwtCheck falló: ${err.message}`);
      return res.status(401).json({ error: 'Token inválido', details: err.message });
    }
    return resolveUser(req.auth?.payload ?? {}, req, res, next);
  });
};

/**
 * Verifica que el usuario autenticado tenga TODOS los permisos indicados.
 * Los permisos se consultan en PostgreSQL (tabla roles) con cache de 5 min.
 * Para agregar/modificar permisos de un rol → usar la API /api/users/roles.
 *
 * Uso en rutas:
 *   router.post('/upload', authenticate, permit('banks:import'), handler);
 *   router.patch('/config', authenticate, permit('banks:config'), handler);
 *
 * @param {...string} permissions  Permisos requeridos (todos deben cumplirse).
 */
const permit = (...permissions) => async (req, res, next) => {
  const role = req.user?.role;

  if (!role) {
    return res.status(401).json({ error: 'No autenticado' });
  }

  try {
    const ok = await rbacStore.hasAllPermissions(role, permissions, req.user.extraPermissions);
    if (!ok) {
      return res.status(403).json({
        error:    'Permisos insuficientes para esta acción.',
        required: permissions,
      });
    }
    next();
  } catch (err) {
    next(err);
  }
};

module.exports = { authenticate, permit, NOMBRE_CLAIM, EMAIL_CLAIM };
