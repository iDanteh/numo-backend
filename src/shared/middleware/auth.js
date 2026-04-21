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

/**
 * Valida el JWT y puebla req.user con datos desde PostgreSQL.
 * Si el usuario está desactivado devuelve 403.
 */
const authenticate = (req, res, next) => {
  jwtCheck(req, res, async (err) => {
    if (err) {
      logger.debug(`[auth] jwtCheck falló: ${err.message}`);
      return res.status(401).json({ error: 'Token inválido', details: err.message });
    }

    const payload = req.auth?.payload ?? {};

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
      };

      next();
    } catch (dbErr) {
      logger.error(`[auth] Error resolviendo usuario en DB: ${dbErr.message}`);
      return res.status(500).json({ error: 'Error interno de autenticación' });
    }
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
    const ok = await rbacStore.hasAllPermissions(role, permissions);
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

module.exports = { authenticate, permit };
