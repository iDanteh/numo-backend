'use strict';

/**
 * auth.real.js — Middleware de autenticación con Auth0.
 *
 * Valida tokens RS256 JWS emitidos por Auth0 y resuelve el rol/estado
 * del usuario desde la base de datos propia (colección users).
 *
 * Variables de entorno requeridas:
 *   AUTH0_DOMAIN   — dominio del tenant, ej: myapp.us.auth0.com
 *   AUTH0_AUDIENCE — API identifier registrado
 */

const { auth }  = require('express-oauth2-jwt-bearer');
const userSvc   = require('../../domains/users/user.service');

const NOMBRE_CLAIM = 'https://cfdi-comparator/nombre';
const EMAIL_CLAIM  = 'https://cfdi-comparator/email';

const jwtCheck = auth({
  issuerBaseURL:   `https://${process.env.AUTH0_DOMAIN}/`,
  audience:        process.env.AUTH0_AUDIENCE,
  tokenSigningAlg: 'RS256',
});

/**
 * authenticate — valida el JWT y puebla req.user con datos de la DB.
 * Si el usuario no existe en DB lo crea con rol 'tienda'.
 * Si el usuario está desactivado devuelve 403.
 */
const authenticate = (req, res, next) => {
  jwtCheck(req, res, async (err) => {
    if (err) {
      console.error('[auth] jwtCheck falló:', err.message);
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
        _id:    payload.sub,
        dbId:   userDoc._id.toString(),
        nombre: userDoc.nombre || payload[NOMBRE_CLAIM] || '',
        email:  userDoc.email  || payload[EMAIL_CLAIM]  || '',
        role:   userDoc.role,
      };

      next();
    } catch (dbErr) {
      console.error('[auth] Error resolviendo usuario en DB:', dbErr.message);
      return res.status(500).json({ error: 'Error interno de autenticación' });
    }
  });
};

/**
 * authorize — verifica que req.user.role sea uno de los roles permitidos.
 * Debe usarse siempre después de authenticate.
 */
const authorize = (...roles) => (req, res, next) => {
  if (!roles.includes(req.user?.role)) {
    return res.status(403).json({
      error: 'Acceso denegado: no tienes el rol requerido para esta acción.',
    });
  }
  next();
};

module.exports = { authenticate, authorize };
