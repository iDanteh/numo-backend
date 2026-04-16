'use strict';

/**
 * visor/middleware/auth.js
 *
 * Valida tokens Auth0 (RS256) y resuelve el usuario desde MongoDB,
 * igual que banks/shared/middleware/auth.real.js.
 *
 * Requiere:
 *   AUTH0_DOMAIN   — ej: dev-xxx.us.auth0.com
 *   AUTH0_AUDIENCE — ej: https://dev-xxx.us.auth0.com/api/v2/
 */

const { auth } = require('express-oauth2-jwt-bearer');
const userSvc  = require('../../banks/domains/users/user.service');

const NOMBRE_CLAIM = 'https://cfdi-comparator/nombre';
const EMAIL_CLAIM  = 'https://cfdi-comparator/email';

const jwtCheck = auth({
  issuerBaseURL:   `https://${process.env.AUTH0_DOMAIN}/`,
  audience:        process.env.AUTH0_AUDIENCE,
  tokenSigningAlg: 'RS256',
});

const authenticate = (req, res, next) => {
  jwtCheck(req, res, async (err) => {
    if (err) {
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
        _id:   userDoc._id,
        role:  userDoc.role,
        email: userDoc.email,
      };

      next();
    } catch (dbErr) {
      return res.status(500).json({ error: 'Error interno de autenticación' });
    }
  });
};

const authorize = (...roles) => (req, res, next) => {
  if (!roles.includes(req.user?.role)) {
    return res.status(403).json({ error: `Se requiere rol: ${roles.join(' o ')}` });
  }
  next();
};

module.exports = { authenticate, authorize };
