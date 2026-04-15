'use strict';

/**
 * auth.stub.js — Middleware de autenticación en modo desarrollo.
 *
 * STUB: Siempre aprueba la solicitud y asigna un usuario ficticio.
 *
 * Para restaurar autenticación real:
 *   1. Crear auth.real.js con verificación JWT + lookup en User model
 *   2. Reemplazar los imports de 'auth.stub' por 'auth.real' en todos los routes
 *   3. En app.js, no se requiere ningún cambio adicional
 */

// eslint-disable-next-line no-unused-vars
const authenticate = (req, _res, next) => {
  req.user = { _id: 'dev|stub', dbId: '000000000000000000000001', nombre: 'Desarrollador', email: 'dev@local', role: 'admin' };
  next();
};

// eslint-disable-next-line no-unused-vars
const authorize = (..._roles) => (_req, _res, next) => next();

module.exports = { authenticate, authorize };
