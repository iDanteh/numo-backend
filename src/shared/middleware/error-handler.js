'use strict';

const { logger } = require('../utils/logger');

/**
 * asyncHandler — envuelve un handler async para que los errores lleguen al
 * middleware de Express sin necesidad de try/catch en cada handler.
 *
 * @param {Function} fn  Función async (req, res, next)
 */
const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

/**
 * errorHandler — manejador centralizado de errores.
 * Debe registrarse como ÚLTIMO middleware en app.js.
 *
 * Cubre:
 *  • AppError y subclases (NotFoundError, BadRequestError, etc.)
 *  • Errores de validación Mongoose y Sequelize
 *  • Clave duplicada Mongoose (código 11000) y Sequelize (UniqueConstraintError)
 *  • CastError de Mongoose (ID inválido)
 *  • JWT errors (JsonWebTokenError, TokenExpiredError)
 *  • Errores de Multer (límite de tamaño, campo inesperado)
 */
const errorHandler = (err, req, res, next) => { // eslint-disable-line no-unused-vars
  logger.error(`${err.name || 'Error'}: ${err.message}`, { stack: err.stack, url: req.url });

  // ── Mongoose: validación ──────────────────────────────────────────────────
  if (err.name === 'ValidationError' && err.errors) {
    const errors = Object.values(err.errors).map((e) => ({ field: e.path, message: e.message }));
    return res.status(400).json({ error: 'Error de validación', errors });
  }

  // ── Mongoose: clave duplicada ─────────────────────────────────────────────
  if (err.code === 11000) {
    const field = Object.keys(err.keyValue || {})[0] ?? 'campo';
    return res.status(409).json({ error: `Ya existe un registro con ese ${field}`, field });
  }

  // ── Mongoose: ID inválido ─────────────────────────────────────────────────
  if (err.name === 'CastError') {
    return res.status(400).json({ error: `ID inválido: ${err.value}` });
  }

  // ── Sequelize: violación de UNIQUE ────────────────────────────────────────
  if (err.name === 'SequelizeUniqueConstraintError') {
    const field = err.errors?.[0]?.path ?? 'campo';
    return res.status(409).json({ error: `Ya existe un registro con ese ${field}`, field });
  }

  // ── Sequelize: validación ─────────────────────────────────────────────────
  if (err.name === 'SequelizeValidationError') {
    const errors = (err.errors ?? []).map((e) => ({ field: e.path, message: e.message }));
    return res.status(400).json({ error: 'Error de validación', errors });
  }

  // ── JWT: token inválido / expirado ────────────────────────────────────────
  if (err.name === 'JsonWebTokenError') {
    return res.status(401).json({ error: 'Token inválido' });
  }
  if (err.name === 'TokenExpiredError') {
    return res.status(401).json({ error: 'Token expirado' });
  }

  // ── Multer: errores de carga de archivos ──────────────────────────────────
  if (err.name === 'MulterError') {
    if (err.code === 'LIMIT_UNEXPECTED_FILE') {
      return res.status(400).json({
        error: `Campo de archivo inesperado: "${err.field}". Revisa el nombre del campo en el formulario.`,
      });
    }
    if (err.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ error: 'El archivo supera el tamaño máximo permitido.' });
    }
    return res.status(400).json({ error: `Error al procesar el archivo: ${err.message}` });
  }

  // ── AppError y subclases (NotFoundError, BadRequestError, etc.) ───────────
  const status  = err.statusCode || err.status || 500;
  const message = process.env.NODE_ENV === 'production' && status === 500
    ? 'Error interno del servidor'
    : err.message;

  const body = { error: message };
  if (err.errors) body.errors = err.errors;

  res.status(status).json(body);
};

module.exports = errorHandler;
module.exports.asyncHandler = asyncHandler;
