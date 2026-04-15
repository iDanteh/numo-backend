'use strict';

const { logger } = require('../utils/logger');

/**
 * asyncHandler — envuelve un handler async para pasar errores al middleware de Express.
 */
const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

/**
 * errorHandler — manejador centralizado de errores (debe ser el último middleware).
 */
const errorHandler = (err, req, res, next) => { // eslint-disable-line no-unused-vars
  logger.error(`${err.name}: ${err.message}`, { stack: err.stack, url: req.url });

  // Mongoose: error de validación
  if (err.name === 'ValidationError') {
    const errors = Object.values(err.errors).map((e) => ({ field: e.path, message: e.message }));
    return res.status(400).json({ error: 'Error de validación', errors });
  }

  // Mongoose: clave duplicada
  if (err.code === 11000) {
    const field = Object.keys(err.keyValue || {})[0] ?? 'campo';
    return res.status(409).json({ error: `Ya existe un registro con ese ${field}`, field });
  }

  // Mongoose: ID inválido
  if (err.name === 'CastError') {
    return res.status(400).json({ error: `ID inválido: ${err.value}` });
  }

  // Errores de dominio con statusCode explícito (AppError y subclases)
  const status = err.statusCode || err.status || 500;

  // En producción, no exponer detalles de errores 500
  const message = process.env.NODE_ENV === 'production' && status === 500
    ? 'Error interno del servidor'
    : err.message;

  // Adjuntar errores adicionales si los hay (ej. errores de hojas Excel)
  const body = { error: message };
  if (err.errors) body.errors = err.errors;

  res.status(status).json(body);
};

module.exports = errorHandler;
module.exports.asyncHandler = asyncHandler;
