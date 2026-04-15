const { logger } = require('../utils/logger');

const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

const errorHandler = (err, req, res, next) => {
  logger.error(`${err.name}: ${err.message}`, { stack: err.stack, url: req.url });

  // Mongoose validation error
  if (err.name === 'ValidationError') {
    const errors = Object.values(err.errors).map(e => ({ field: e.path, message: e.message }));
    return res.status(400).json({ error: 'Error de validación', errors });
  }

  // Mongoose duplicate key
  if (err.code === 11000) {
    const field = Object.keys(err.keyValue)[0];
    return res.status(409).json({ error: `Ya existe un registro con ese ${field}`, field });
  }

  // Mongoose cast error (ID inválido)
  if (err.name === 'CastError') {
    return res.status(400).json({ error: `ID inválido: ${err.value}` });
  }

  // JWT errors
  if (err.name === 'JsonWebTokenError') {
    return res.status(401).json({ error: 'Token inválido' });
  }

  // Multer errors (upload de archivos)
  if (err.name === 'MulterError') {
    if (err.code === 'LIMIT_UNEXPECTED_FILE') {
      return res.status(400).json({
        error: `Campo de archivo inesperado: "${err.field}". Para subir XMLs usa el campo "xmlFiles"; para Excel usa el campo "excelFile".`,
      });
    }
    if (err.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ error: 'El archivo supera el tamaño máximo permitido.' });
    }
    return res.status(400).json({ error: `Error al procesar el archivo: ${err.message}` });
  }

  // Default
  const status = err.statusCode || err.status || 500;
  const message = process.env.NODE_ENV === 'production' && status === 500
    ? 'Error interno del servidor'
    : err.message;

  res.status(status).json({ error: message });
};

module.exports = errorHandler;
module.exports.asyncHandler = asyncHandler;
