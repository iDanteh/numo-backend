'use strict';

const winston = require('winston');

const { combine, timestamp, printf, colorize, errors } = winston.format;

// BUG REAL encontrado 2026-08-17: el patrón `logger.warn('etiqueta:', variable)` — usado
// en 16 lugares de todo el backend — perdía silenciosamente ese 2do argumento. winston
// guarda los argumentos extra en `info[Symbol.for('splat')]`, pero como este printf solo
// leía `message`, nunca se veían — el log mostraba literalmente "etiqueta:" sin nada
// después. `winston.format.splat()` NO alcanza para arreglarlo: solo interpola cuando el
// mensaje tiene placeholders %s/%d/etc (verificado corriendo el código real), y estas
// llamadas nunca los usan. Se arma el mismo comportamiento de `console.log(a, b, c)` a
// mano acá — errores se expanden con su `.stack`, objetos con JSON.stringify, y se filtran
// null/undefined para no imprimir "undefined" si alguien pasa un argumento vacío por error.
const SPLAT = Symbol.for('splat');
const logFormat = printf((info) => {
  const { level, message, timestamp: ts, stack } = info;
  const extraArgs = (info[SPLAT] || []).filter(a => a !== undefined && a !== null);
  const extra = extraArgs.length
    ? ' ' + extraArgs
        .map(a => (a instanceof Error ? a.stack : typeof a === 'object' ? JSON.stringify(a) : String(a)))
        .join(' ')
    : '';
  return `${ts} [${level}]: ${stack || message}${extra}`;
});

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: combine(
    timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    errors({ stack: true }),
    logFormat,
  ),
  transports: [
    new winston.transports.Console({
      format: combine(colorize(), timestamp({ format: 'HH:mm:ss' }), logFormat),
    }),
    new winston.transports.File({
      filename: process.env.LOG_FILE || 'logs/app.log',
      maxsize:  10 * 1024 * 1024, // 10 MB
      maxFiles: 5,
    }),
    new winston.transports.File({
      filename: 'logs/errors.log',
      level:    'error',
      maxsize:  10 * 1024 * 1024,
      maxFiles: 5,
    }),
  ],
});

module.exports = { logger, logFormat };
