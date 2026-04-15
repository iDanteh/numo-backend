/**
 * Expresión regular oficial del SAT para validar RFC de personas físicas y morales.
 * Personas morales:  3 letras + 6 dígitos (fecha) + 3 alfanuméricos = 12 chars
 * Personas físicas:  4 letras + 6 dígitos (fecha) + 3 alfanuméricos = 13 chars
 */
const RFC_REGEX = /^[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}$/i;

/**
 * Fuentes de CFDI válidas aceptadas por la plataforma.
 */
const CFDI_SOURCES = new Set(['ERP', 'SAT', 'MANUAL', 'RECEPTOR']);

/**
 * Normaliza un valor de source a mayúsculas.
 * Si no es válido devuelve el valor por defecto.
 * @param {string} raw
 * @param {string} [defaultSource='ERP']
 * @returns {string}
 */
const normalizeSource = (raw = '', defaultSource = 'ERP') => {
  const upper = raw.toUpperCase();
  return CFDI_SOURCES.has(upper) ? upper : defaultSource;
};

module.exports = { RFC_REGEX, CFDI_SOURCES, normalizeSource };
