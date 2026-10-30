'use strict';

/**
 * domains/public/loyalty-public.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — PRIMER endpoint totalmente anónimo de este backend que
 * devuelve datos personales (ver design.md D6-D8 y el Threat Matrix). Autentica
 * con teléfono + NIP de 4 dígitos (nunca teléfono solo). Toda falla — teléfono
 * desconocido, NIP incorrecto, tarjeta inactiva o bloqueada, input malformado —
 * es INDISTINGUIBLE desde afuera: mismo status 422, mismo `code`, mismo
 * mensaje. Nunca revela cuál de los dos campos fue el que falló.
 *
 * Defensa contra fuerza bruta (D7), además de los limiters por IP de la ruta:
 * - Igualación de tiempo: con un teléfono desconocido, igual se corre
 *   `bcrypt.compare` contra `DUMMY_HASH` (nunca asignado a ninguna tarjeta
 *   real), para que la respuesta tarde lo mismo que un NIP incorrecto real.
 * - Lockout por tarjeta: 5 fallos consecutivos → `nipBloqueadoHasta` = ahora +
 *   15 min. Mientras está bloqueada, incluso el NIP correcto falla con el
 *   mismo error genérico — el servicio nunca distingue "bloqueada" de "NIP
 *   incorrecto" en la respuesta.
 *
 * Logging (specs/loyalty-public-lookup "Failed Attempts Logged..."): cada
 * intento fallido se loguea vía `logger.warn` — el teléfono SIEMPRE enmascarado
 * a sus últimos 4 dígitos, el NIP intentado JAMÁS aparece en el log.
 */

const { LoyaltyCard } = require('../../../shared/models/postgres');
const { UnprocessableError } = require('../../../shared/errors/AppError');
const { compararNip, DUMMY_HASH, MAX_INTENTOS_FALLIDOS, LOCKOUT_MS } = require('../../shared/nip.util');
const { logger } = require('../../../shared/utils/logger');

const TELEFONO_RE = /^\d{10}$/;
const NIP_RE = /^\d{4}$/;

class LookupFailedError extends UnprocessableError {
  constructor() {
    super('No se encontró ninguna tarjeta con ese teléfono y NIP.');
    this.code = 'LOYALTY_LOOKUP_FAILED';
  }
}

/** Nunca el teléfono completo en un log — solo los últimos 4 dígitos. */
function _maskTelefono(telefono) {
  const str = String(telefono ?? '');
  return str.length >= 4 ? `***${str.slice(-4)}` : '***';
}

function _logFallo(motivo, telefono, ip) {
  logger.warn(`[loyalty-public] Intento fallido (${motivo}) tel=${_maskTelefono(telefono)} ip=${ip ?? 'desconocida'}`);
}

/**
 * Consulta pública de saldo por teléfono + NIP. Lanza `LookupFailedError`
 * (422, `code: 'LOYALTY_LOOKUP_FAILED'`) en CUALQUIER camino de falla — jamás
 * un error distinto que permita inferir cuál dato fue el incorrecto.
 *
 * @param {string} telefono
 * @param {string} nip
 * @param {{ip?: string}} [ctx]  IP del request, solo para logging (nunca se usa para autenticar).
 * @returns {Promise<{nombre: string, nivel: string, saldoPuntos: string}>}
 */
async function consultaPublica(telefono, nip, { ip } = {}) {
  const telefonoStr = String(telefono ?? '').trim();
  const nipStr = String(nip ?? '').trim();

  if (!TELEFONO_RE.test(telefonoStr) || !NIP_RE.test(nipStr)) {
    _logFallo('formato inválido', telefonoStr, ip);
    throw new LookupFailedError();
  }

  const card = await LoyaltyCard.scope('withNipHash').findOne({ where: { telefono: telefonoStr } });

  if (!card) {
    // Igualación de tiempo (D8): sin esto, "teléfono no existe" respondería
    // notablemente más rápido que "NIP incorrecto" (sin costo de bcrypt),
    // una fuga de timing que permitiría enumerar teléfonos registrados.
    await compararNip(nipStr, DUMMY_HASH);
    _logFallo('teléfono desconocido', telefonoStr, ip);
    throw new LookupFailedError();
  }

  const bloqueada = card.nipBloqueadoHasta != null && new Date(card.nipBloqueadoHasta) > new Date();
  if (!card.activa || bloqueada) {
    // Igual se compara contra el hash real (mismo costo que el camino feliz)
    // para no introducir una fuga de timing entre "bloqueada/inactiva" y
    // "activa con NIP incorrecto" — el resultado de esta comparación se
    // descarta a propósito, el motivo de rechazo ya quedó decidido arriba.
    await compararNip(nipStr, card.nipHash);
    _logFallo(card.activa ? 'tarjeta bloqueada' : 'tarjeta inactiva', telefonoStr, ip);
    throw new LookupFailedError();
  }

  const nipOk = await compararNip(nipStr, card.nipHash);
  if (!nipOk) {
    const intentos = card.nipIntentosFallidos + 1;
    const cambios = { nipIntentosFallidos: intentos };
    if (intentos >= MAX_INTENTOS_FALLIDOS) {
      cambios.nipBloqueadoHasta = new Date(Date.now() + LOCKOUT_MS);
    }
    await card.update(cambios);
    _logFallo('NIP incorrecto', telefonoStr, ip);
    throw new LookupFailedError();
  }

  // Éxito: resetea el contador de fallos (si hacía falta) — un NIP correcto
  // siempre limpia cualquier racha de fallos previa, igual que un login exitoso.
  if (card.nipIntentosFallidos !== 0 || card.nipBloqueadoHasta !== null) {
    await card.update({ nipIntentosFallidos: 0, nipBloqueadoHasta: null });
  }

  // Nombre completo (2026-10-29, pedido explícito del usuario) — antes solo el primer nombre
  // (`primerNombre`); `card.nombre` ya es el combinado "Nombre Apellidos", no hace falta concatenar.
  return { nombre: card.nombre, nivel: card.nivel, saldoPuntos: card.saldoPuntos };
}

module.exports = { consultaPublica, LookupFailedError };
