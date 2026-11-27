'use strict';

/**
 * domains/security/loyalty-security-event.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — lectura de `LoyaltySecurityEvent` (auditoría append-only
 * de los flujos OTP: consulta pública/recuperación de NIP y canje de puntos
 * con Kore). Hasta 2026-11-13 esta tabla solo se ESCRIBÍA (`_registrarEvento`
 * en `loyalty-recovery.service.js`/`loyalty-kore-canje.service.js`) — nadie
 * podía consultarla. Pedido explícito del usuario: poder auditar quién pidió
 * o validó un código, cuándo, y con qué resultado — SIN el código en sí, que
 * nunca se persiste (ni siquiera hasheado) a propósito, ver
 * `LoyaltySecurityEvent.js`. Guardar el código para "verlo" en auditoría
 * convertiría esta tabla en un inventario de credenciales válidas — se evaluó
 * y se descartó, el usuario confirmó que esto (quién/cuándo/resultado) alcanza.
 *
 * `tipo` distingue el flujo de origen (ver `LoyaltySecurityEvent.js` para la
 * lista completa): `solicitud_codigo`/`confirmacion_codigo`/`ver_consulta`/
 * `guardar_email` (consulta pública) vs. `solicitud_codigo_canje`/
 * `confirmacion_codigo_canje` (canje con Kore).
 */

const { Op } = require('sequelize');
const { LoyaltySecurityEvent } = require('../../../shared/models/postgres');
const { BadRequestError } = require('../../../shared/errors/AppError');

const LIMITE_DEFAULT = 100;
const LIMITE_MAX = 500;

function _parseFecha(valor, nombreCampo) {
  if (!valor) return null;
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) {
    throw new BadRequestError(`${nombreCampo} no es una fecha válida (usar ISO 8601).`);
  }
  return fecha;
}

/**
 * Lista eventos de auditoría, más recientes primero. Todos los filtros son opcionales — sin
 * ninguno, devuelve los últimos `limit` eventos de cualquier teléfono/tipo.
 *
 * @param {{telefono?: string, tipo?: string, resultado?: 'exito'|'fallo', desde?: string,
 *          hasta?: string, limit?: number, offset?: number}} filtros
 */
async function listarEventosSeguridad({
  telefono, tipo, resultado, desde, hasta, limit, offset,
} = {}) {
  if (resultado && !['exito', 'fallo'].includes(resultado)) {
    throw new BadRequestError('resultado debe ser "exito" o "fallo".');
  }

  const desdeFecha = _parseFecha(desde, 'desde');
  const hastaFecha = _parseFecha(hasta, 'hasta');

  const where = {};
  if (telefono) where.telefono = String(telefono).trim();
  if (tipo) where.tipo = tipo;
  if (resultado) where.resultado = resultado;
  if (desdeFecha || hastaFecha) {
    where.createdAt = {};
    if (desdeFecha) where.createdAt[Op.gte] = desdeFecha;
    if (hastaFecha) where.createdAt[Op.lte] = hastaFecha;
  }

  const limitFinal = Number(limit) > 0 ? Math.min(Number(limit), LIMITE_MAX) : LIMITE_DEFAULT;
  const offsetFinal = Number(offset) > 0 ? Number(offset) : 0;

  const { rows, count } = await LoyaltySecurityEvent.findAndCountAll({
    where,
    order: [['createdAt', 'DESC']],
    limit: limitFinal,
    offset: offsetFinal,
  });

  return { total: count, eventos: rows };
}

module.exports = { listarEventosSeguridad };
