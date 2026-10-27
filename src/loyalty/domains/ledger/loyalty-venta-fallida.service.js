'use strict';

/**
 * domains/ledger/loyalty-venta-fallida.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — "procesos fallidos" del webhook Kore→Numo de venta (ver
 * loyalty-kore-venta.service.js). Visibilidad y reproceso MANUAL, no un
 * reintento automático en segundo plano (decisión confirmada con el usuario,
 * 2026-10-26, ver Docs/integracion-kore-puntos-lealtad-DRAFT.md §6): el
 * reintento real es que Kore reenvíe el mismo POST con el mismo `serieFolio`
 * (idempotente) una vez que el equipo de Numo corrigió la causa raíz.
 *
 * Un solo registro "vivo" (resuelto:false) por `serieFolio` — si el mismo
 * serieFolio vuelve a fallar antes de resolverse, se incrementa `intentos` en
 * la misma fila en vez de crear una nueva. Un `serieFolio` que ya se resolvió
 * y vuelve a fallar después SÍ genera una fila nueva (el historial de la
 * atención anterior no se pisa).
 */

const { LoyaltyKoreVentaFallida } = require('../../../shared/models/postgres');
const { NotFoundError } = require('../../../shared/errors/AppError');
const { logger } = require('../../../shared/utils/logger');

function _userLabel(user) {
  return {
    usuarioId:     user?.dbId != null ? String(user.dbId) : (user?._id != null ? String(user._id) : null),
    usuarioNombre: user?.nombre ?? null,
  };
}

/**
 * Registra (o actualiza) un proceso fallido. Nunca lanza — un fallo al
 * guardar este registro no debe tapar el error real que ya se le va a
 * responder a Kore (mismo criterio que `_registrarEvento` en
 * loyalty-recovery.service.js).
 */
async function registrarVentaFallida(serieFolio, payload, motivo) {
  try {
    const existente = await LoyaltyKoreVentaFallida.findOne({ where: { serieFolio, resuelto: false } });
    if (existente) {
      await existente.update({
        motivo,
        payloadRecibido: JSON.stringify(payload ?? {}),
        intentos: existente.intentos + 1,
      });
      return existente;
    }
    return await LoyaltyKoreVentaFallida.create({
      serieFolio,
      motivo,
      payloadRecibido: JSON.stringify(payload ?? {}),
      intentos: 1,
      resuelto: false,
    });
  } catch (err) {
    logger.error(`[loyalty-venta-fallida] No se pudo registrar el proceso fallido de serieFolio=${serieFolio}: ${err.message}`);
    return null;
  }
}

async function listarVentasFallidas({ soloNoResueltas = false } = {}) {
  const where = soloNoResueltas ? { resuelto: false } : {};
  return LoyaltyKoreVentaFallida.findAll({ where, order: [['createdAt', 'DESC']] });
}

async function marcarVentaFallidaResuelta(id, user) {
  const fila = await LoyaltyKoreVentaFallida.findByPk(id);
  if (!fila) throw new NotFoundError('Proceso fallido');

  const { usuarioId, usuarioNombre } = _userLabel(user);
  await fila.update({
    resuelto: true,
    resueltoPorUserId: usuarioId,
    resueltoPorNombre: usuarioNombre,
    resueltoEn: new Date(),
  });
  return fila;
}

module.exports = { registrarVentaFallida, listarVentasFallidas, marcarVentaFallidaResuelta };
