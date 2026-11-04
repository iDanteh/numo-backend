'use strict';

/**
 * domains/cards/loyalty-card-audit.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — historial de cambios de catálogo de una tarjeta
 * (teléfono, nombre, activa, tarjetaLegada, koreClienteId, koreMonedero).
 * Único punto de escritura de `LoyaltyCardAuditLog` — mismo patrón que
 * `_aplicarMovimiento` para el ledger de puntos: todo el que quiera auditar un
 * cambio de tarjeta pasa por acá, nunca inserta en el modelo directamente.
 *
 * `registrarCambioTarjeta` recibe explícitamente `antes`/`despues` (nunca lee
 * la instancia de `LoyaltyCard` directamente) a propósito: en Sequelize,
 * `card.update(...)` muta la instancia en memoria apenas se resuelve, así que
 * si esta función leyera `card[campo]` DESPUÉS del update ya vería el valor
 * nuevo de los dos lados — el caller tiene que capturar `antes` ANTES de
 * llamar a `.update()`. Esto evita ese bug de orden silenciosamente.
 *
 * Alcance deliberado (gap real cerrado 2026-11-04): teléfono, nombre, activa,
 * tarjetaLegada, koreClienteId, koreMonedero — los campos de identidad/catálogo
 * que hoy se pueden editar después de creada la tarjeta y que NO tenían ningún
 * historial. Fuera de alcance a propósito: `nivel` (ya tiene su propio
 * `LoyaltyTierHistory`, no se duplica acá), `nipHash` (decisión explícita del
 * usuario 2026-10-19 de NO loguear valores de un secreto, solo quién/cuándo —
 * ver `cambiarNip`), `fotoUrl`/`revisionPendiente` (ya tienen sus propias
 * columnas de quién/cuándo en `LoyaltyCard`, menor criticidad que identidad).
 */

const { LoyaltyCardAuditLog } = require('../../../shared/models/postgres');

/**
 * Compara `antes` contra `despues` campo por campo y, si hay al menos una
 * diferencia real, inserta UNA fila con todos los campos que cambiaron en esa
 * acción. Si nada cambió (ej. Kore reenvía el mismo valor que ya había), no
 * inserta nada — devuelve `null`.
 */
async function registrarCambioTarjeta({
  cardId, antes, despues, accion, origen, usuarioId = null, usuarioNombre = null, transaction,
} = {}) {
  const cambios = {};
  for (const campo of Object.keys(despues ?? {})) {
    const valorAnterior = antes?.[campo] ?? null;
    const valorNuevo = despues[campo] ?? null;
    if (valorAnterior !== valorNuevo) {
      cambios[campo] = { antes: valorAnterior, despues: valorNuevo };
    }
  }

  if (!Object.keys(cambios).length) return null;

  const datos = {
    cardId, accion, origen, usuarioId, usuarioNombre, cambios,
  };
  return transaction
    ? LoyaltyCardAuditLog.create(datos, { transaction })
    : LoyaltyCardAuditLog.create(datos);
}

/** Historial de cambios de catálogo de una tarjeta, más reciente primero. */
async function listarHistorialCambios(cardId) {
  return LoyaltyCardAuditLog.findAll({ where: { cardId }, order: [['fecha', 'DESC']] });
}

module.exports = { registrarCambioTarjeta, listarHistorialCambios };
