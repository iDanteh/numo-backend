'use strict';

/**
 * shared/models/postgres/LoyaltyCardAuditLog.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — historial de cambios sobre los datos de catálogo de una
 * tarjeta (teléfono, nombre, activa, tarjetaLegada, koreClienteId, koreMonedero).
 *
 * Gap real cerrado (2026-11-04, pedido explícito del usuario): antes de esto,
 * `LoyaltyCard.updatedById`/`updatedByNombre` solo guardaban quién hizo la
 * ÚLTIMA edición, pisando el rastro anterior — un cambio de teléfono (por
 * ejemplo, vía el webhook de catálogo de Kore) no dejaba ningún registro del
 * valor viejo ni de ediciones previas. `cambiarNip`/`subirFoto`/`marcarRevisada`
 * siguen usando SOLO quién/cuándo a propósito (decisión explícita del usuario,
 * 2026-10-19: "agrega solo el quién y cuándo", ver loyalty-card.service.js) —
 * no se tocan acá. Igual que `LoyaltyTierHistory` (nivel), que ya tenía su
 * propio historial dedicado y sigue así, sin duplicarse en esta tabla.
 *
 * Una fila por ACCIÓN que cambió al menos un campo (no una fila por campo,
 * mismo criterio que `PolizaAuditLog`): `cambios` es un JSONB con solo los
 * campos que realmente cambiaron en esa acción, `{ campo: { antes, despues } }`.
 * Una acción que no cambia nada (ej. Kore reenvía el mismo valor) no genera
 * fila — ver `loyalty-card-audit.service.js#registrarCambioTarjeta`.
 *
 * `onDelete: 'RESTRICT'`: el historial nunca se borra en cascada, mismo
 * criterio que el resto de las tablas de auditoría de Lealtad.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyCardAuditLog = sequelize.define('LoyaltyCardAuditLog', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  cardId: {
    // UUID — coincide con el tipo de loyalty_cards.id, ver LoyaltyCard.js.
    type:       DataTypes.UUID,
    allowNull:  false,
    references: { model: 'loyalty_cards', key: 'id' },
    onDelete:   'RESTRICT',
  },
  accion: {
    // 'edicion_staff' | 'webhook_kore_actualizacion' | 'webhook_kore_baja' | 'match_kore'
    type:      DataTypes.STRING(30),
    allowNull: false,
  },
  origen: {
    // 'numo' (staff, vía panel) | 'kore' (webhook o job de match) — mismo criterio que
    // `origen` ya usado en LoyaltyPointsLedger.
    type:      DataTypes.STRING(20),
    allowNull: false,
  },
  usuarioId: {
    // Null cuando `origen` es 'kore' sin un actor humano real (igual criterio que el resto
    // de columnas *Id de actor en Lealtad: el webhook usa un literal fijo en `loyalty-card-
    // audit.service.js`, no un usuario real de Auth0).
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  usuarioNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  cambios: {
    // { telefono: { antes: '5551234567', despues: '5559876543' }, nombre: { antes, despues }, ... }
    // Nunca incluye nipHash/recuperacionCodigoHash ni ningún otro campo sensible — solo los
    // campos de catálogo que `registrarCambioTarjeta` recibe explícitamente para auditar.
    type:      DataTypes.JSONB,
    allowNull: false,
  },
  fecha: {
    type:         DataTypes.DATE,
    allowNull:    false,
    defaultValue: DataTypes.NOW,
  },
}, {
  tableName:   'loyalty_card_audit_logs',
  underscored: true,
  timestamps:  false,
  indexes: [
    { name: 'loyalty_card_audit_logs_card_id', fields: ['card_id'] },
    { name: 'loyalty_card_audit_logs_fecha',   fields: ['fecha'] },
  ],
});

module.exports = LoyaltyCardAuditLog;
