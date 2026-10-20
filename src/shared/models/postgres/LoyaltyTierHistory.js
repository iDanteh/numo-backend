'use strict';

/**
 * shared/models/postgres/LoyaltyTierHistory.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — historial de cambios de nivel de una tarjeta.
 * `onDelete: 'RESTRICT'`: el historial nunca se borra en cascada.
 * `timestamps:false` + `at` propio, mismo patrón que ConfigAuditLog.fecha.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyTierHistory = sequelize.define('LoyaltyTierHistory', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  cardId: {
    type:       DataTypes.INTEGER,
    allowNull:  false,
    references: { model: 'loyalty_cards', key: 'id' },
    onDelete:   'RESTRICT',
  },
  nivelAnterior: {
    type:      DataTypes.STRING(10),
    allowNull: false,
  },
  nivelNuevo: {
    type:      DataTypes.STRING(10),
    allowNull: false,
  },
  motivo: {
    type:      DataTypes.TEXT,
    allowNull: true,
  },
  usuarioId: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  usuarioNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  at: {
    type:         DataTypes.DATE,
    allowNull:    false,
    defaultValue: DataTypes.NOW,
  },
}, {
  tableName:   'loyalty_tier_history',
  underscored: true,
  timestamps:  false,
});

module.exports = LoyaltyTierHistory;
