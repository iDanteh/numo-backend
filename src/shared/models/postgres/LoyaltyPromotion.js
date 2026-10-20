'use strict';

/**
 * shared/models/postgres/LoyaltyPromotion.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — catálogo de promociones de bono extra sobre la
 * acumulación normal, vigentes por un rango de fechas (ej. "Buen Fin"). El %
 * de la promoción vigente se SUMA al % del nivel de la tarjeta al acumular
 * (ver loyalty-ledger.service.js#registrarAcumulacion).
 *
 * `activa:false` NUNCA borra la fila (mismo criterio que
 * LoyaltyAdjustmentReason#activo) — por auditoría/histórico, ya que
 * `LoyaltyPointsLedger` guarda un snapshot de la promoción aplicada
 * (promocionNombre/promocionPorcentaje) en cada acumulación.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyPromotion = sequelize.define('LoyaltyPromotion', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  nombre: {
    type:      DataTypes.STRING(150),
    allowNull: false,
  },
  fechaInicio: {
    type:      DataTypes.DATEONLY,
    allowNull: false,
  },
  fechaFin: {
    type:      DataTypes.DATEONLY,
    allowNull: false,
  },
  porcentajeBono: {
    type:      DataTypes.DECIMAL(5, 2),
    allowNull: false,
  },
  activa: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: true,
  },
}, {
  tableName:   'loyalty_promotions',
  underscored: true,
});

module.exports = LoyaltyPromotion;
