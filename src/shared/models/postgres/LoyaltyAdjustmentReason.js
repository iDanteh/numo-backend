'use strict';

/**
 * shared/models/postgres/LoyaltyAdjustmentReason.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — catálogo de motivos de ajuste manual de puntos.
 * `signoPermitido` usa palabras en vez de +/-/± (una trampa de encoding real
 * con el signo menos tipográfico). Seeds (ver scripts/seed-lealtad.js, PR3):
 * 'bono' POSITIVO, 'regalo' POSITIVO, 'correccion' AMBOS.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyAdjustmentReason = sequelize.define('LoyaltyAdjustmentReason', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  clave: {
    type:      DataTypes.STRING(50),
    allowNull: false,
    unique:    true,
  },
  nombre: {
    type:      DataTypes.STRING(150),
    allowNull: false,
  },
  signoPermitido: {
    type:      DataTypes.STRING(10),
    allowNull: false,
    validate:  { isIn: [['POSITIVO', 'NEGATIVO', 'AMBOS']] },
  },
  activo: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: true,
  },
}, {
  tableName:   'loyalty_adjustment_reasons',
  underscored: true,
});

module.exports = LoyaltyAdjustmentReason;
