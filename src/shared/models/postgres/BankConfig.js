'use strict';

/**
 * shared/models/postgres/BankConfig.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Configuración contable por banco (cuenta contable y número de cuenta).
 * Un registro por banco — se usa upsert para mantener idempotencia.
 *
 * Migrado de MongoDB a PostgreSQL: los datos son puramente relacionales,
 * con esquema fijo y bajo volumen de escritura.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const BankConfig = sequelize.define('BankConfig', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  banco: {
    type:      DataTypes.STRING(50),
    allowNull: false,
    unique:    true,
  },
  cuentaContable: {
    type:      DataTypes.STRING(100),
    allowNull: true,
  },
  numeroCuenta: {
    type:      DataTypes.STRING(100),
    allowNull: true,
  },
}, {
  tableName:   'bank_configs',
  underscored: true,
});

module.exports = BankConfig;
