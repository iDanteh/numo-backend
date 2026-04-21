'use strict';

/**
 * shared/models/postgres/PeriodoFiscal.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Períodos fiscales (año + mes opcional).
 *
 * Migrado de MongoDB a PostgreSQL:
 *   • La restricción UNIQUE (ejercicio, periodo) es nativa en SQL.
 *   • La FK a users garantiza integridad referencial del campo createdBy.
 *   • periodo NULL = ejercicio anual completo (permitido por la restricción única
 *     porque en Postgres dos NULL no se consideran iguales en UNIQUE).
 *
 * NOTA: Las agregaciones sobre Comparison, Discrepancy y CFDI siguen siendo
 *       MongoDB pipelines; el controller hace un join en la capa de aplicación.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const PeriodoFiscal = sequelize.define('PeriodoFiscal', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  ejercicio: {
    type:      DataTypes.INTEGER,
    allowNull: false,
  },
  /** Mes 1–12. NULL = ejercicio anual completo */
  periodo: {
    type:      DataTypes.INTEGER,
    allowNull: true,
    validate:  { min: 1, max: 12 },
  },
  label: {
    type:      DataTypes.STRING(100),
    allowNull: true,
  },
  /** FK al usuario que creó el registro */
  createdBy: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'users', key: 'id' },
    onDelete:   'SET NULL',
  },
}, {
  tableName:   'periodos_fiscales',
  underscored: true,
  indexes: [
    /**
     * Postgres permite múltiples NULL en columnas UNIQUE normales.
     * La restricción aquí replica el índice único del schema Mongoose:
     *   { ejercicio: 1, periodo: 1 }, unique: true
     */
    {
      unique: true,
      fields: ['ejercicio', 'periodo'],
      name:   'uq_periodo_fiscal_ejercicio_periodo',
    },
  ],
});

module.exports = PeriodoFiscal;
