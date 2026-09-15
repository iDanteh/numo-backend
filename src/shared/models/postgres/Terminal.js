'use strict';

/**
 * shared/models/postgres/Terminal.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Catálogo de terminales (de cobro). Cada terminal pertenece a una sucursal
 * (centro de costo) y se identifica por su nombre comercial y número de serie.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const Terminal = sequelize.define('Terminal', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  nombreComercial: {
    type:      DataTypes.STRING(150),
    allowNull: false,
    comment:   'Nombre comercial de la terminal',
  },
  numeroSerie: {
    type:      DataTypes.STRING(100),
    allowNull: false,
    unique:    true,
    comment:   'Número de serie único de la terminal',
  },
  centroCostoId: {
    type:      DataTypes.INTEGER,
    allowNull: false,
    comment:   'Sucursal (centro de costo) a la que pertenece la terminal',
  },
  isActive: {
    type:         DataTypes.BOOLEAN,
    defaultValue: true,
  },
}, {
  tableName:   'terminales',
  underscored: true,
});

module.exports = Terminal;
