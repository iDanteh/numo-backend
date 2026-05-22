'use strict';

/**
 * shared/models/postgres/CentroCosto.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Catálogo de centros de costo.
 * Cada centro agrupa una sucursal con su clave contable y la serie de
 * facturación que le corresponde, permitiendo relacionar cada asiento
 * contable con el centro de costo que lo originó.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const CentroCosto = sequelize.define('CentroCosto', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  clave: {
    type:      DataTypes.STRING(50),
    allowNull: false,
    unique:    true,
    comment:   'Clave única del centro de costo (referenciada desde CfdiMappingRule.centroCosto)',
  },
  sucursal: {
    type:      DataTypes.STRING(150),
    allowNull: false,
    comment:   'Nombre o descripción de la sucursal',
  },
  serieFacturacion: {
    type:      DataTypes.STRING(25),
    allowNull: true,
    comment:   'Serie de facturación asociada a este centro de costo',
  },
  isActive: {
    type:         DataTypes.BOOLEAN,
    defaultValue: true,
  },
}, {
  tableName:   'centros_costo',
  underscored: true,
});

module.exports = CentroCosto;
