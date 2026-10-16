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
  participaLealtad: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: true,
    comment:      'Sistema de Lealtad: si esta sucursal acumula puntos en sus ventas (independiente de permiteCanjeLealtad)',
  },
  permiteCanjeLealtad: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: true,
    comment:      'Sistema de Lealtad: si esta sucursal permite canjear puntos (independiente de participaLealtad)',
  },
}, {
  tableName:   'centros_costo',
  underscored: true,
});

module.exports = CentroCosto;
