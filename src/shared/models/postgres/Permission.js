'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

/**
 * Permission — catálogo de permisos disponibles en el sistema.
 * La clave (PK) tiene el formato "módulo:acción" (ej: banks:read).
 */
const Permission = sequelize.define('Permission', {
  key:    { type: DataTypes.STRING(100), primaryKey: true },
  label:  { type: DataTypes.STRING(200), allowNull: false },
  module: { type: DataTypes.STRING(50),  allowNull: false, defaultValue: 'General' },
}, {
  tableName:  'permissions',
  timestamps: false,
});

module.exports = Permission;
