'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

/**
 * Role — roles del sistema gestionables desde la API.
 *
 * permissions: JSONB — array de claves de permiso o ['*'] para acceso total.
 * isSystem:    true  — los roles sembrados desde rbac.js no se pueden eliminar,
 *                      pero sí se pueden editar sus permisos.
 */
const Role = sequelize.define('Role', {
  value:       { type: DataTypes.STRING(50),  primaryKey: true },
  label:       { type: DataTypes.STRING(100), allowNull: false },
  permissions: { type: DataTypes.JSONB,       allowNull: false, defaultValue: [] },
  isSystem:    { type: DataTypes.BOOLEAN,     allowNull: false, defaultValue: false },
}, {
  tableName:  'roles',
  timestamps: false,
});

module.exports = Role;
