'use strict';

/**
 * shared/models/postgres/User.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Modelo Sequelize para usuarios Auth0.
 * Reemplaza el Mongoose User que vivía en banks/domains/users/User.model.js.
 *
 * Estrategia de auth:
 *   • auth0_sub es la clave de identidad — llega del JWT de Auth0.
 *   • Un usuario pre-sembrado lleva auth0_sub = 'seed:<email>' como placeholder
 *     hasta que haga su primer login real y user.service lo "reclame".
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const User = sequelize.define('User', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  auth0Sub: {
    type:      DataTypes.STRING(255),
    allowNull: false,
    unique:    true,
  },
  nombre: {
    type:         DataTypes.STRING(255),
    defaultValue: '',
  },
  email: {
    type:         DataTypes.STRING(255),
    defaultValue: '',
  },
  role: {
    type:         DataTypes.STRING(50),
    defaultValue: 'tienda',
  },
  isActive: {
    type:         DataTypes.BOOLEAN,
    defaultValue: true,
  },
  lastLogin: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
}, {
  tableName:  'users',
  underscored: true,
});

module.exports = User;
