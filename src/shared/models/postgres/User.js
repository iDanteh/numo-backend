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
  // Empresas fijas asignadas directo a ESTE usuario (RFCs) — se asignan desde
  // la pantalla de Roles (selección múltiple de usuarios + empresa). Array
  // vacío = sin restricción, puede elegir cualquier empresa. Un usuario puede
  // tener varias (confirmado con el usuario 2026-07-28).
  empresaRfcs: {
    type:         DataTypes.ARRAY(DataTypes.STRING(20)),
    allowNull:    false,
    defaultValue: [],
  },
}, {
  tableName:  'users',
  underscored: true,
});

module.exports = User;
