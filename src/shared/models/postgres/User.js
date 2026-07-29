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
  // Permisos extra asignados directo a ESTE usuario, además de los que ya le
  // da su rol. Puramente ADITIVO: nunca revoca lo que el rol concede, solo
  // amplía (unión con rbacStore.getPermissions(role) al calcular permisos
  // efectivos — ver rbac-store.js). ARRAY(TEXT), no STRING(20) como empresaRfcs:
  // las claves de permiso son 'modulo:accion' y pueden superar 20 caracteres
  // (ej. 'banks:movement:categoria' = 24) — TEXT no impone límite. Debe
  // coincidir EXACTO con el tipo real de la columna (TEXT[], ver el ALTER
  // TABLE en index.js): User.sync({alter:true}) corre en no-prod y reconcilia
  // el esquema contra este modelo — si acá dijera STRING (VARCHAR(255) por
  // defecto), podría intentar angostar la columna de vuelta sin que nadie lo
  // pidiera. Mismo criterio que empresaRfcs (STRING(20) ↔ VARCHAR(20)[] exacto).
  extraPermissions: {
    type:         DataTypes.ARRAY(DataTypes.TEXT),
    allowNull:    false,
    defaultValue: [],
  },
}, {
  tableName:  'users',
  underscored: true,
});

module.exports = User;
