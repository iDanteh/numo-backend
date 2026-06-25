'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const ClienteCatalogo = sequelize.define('ClienteCatalogo', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  cuenta: {
    type:      DataTypes.STRING(20),
    allowNull: false,
    comment:   'Código de cuenta contable asociada (ej. 1102010001)',
  },
  nombre: {
    type:      DataTypes.STRING(200),
    allowNull: false,
    comment:   'Razón social o nombre del cliente / proveedor',
  },
  tipo: {
    type:         DataTypes.STRING(30),
    allowNull:    false,
    defaultValue: 'CLIENTE',
    comment:      'CLIENTE | PROVEEDOR | CLIENTE-PROVEEDOR',
  },
  rfc: {
    type:      DataTypes.STRING(13),
    allowNull: false,
    unique:    true,
    comment:   'RFC (identificador fiscal único)',
  },
  isActive: {
    type:         DataTypes.BOOLEAN,
    defaultValue: true,
  },
}, {
  tableName:   'clientes_catalogo',
  underscored: true,
});

module.exports = ClienteCatalogo;
