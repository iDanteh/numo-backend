'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

/**
 * GlobalConfig.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Un valor de configuración runtime dentro de una `ConfigSection` — ver
 * global-config.service.js para lectura/escritura (nunca leer/escribir esta
 * tabla directo desde una ruta, siempre a través del service, que gestiona
 * caché + cifrado + auditoría).
 *
 * `valor` (texto plano) y `valorCifrado` (bytea, pgcrypto pgp_sym_encrypt) son
 * mutuamente excluyentes según `esSecreto` — nunca conviven ambos con dato real
 * para la misma fila.
 */
const GlobalConfig = sequelize.define('GlobalConfig', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  sectionId: {
    type:       DataTypes.INTEGER,
    allowNull:  false,
    references: { model: 'config_sections', key: 'id' },
  },
  clave: {
    type:      DataTypes.STRING(100),
    allowNull: false,
  },
  valor: {
    type:      DataTypes.TEXT,
    allowNull: true,
  },
  valorCifrado: {
    type:      DataTypes.BLOB,
    allowNull: true,
  },
  esSecreto: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: false,
  },
  tipo: {
    type:         DataTypes.STRING(20), // 'url' | 'ruta' | 'texto' | 'numero' | 'booleano'
    allowNull:    false,
    defaultValue: 'texto',
  },
  descripcion: {
    type:      DataTypes.TEXT,
    allowNull: true,
  },
  updatedBy: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
}, {
  tableName:   'global_configs',
  underscored: true,
  indexes: [
    {
      name:   'global_configs_section_clave',
      fields: ['section_id', 'clave'],
      unique: true,
    },
  ],
});

module.exports = GlobalConfig;
