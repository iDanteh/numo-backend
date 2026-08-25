'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

/**
 * ConfigAuditLog.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Historial completo de cambios sobre un `GlobalConfig` — quién, cuándo, qué
 * acción. IMPORTANTE (confirmado con el usuario): si el `GlobalConfig` es
 * `esSecreto=true`, `valorAnterior`/`valorNuevo` quedan SIEMPRE null — el log
 * registra QUE hubo un cambio, nunca el valor real, para no crear una segunda
 * puerta trasera al secreto. Esto lo aplica quien escribe (global-config.service.js),
 * no una constraint de esta tabla.
 */
const ConfigAuditLog = sequelize.define('ConfigAuditLog', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  configId: {
    type:       DataTypes.INTEGER,
    allowNull:  false,
    references: { model: 'global_configs', key: 'id' },
  },
  usuarioId: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  usuarioNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  accion: {
    type:      DataTypes.STRING(30), // 'creado' | 'editado' | 'secreto_revelado'
    allowNull: false,
  },
  valorAnterior: {
    type:      DataTypes.TEXT,
    allowNull: true,
  },
  valorNuevo: {
    type:      DataTypes.TEXT,
    allowNull: true,
  },
  fecha: {
    type:         DataTypes.DATE,
    allowNull:    false,
    defaultValue: DataTypes.NOW,
  },
}, {
  tableName:   'config_audit_logs',
  underscored: true,
  timestamps:  false,
  indexes: [
    { name: 'config_audit_logs_config_id', fields: ['config_id'] },
  ],
});

module.exports = ConfigAuditLog;
