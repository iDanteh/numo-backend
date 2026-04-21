'use strict';

/**
 * shared/models/postgres/Entity.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Entidades fiscales (empresas / personas físicas con actividad empresarial).
 *
 * Migrado de MongoDB a PostgreSQL.
 * Campos complejos (domicilioFiscal, fiel, syncConfig) se almacenan como JSONB,
 * preservando la flexibilidad documental donde se necesita y beneficiando del
 * índice GIN de Postgres para consultas sobre campos anidados.
 *
 * IMPORTANTE: el campo `fiel` contiene rutas a certificados y contraseña cifrada.
 * Nunca incluirlo en respuestas API sin filtrar explícitamente.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const Entity = sequelize.define('Entity', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  rfc: {
    type:      DataTypes.STRING(20),
    allowNull: false,
    unique:    true,
    set(value) {
      this.setDataValue('rfc', value?.toUpperCase().trim() ?? value);
    },
  },
  nombre: {
    type:      DataTypes.STRING(255),
    allowNull: false,
  },
  regimenFiscal: {
    type:      DataTypes.STRING(100),
    allowNull: true,
  },
  /** Dirección fiscal — estructura libre, suficientemente estable para JSONB */
  domicilioFiscal: {
    type:         DataTypes.JSONB,
    defaultValue: {},
  },
  tipo: {
    type:      DataTypes.ENUM('moral', 'fisica'),
    allowNull: false,
  },
  /** true = esta entidad es la empresa propietaria del sistema */
  isOwn: {
    type:         DataTypes.BOOLEAN,
    defaultValue: false,
  },
  /** Credenciales e.firma — nunca retornar al cliente sin filtrar */
  fiel: {
    type:         DataTypes.JSONB,
    defaultValue: {},
  },
  /** Configuración de sincronización con SAT */
  syncConfig: {
    type: DataTypes.JSONB,
    defaultValue: {
      autoSync:      false,
      syncFrequency: 'daily',
      lastSync:      null,
      nextSync:      null,
      syncEmitidos:  true,
      syncRecibidos: true,
    },
  },
  isActive: {
    type:         DataTypes.BOOLEAN,
    defaultValue: true,
  },
  notes: {
    type:      DataTypes.TEXT,
    allowNull: true,
  },
}, {
  tableName:   'entities',
  underscored: true,
  indexes: [
    { fields: ['is_active'] },
    { fields: ['tipo'] },
  ],
});

module.exports = Entity;
