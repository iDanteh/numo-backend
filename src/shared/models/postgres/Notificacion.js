'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

/**
 * Notificacion.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Bandeja de notificaciones global (visible en toda la app, no por usuario) —
 * primer tipo: `cfdi_cancelado_con_poliza`, generado por
 * cfdi-cancelado-notificacion.job.js cuando un CFDI que ya tiene una póliza
 * ACTIVA (estado != 'cancelada') aparece como Cancelado en el SAT. Diseñado
 * genérico (`tipo`/`titulo`/`mensaje`) para que futuros tipos de alerta
 * reutilicen la misma tabla/bandeja sin cambiar el modelo.
 *
 * `leida` es un solo booleano compartido (no por usuario) — simplificación
 * inicial mientras no haga falta rastrear lectura individual por usuario.
 *
 * `leida` != `resuelta` (confirmado con el usuario 2026-08-13): ver/hacer
 * clic en la notificación solo la marca `leida` (deja de sumar al badge,
 * pero SIGUE en la bandeja) — el problema real (CFDI cancelado sin
 * sustituto, etc.) puede seguir sin resolverse. Solo desaparece de la
 * bandeja cuando alguien la marca `resuelta` explícitamente.
 */
const Notificacion = sequelize.define('Notificacion', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  tipo: {
    type:      DataTypes.STRING(50),
    allowNull: false,
  },
  titulo: {
    type:      DataTypes.STRING(255),
    allowNull: false,
  },
  mensaje: {
    type:      DataTypes.TEXT,
    allowNull: true,
  },
  polizaId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'polizas', key: 'id' },
  },
  cfdiUuid: {
    type:      DataTypes.STRING(36),
    allowNull: true,
  },
  leida: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: false,
  },
  leidaPor: {
    type:      DataTypes.STRING(150),
    allowNull: true,
  },
  leidaAt: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
  resuelta: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: false,
  },
  resueltaPor: {
    type:      DataTypes.STRING(150),
    allowNull: true,
  },
  resueltaAt: {
    type:      DataTypes.DATE,
    allowNull: true,
  },
}, {
  tableName:   'notificaciones',
  underscored: true,
  indexes: [
    // Idempotencia: el job no debe duplicar la misma alerta de un mismo CFDI
    // en una misma póliza si ya corrió antes y sigue sin resolverse.
    {
      name:   'notificaciones_tipo_poliza_cfdi',
      fields: ['tipo', 'poliza_id', 'cfdi_uuid'],
      unique: true,
    },
    { name: 'notificaciones_leida', fields: ['leida'] },
    { name: 'notificaciones_resuelta', fields: ['resuelta'] },
  ],
});

module.exports = Notificacion;
