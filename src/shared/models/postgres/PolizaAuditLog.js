'use strict';

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

/**
 * PolizaAuditLog.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Bitácora de acciones sobre pólizas (2026-10-15, pedido del usuario: monitor
 * solo-admin de "quién crea, quién cancela, uso por usuario"). A diferencia de
 * las columnas *Por de `polizas` (solo guardan el ÚLTIMO actor), aquí queda una
 * fila por cada acción — incluidas exportaciones y ediciones, que antes no se
 * registraban en ningún lado.
 *
 * Se copian tipo/número/fecha/concepto de la póliza al momento de la acción
 * (sin FK a `polizas`): la fila sigue siendo legible aunque la póliza cambie.
 *
 * origen: 'app' = registrado en vivo; 'historico' = reconstruido una sola vez
 * desde las columnas *Por existentes al crear la tabla (ver syncModels).
 */
const PolizaAuditLog = sequelize.define('PolizaAuditLog', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  polizaId: {
    type:      DataTypes.INTEGER,
    allowNull: true, // null en exportaciones de periodo completo (XML SAT)
  },
  accion: {
    // 'creada' | 'editada' | 'contabilizada' | 'cancelada' | 'revertida' |
    // 'exportada' | 'exportada_xml_sat' | 'folio_contpaq' | 'cuenta_reemplazada'
    type:      DataTypes.STRING(30),
    allowNull: false,
  },
  usuarioId: {
    type:      DataTypes.STRING(50),
    allowNull: true,
  },
  usuarioNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  usuarioRol: {
    type:      DataTypes.STRING(50),
    allowNull: true,
  },
  rfc: {
    type:      DataTypes.STRING(13),
    allowNull: true,
  },
  polizaTipo: {
    type:      DataTypes.STRING(1),
    allowNull: true,
  },
  polizaNumero: {
    type:      DataTypes.INTEGER,
    allowNull: true,
  },
  polizaFecha: {
    type:      DataTypes.DATEONLY,
    allowNull: true,
  },
  polizaConcepto: {
    type:      DataTypes.STRING(500),
    allowNull: true,
  },
  ejercicio: {
    type:      DataTypes.INTEGER,
    allowNull: true,
  },
  periodo: {
    type:      DataTypes.INTEGER,
    allowNull: true,
  },
  estadoAnterior: {
    type:      DataTypes.STRING(20),
    allowNull: true,
  },
  estadoNuevo: {
    type:      DataTypes.STRING(20),
    allowNull: true,
  },
  motivo: {
    type:      DataTypes.TEXT,
    allowNull: true,
  },
  detalle: {
    type:      DataTypes.JSONB,
    allowNull: true,
  },
  origen: {
    type:         DataTypes.STRING(20),
    allowNull:    false,
    defaultValue: 'app',
  },
  fecha: {
    type:         DataTypes.DATE,
    allowNull:    false,
    defaultValue: DataTypes.NOW,
  },
}, {
  tableName:   'poliza_audit_logs',
  underscored: true,
  timestamps:  false,
  indexes: [
    { name: 'poliza_audit_logs_fecha',     fields: ['fecha'] },
    { name: 'poliza_audit_logs_poliza_id', fields: ['poliza_id'] },
    { name: 'poliza_audit_logs_usuario',   fields: ['usuario_nombre'] },
    { name: 'poliza_audit_logs_accion',    fields: ['accion', 'fecha'] },
  ],
});

module.exports = PolizaAuditLog;
