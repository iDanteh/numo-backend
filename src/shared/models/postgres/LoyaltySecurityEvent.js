'use strict';

/**
 * shared/models/postgres/LoyaltySecurityEvent.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — auditoría append-only de intentos sobre el flujo OTP de
 * la ruta pública de consulta de puntos (ver `loyalty-recovery.service.js`).
 * Nunca se actualiza ni se borra desde la app. Complementa — no reemplaza — el
 * log de Winston: acá se puede investigar un teléfono o una IP a través del
 * tiempo (patrones de abuso), algo que el log rotativo no permite. Nunca
 * persiste el código ni el NIP, ni siquiera hasheado — no hace falta para
 * auditoría y sería superficie de ataque de más.
 */
const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltySecurityEvent = sequelize.define('LoyaltySecurityEvent', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  telefono: {
    type:      DataTypes.STRING(10),
    allowNull: true,
  },
  tipo: {
    type:      DataTypes.STRING(30), // 'solicitud_codigo' | 'confirmacion_codigo'
    allowNull: false,
  },
  resultado: {
    type:      DataTypes.STRING(10), // 'exito' | 'fallo'
    allowNull: false,
  },
  motivo: {
    type:      DataTypes.STRING(50),
    allowNull: true,
    comment:   'Null cuando resultado=exito. En fallo: formato invalido | telefono desconocido | recuperacion bloqueada | sin codigo vigente | codigo incorrecto | tarjeta inactiva.',
  },
  ip: {
    type:      DataTypes.STRING(45), // cabe IPv6
    allowNull: true,
  },
  createdAt: {
    type:         DataTypes.DATE,
    allowNull:    false,
    defaultValue: DataTypes.NOW,
  },
}, {
  tableName:   'loyalty_security_events',
  underscored: true,
  timestamps:  false,
  indexes: [
    { name: 'loyalty_security_events_telefono', fields: ['telefono'] },
    { name: 'loyalty_security_events_ip', fields: ['ip'] },
  ],
});

module.exports = LoyaltySecurityEvent;
