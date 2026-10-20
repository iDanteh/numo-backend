'use strict';

/**
 * shared/models/postgres/LoyaltyCard.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — tarjeta de puntos identificada por teléfono (D1: el
 * teléfono es la única llave de identidad; la UI la sigue llamando "tarjeta").
 * Ver openspec/changes/sistema-lealtad/design.md "Data Model".
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyCard = sequelize.define('LoyaltyCard', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  telefono: {
    type:      DataTypes.STRING(10),
    allowNull: false,
    unique:    true,
    validate:  { is: /^\d{10}$/ },
    comment:   'Única llave de identidad de la tarjeta (D1). 10 dígitos, sin lada ni símbolos.',
  },
  nipHash: {
    type:      DataTypes.STRING(100),
    allowNull: false,
    comment:   'Hash bcrypt del NIP (D6). El texto plano nunca se persiste.',
  },
  nombre: {
    type:      DataTypes.STRING(200),
    allowNull: false,
    comment:   'Nombre del cliente dueño de la tarjeta.',
  },
  nivel: {
    type:         DataTypes.STRING(10),
    allowNull:    false,
    defaultValue: 'Bronce',
    validate:     { isIn: [['Bronce', 'Plata', 'Oro']] },
    comment:      'Nivel actual de la tarjeta (D2: STRING + isIn, no ENUM — ver rationale en design.md).',
  },
  saldoPuntos: {
    type:         DataTypes.DECIMAL(12, 2),
    allowNull:    false,
    defaultValue: 0,
    comment:      'Saldo denormalizado de puntos. Respaldado por CHECK saldo_puntos >= 0 (D4).',
  },
  activa: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: true,
  },
  nipIntentosFallidos: {
    type:         DataTypes.INTEGER,
    allowNull:    false,
    defaultValue: 0,
    comment:      'Contador de intentos fallidos consecutivos del NIP en la consulta pública (D7).',
  },
  nipBloqueadoHasta: {
    type:      DataTypes.DATE,
    allowNull: true,
    comment:   'Bloqueo temporal tras 5 intentos fallidos (D7): 15 min desde el 5to fallo.',
  },
  recuperacionCodigoHash: {
    type:      DataTypes.STRING(100),
    allowNull: true,
    comment:   'Hash bcrypt del código de verificación de 6 dígitos (recuperación de NIP en 2 pasos). El texto plano nunca se persiste. Null cuando no hay recuperación en curso.',
  },
  recuperacionExpiraEn: {
    type:      DataTypes.DATE,
    allowNull: true,
    comment:   'Vencimiento del código de recuperación (10 min desde que se solicitó).',
  },
  recuperacionIntentosFallidos: {
    type:         DataTypes.INTEGER,
    allowNull:    false,
    defaultValue: 0,
    comment:      'Contador de intentos fallidos consecutivos del código de recuperación. Independiente del contador de NIP.',
  },
  recuperacionBloqueadaHasta: {
    type:      DataTypes.DATE,
    allowNull: true,
    comment:   'Bloqueo temporal tras 5 intentos fallidos del código de recuperación: 15 min desde el 5to fallo.',
  },
  createdById: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  createdByNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  updatedById: {
    type:      DataTypes.STRING(200),
    allowNull: true,
    comment:   'Quién hizo la última edición de teléfono/nombre/activa. Alcanza con quién/cuándo (decisión confirmada, ver design.md), sin tabla de historial completo.',
  },
  updatedByNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
}, {
  tableName:   'loyalty_cards',
  underscored: true,
  defaultScope: {
    attributes: { exclude: ['nipHash', 'recuperacionCodigoHash'] },
  },
  scopes: {
    withNipHash: {
      attributes: { include: ['nipHash'] },
    },
    withRecuperacionCodigoHash: {
      attributes: { include: ['recuperacionCodigoHash'] },
    },
  },
});

module.exports = LoyaltyCard;
