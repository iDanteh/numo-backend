'use strict';

/**
 * shared/models/postgres/BankRule.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Reglas de categorización y bloqueo de identificación, migradas de MongoDB.
 *
 * Motivación:
 *   • Esquema fijo — sin necesidad de documentos flexibles.
 *   • Reordenamiento con garantías ACID (UPDATE múltiple en transacción).
 *   • Condiciones como JSONB: flexibles sin cambiar el schema de la tabla.
 *
 * Acciones disponibles:
 *   'categorizar'               — asigna `categoria` al movimiento que coincide.
 *   'bloquear_identificacion'   — impide marcar el movimiento como 'identificado'
 *                                 (admins pueden forzarlo; el resto recibe mensajeBloqueo).
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const BankRule = sequelize.define('BankRule', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },

  banco: {
    type:      DataTypes.STRING(50),
    allowNull: false,
  },

  nombre: {
    type:      DataTypes.STRING(200),
    allowNull: false,
  },

  // Array de { campo, operador, valor } — mismo schema que el modelo Mongoose anterior.
  condiciones: {
    type:         DataTypes.JSONB,
    allowNull:    false,
    defaultValue: [],
  },

  // 'Y' = todas deben cumplirse (AND), 'O' = al menos una (OR).
  logica: {
    type:         DataTypes.STRING(1),
    allowNull:    false,
    defaultValue: 'Y',
    validate:     { isIn: [['Y', 'O']] },
  },

  // Qué hace la regla cuando aplica a un movimiento.
  accion: {
    type:         DataTypes.STRING(30),
    allowNull:    false,
    defaultValue: 'categorizar',
    validate:     { isIn: [['categorizar', 'bloquear_identificacion', 'ocultar']] },
  },

  // Mensaje mostrado al usuario cuando una regla bloquea la identificación.
  // Solo relevante cuando accion = 'bloquear_identificacion'.
  mensajeBloqueo: {
    type:      DataTypes.STRING(500),
    allowNull: true,
  },

  // Posición de la regla dentro del banco (primera que aplica gana).
  orden: {
    type:         DataTypes.INTEGER,
    allowNull:    false,
    defaultValue: 0,
  },
}, {
  tableName:   'bank_rules',
  underscored: true,
  indexes: [
    { fields: ['banco'] },
    { fields: ['banco', 'orden'] },
    { fields: ['banco', 'accion'] },
  ],
});

module.exports = BankRule;
