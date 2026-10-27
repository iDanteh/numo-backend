'use strict';

/**
 * shared/models/postgres/LoyaltyAdjustmentReason.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — catálogo de motivos de ajuste manual de puntos.
 *
 * REDISEÑO 2026-10-23 (pedido explícito del usuario, reemplaza el modelo de
 * `signoPermitido`/`porcentaje` — ninguno de los dos tenía datos reales
 * configurados, verificado contra producción antes del cambio): un motivo
 * ahora es uno de dos modos, sin campo de "signo" separado:
 *   - `puntosFijos` seteado (ej. "Bono"=100, "Comisión por mora"=-200): un
 *     AJUSTE con este motivo aplica EXACTAMENTE ese valor, sin pedir nada más.
 *   - `puntosFijos` null (ej. "Corrección"): modo manual — quien aplica el
 *     ajuste escribe los puntos a mano, cualquier signo, ningún motivo lo
 *     restringe.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyAdjustmentReason = sequelize.define('LoyaltyAdjustmentReason', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  clave: {
    type:      DataTypes.STRING(50),
    allowNull: false,
    unique:    true,
  },
  nombre: {
    type:      DataTypes.STRING(150),
    allowNull: false,
  },
  activo: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: true,
  },
  puntosFijos: {
    type:      DataTypes.INTEGER,
    allowNull: true,
    comment:   'null = motivo manual (quien aplica el ajuste escribe los puntos). Con valor (puede ser negativo): el AJUSTE aplica EXACTAMENTE ese número de puntos, sin pedir nada más.',
  },
}, {
  tableName:   'loyalty_adjustment_reasons',
  underscored: true,
});

module.exports = LoyaltyAdjustmentReason;
