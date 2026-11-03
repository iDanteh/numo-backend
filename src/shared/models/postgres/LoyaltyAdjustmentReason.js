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
 *
 * SPLIT DE CATÁLOGO (2026-11-03, pedido explícito del usuario): `categoria`
 * separa visualmente (y por filtro de API) dos usos distintos del mismo
 * catálogo físico — NO se crea una tabla nueva a propósito, ambos conviven en
 * `loyalty_adjustment_reasons` porque comparten el 100% del esquema
 * (clave/nombre/activo/puntosFijos) y el mismo `reasonId` en
 * `loyalty_points_ledger.reasonId` (un AJUSTE puede usar un motivo de
 * cualquiera de las dos categorías, el ledger no distingue):
 *   - `'ajuste'`: motivos de corrección manual (ej. "Corrección").
 *   - `'bonificacion'`: catálogo de bonos/regalos (ej. "Bono", "Regalo").
 * La UI (lealtad-config.component.ts) muestra dos secciones separadas,
 * cada una fija a su categoria — el usuario nunca la elige en el formulario.
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
  categoria: {
    // STRING + validate (no ENUM Postgres nativo): un ENUM de Postgres exige
    // `ALTER TYPE ... ADD VALUE` fuera de transacción para agregar un valor
    // futuro — mismo criterio ya elegido para `tipo` en LoyaltyPointsLedger.js.
    type:         DataTypes.STRING(20),
    allowNull:    false,
    defaultValue: 'ajuste',
    validate:     { isIn: [['ajuste', 'bonificacion']] },
    comment:      "'ajuste': motivos de corrección manual. 'bonificacion': catálogo de bonos/regalos. Split 2026-11-03 — mismo esquema físico, separado solo visualmente/por filtro en la UI.",
  },
}, {
  tableName:   'loyalty_adjustment_reasons',
  underscored: true,
});

module.exports = LoyaltyAdjustmentReason;
