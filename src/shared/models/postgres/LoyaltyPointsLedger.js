'use strict';

/**
 * shared/models/postgres/LoyaltyPointsLedger.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — historial append-only de movimientos de puntos.
 * Única escritura: el servicio de ledger (_aplicarMovimiento, PR2). No hay
 * update ni delete en ningún servicio. `onDelete: 'RESTRICT'`: el historial
 * nunca desaparece aunque la tarjeta se intente borrar.
 *
 * El índice único es PARCIAL sobre (origen, referencia_externa), no sobre la
 * referencia sola: dos orígenes externos distintos pueden reusar el mismo
 * espacio de IDs (ver design.md "Data Model").
 */

const { DataTypes, Op } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyPointsLedger = sequelize.define('LoyaltyPointsLedger', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  cardId: {
    type:       DataTypes.INTEGER,
    allowNull:  false,
    references: { model: 'loyalty_cards', key: 'id' },
    onDelete:   'RESTRICT',
    comment:    'Tarjeta (RESTRICT: el historial nunca desaparece)',
  },
  tipo: {
    type:      DataTypes.STRING(20),
    allowNull: false,
    validate:  { isIn: [['ACUMULACION', 'CANJE', 'AJUSTE', 'MIGRACION']] },
  },
  puntos: {
    type:      DataTypes.DECIMAL(10, 2),
    allowNull: false,
    comment:   'Con signo: + abona, - descuenta',
  },
  saldoResultante: {
    type:      DataTypes.DECIMAL(12, 2),
    allowNull: false,
  },
  montoVenta: {
    type:      DataTypes.DECIMAL(14, 2),
    allowNull: true,
    comment:   'Solo ACUMULACION — subtotal (antes de impuesto) de la venta en Kore, usado para calcular puntos',
  },
  montoVentaTotal: {
    type:      DataTypes.DECIMAL(14, 2),
    allowNull: true,
    comment:   'Solo ACUMULACION — total de la venta en Kore (con impuesto), solo auditoría, nunca se muestra en la UI',
  },
  porcentajeAplicado: {
    type:      DataTypes.DECIMAL(5, 2),
    allowNull: true,
    comment:   'Snapshot del % del nivel al acumular',
  },
  nivelAplicado: {
    type:      DataTypes.STRING(10),
    allowNull: true,
    comment:   'Snapshot del nivel al acumular',
  },
  promocionNombre: {
    type:      DataTypes.STRING(150),
    allowNull: true,
    comment:   'Snapshot del nombre de la promoción vigente al acumular (null si no hubo)',
  },
  promocionPorcentaje: {
    type:      DataTypes.DECIMAL(5, 2),
    allowNull: true,
    comment:   'Snapshot del % de bono de la promoción vigente al acumular (null si no hubo)',
  },
  centroCostoId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'centros_costo', key: 'id' },
    comment:    'Obligatorio para ACUMULACION/CANJE (validado en servicio); null en AJUSTE/MIGRACION',
  },
  reasonId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'loyalty_adjustment_reasons', key: 'id' },
  },
  justificacion: {
    type:      DataTypes.TEXT,
    allowNull: true,
  },
  origen: {
    type:         DataTypes.STRING(20),
    allowNull:    false,
    defaultValue: 'manual',
    comment:      "'manual' | 'migracion' | 'kore' (futuro)",
  },
  referenciaExterna: {
    type:      DataTypes.STRING(100),
    allowNull: true,
    comment:   'Llave de idempotencia futura (Kore)',
  },
  usuarioId: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
  usuarioNombre: {
    type:      DataTypes.STRING(200),
    allowNull: true,
  },
}, {
  tableName:   'loyalty_points_ledger',
  underscored: true,
  updatedAt:   false,
  indexes: [
    { name: 'loyalty_ledger_card_created', fields: ['card_id', 'created_at'] },
    {
      name:   'loyalty_ledger_origen_ref_uq',
      unique: true,
      fields: ['origen', 'referencia_externa'],
      where:  { referencia_externa: { [Op.ne]: null } },
    },
  ],
});

module.exports = LoyaltyPointsLedger;
