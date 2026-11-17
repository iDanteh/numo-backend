'use strict';

/**
 * shared/models/postgres/LoyaltyKoreVentaLog.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — auditoría append-only de CADA request real que Kore manda
 * a `POST /api/loyalty/kore/venta` y `/ventas` (2026-11-17, pedido explícito del
 * usuario, mismo criterio que LoyaltySecurityEvent: complementa — no reemplaza —
 * el log de Winston, acá se puede investigar/filtrar por fecha o resultado desde
 * la pantalla de Auditoría de Lealtad en vez de buscar en el log rotativo).
 * Guarda el payload CRUDO (`endpoint` distingue singular/lote) y el resultado —
 * nunca se actualiza ni se borra desde la app.
 */
const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyKoreVentaLog = sequelize.define('LoyaltyKoreVentaLog', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  endpoint: {
    type:      DataTypes.STRING(10), // 'venta' | 'ventas'
    allowNull: false,
  },
  payload: {
    type:      DataTypes.JSONB,
    allowNull: false,
  },
  resultado: {
    type:      DataTypes.STRING(10), // 'exito' | 'error'
    allowNull: false,
  },
  detalle: {
    type:      DataTypes.TEXT,
    allowNull: true,
    comment:   'Null cuando resultado=exito. En error: err.message de procesarVentaKore/procesarLoteVentasKore.',
  },
  createdAt: {
    type:         DataTypes.DATE,
    allowNull:    false,
    defaultValue: DataTypes.NOW,
  },
}, {
  tableName:   'loyalty_kore_venta_logs',
  underscored: true,
  timestamps:  false,
  indexes: [
    { name: 'loyalty_kore_venta_logs_created_at', fields: ['created_at'] },
    { name: 'loyalty_kore_venta_logs_resultado', fields: ['resultado'] },
  ],
});

module.exports = LoyaltyKoreVentaLog;
