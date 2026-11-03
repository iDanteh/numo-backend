'use strict';

/**
 * shared/models/postgres/LoyaltyKoreConcepto.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — catálogo de conceptos (tipos de venta) espejado desde
 * Kore (`GET /conceptos`, ver loyalty-kore-catalogos.client.js), con un flag
 * propio `acumulaPuntos` 100% manual: Kore no expone ningún flag nativo de
 * acumulación para conceptos (a diferencia de formas de pago), así que esta
 * columna es la única fuente de verdad desde el día uno.
 *
 * `activo:false` cuando un sync posterior ya no trae ese `koreId` en la
 * respuesta — nunca se borra la fila (mismo criterio que
 * LoyaltyAdjustmentReason/LoyaltyPromotion: podría quedar referenciada a
 * futuro desde el ledger).
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyKoreConcepto = sequelize.define('LoyaltyKoreConcepto', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  koreId: {
    type:      DataTypes.STRING(50),
    allowNull: false,
    unique:    true,
  },
  nombre: {
    type:      DataTypes.STRING(150),
    allowNull: false,
  },
  abreviatura: {
    type:      DataTypes.STRING(20),
    allowNull: true,
  },
  tipo: {
    type:      DataTypes.STRING(20), // 'ENTRADA' | 'SALIDA', informativo, tal cual lo manda Kore
    allowNull: true,
  },
  acumulaPuntos: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: false,
    comment:      'Decisión 100% manual del admin — Kore no expone ningún flag nativo de acumulación para conceptos.',
  },
  activo: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: true,
    comment:      'false cuando un sync posterior ya no trae este koreId — nunca se borra la fila.',
  },
}, {
  tableName:   'loyalty_kore_conceptos',
  underscored: true,
});

module.exports = LoyaltyKoreConcepto;
