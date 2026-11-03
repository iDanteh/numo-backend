'use strict';

/**
 * shared/models/postgres/LoyaltyKoreFormaPago.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — catálogo de formas de pago espejado desde Kore
 * (`GET /api/formasdepago`, ver loyalty-kore-catalogos.client.js).
 *
 * Kore SÍ expone flags nativos (`AcumulaPuntosTuberos`, `EsPuntos`), pero
 * decisión confirmada con el usuario (2026-10-26): nunca se confía en ellos
 * como la verdad — el entorno de test ya demostró estar desincronizado de
 * producción, y Kore no va a avisar con webhook si los cambia. Se guardan acá
 * como `koreAcumulaPuntosHint`/`koreEsPuntosHint`, solo de referencia
 * informativa en la UI; `acumulaPuntos`/`esPuntos` son la config real,
 * siempre manual, nunca pisada por un sync.
 *
 * Invariante: como mucho UNA fila con `esPuntos:true` a la vez (índice único
 * parcial) — además reforzado explícitamente en
 * loyalty-config.service.js#actualizarFormaPagoKore antes de escribir, para
 * dar un error de negocio legible en vez de uno de constraint.
 *
 * `activo:false` cuando un sync posterior ya no trae ese `koreId` — mismo
 * criterio que LoyaltyKoreConcepto, nunca se borra la fila.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyKoreFormaPago = sequelize.define('LoyaltyKoreFormaPago', {
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
  acumulaPuntos: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: false,
    comment:      'Decisión manual del admin — nunca pisada por un sync, independiente de koreAcumulaPuntosHint.',
  },
  esPuntos: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: false,
    comment:      'Marca cuál forma de pago representa "Puntos" para el futuro webhook de devengación. Como mucho una fila true a la vez.',
  },
  koreAcumulaPuntosHint: {
    type:      DataTypes.BOOLEAN,
    allowNull: true,
    comment:   'Espejo informativo de AcumulaPuntosTuberos tal cual lo manda Kore — se pisa en cada sync, NUNCA se usa en lógica de negocio.',
  },
  koreEsPuntosHint: {
    type:      DataTypes.BOOLEAN,
    allowNull: true,
    comment:   'Espejo informativo de EsPuntos tal cual lo manda Kore — se pisa en cada sync, NUNCA se usa en lógica de negocio.',
  },
  activo: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: true,
    comment:      'false cuando un sync posterior ya no trae este koreId — nunca se borra la fila.',
  },
}, {
  tableName:   'loyalty_kore_formas_pago',
  underscored: true,
  indexes: [
    {
      name:   'loyalty_kore_formas_pago_es_puntos_uq',
      unique: true,
      fields: ['es_puntos'],
      where:  { es_puntos: true },
    },
  ],
});

module.exports = LoyaltyKoreFormaPago;
