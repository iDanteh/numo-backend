'use strict';

/**
 * shared/models/postgres/LoyaltyKoreNivel.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — catálogo de niveles (Bronce/Plata/Oro/...) gestionado
 * por Kore (2026-11-06, pedido explícito del usuario). A diferencia de
 * LoyaltyKoreConcepto/LoyaltyKoreFormaPago, acá `porcentaje` SÍ es autoritativo
 * de Kore (decisión confirmada: "Kore también manda el %") — Numo no
 * configura ningún % propio para esto.
 *
 * `esInicial` es la ÚNICA columna que sigue siendo decisión manual de Numo:
 * qué nivel le toca a una tarjeta nueva. Kore no tiene por qué saber esto,
 * mismo espíritu que `esPuntos` en LoyaltyKoreFormaPago (invariante de una
 * sola fila a la vez, índice único parcial).
 *
 * `koreId` es NULLABLE a propósito: ver seed-lealtad.js, que siembra 3 filas
 * transicionales (Bronce/Plata/Oro, koreId:null) para no romper el alta/
 * cambio de nivel mientras el catálogo real de Kore todavía no llegó. El
 * primer sync o webhook de Kore para ese `nombre` ADOPTA la fila existente
 * (le asigna su koreId real) en vez de crear una duplicada — ver
 * loyalty-config.service.js#sincronizarNivelesKore y
 * loyalty-kore-nivel.service.js. Postgres permite múltiples NULL bajo un
 * índice único (NULL ≠ NULL), así que las 3 filas transicionales conviven
 * sin chocar.
 *
 * `activo:false` cuando un sync posterior ya no trae ese `koreId` — nunca se
 * borra la fila (mismo criterio que LoyaltyKoreConcepto/LoyaltyKoreFormaPago:
 * podría quedar referenciada desde LoyaltyCard.nivel/LoyaltyTierHistory).
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

const LoyaltyKoreNivel = sequelize.define('LoyaltyKoreNivel', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  koreId: {
    type:      DataTypes.STRING(50),
    allowNull: true,
    unique:    true,
    comment:   'NULL = fila transicional de bootstrap (ver seed-lealtad.js), adoptada por el primer sync/webhook real de Kore.',
  },
  nombre: {
    type:      DataTypes.STRING(50),
    allowNull: false,
  },
  porcentaje: {
    type:      DataTypes.DECIMAL(5, 2),
    allowNull: true,
    comment:   'Autoritativo de Kore (a diferencia de LoyaltyKoreConcepto/FormaPago) — Numo nunca configura esto manualmente.',
  },
  esInicial: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: false,
    comment:      'Única decisión manual de Numo: qué nivel le toca a una tarjeta nueva. Como mucho una fila true a la vez.',
  },
  activo: {
    type:         DataTypes.BOOLEAN,
    allowNull:    false,
    defaultValue: true,
    comment:      'false cuando un sync posterior ya no trae este koreId — nunca se borra la fila.',
  },
}, {
  tableName:   'loyalty_kore_niveles',
  underscored: true,
  indexes: [
    {
      name:   'loyalty_kore_niveles_es_inicial_uq',
      unique: true,
      fields: ['es_inicial'],
      where:  { es_inicial: true },
    },
  ],
});

module.exports = LoyaltyKoreNivel;
