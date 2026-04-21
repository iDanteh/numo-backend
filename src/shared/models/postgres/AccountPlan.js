'use strict';

/**
 * shared/models/postgres/AccountPlan.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Catálogo de cuentas contables (formato SAT 10 dígitos).
 *
 * Migrado de MongoDB a PostgreSQL para:
 *   • Garantías ACID en la jerarquía (parentId FK con integridad referencial)
 *   • Búsquedas de texto más eficientes con índices GIN / tsvector
 *   • Consultas recursivas (árbol) con CTE de Postgres
 *
 * Los campos `tipo`, `naturaleza` y `nivel` se auto-derivan del código
 * en el hook `beforeValidate`, igual que el pre-validate de Mongoose.
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');

// ── Helpers (misma lógica que el modelo Mongoose original) ───────────────────

function codigoToNivel(codigo = '') {
  const sig = codigo.replace(/0+$/, '') || codigo[0] || '0';
  return sig.length === 1 ? 1 : Math.floor(sig.length / 2) + 1;
}

function inferTipoNat(codigo) {
  const map = {
    '1': { tipo: 'ACTIVO',  naturaleza: 'DEUDORA'   },
    '2': { tipo: 'PASIVO',  naturaleza: 'ACREEDORA'  },
    '3': { tipo: 'CAPITAL', naturaleza: 'ACREEDORA'  },
    '4': { tipo: 'INGRESO', naturaleza: 'ACREEDORA'  },
    '5': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
    '6': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
    '7': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
    '8': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
    '9': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
  };
  return map[String(codigo).trim()[0]] ?? { tipo: 'GASTO', naturaleza: 'DEUDORA' };
}

// ── Modelo ───────────────────────────────────────────────────────────────────

const AccountPlan = sequelize.define('AccountPlan', {
  id: {
    type:          DataTypes.INTEGER,
    primaryKey:    true,
    autoIncrement: true,
  },
  codigo: {
    type:      DataTypes.STRING(20),
    allowNull: false,
    unique:    true,
  },
  nombre: {
    type:      DataTypes.STRING(255),
    allowNull: false,
  },
  ctaMayor: {
    type:      DataTypes.STRING(20),
    allowNull: true,
  },
  tipo: {
    type:         DataTypes.ENUM('ACTIVO', 'PASIVO', 'CAPITAL', 'INGRESO', 'GASTO'),
    allowNull:    false,
  },
  naturaleza: {
    type:      DataTypes.ENUM('DEUDORA', 'ACREEDORA'),
    allowNull: false,
  },
  nivel: {
    type:      DataTypes.INTEGER,
    allowNull: false,
  },
  parentId: {
    type:       DataTypes.INTEGER,
    allowNull:  true,
    references: { model: 'account_plans', key: 'id' },
    onDelete:   'SET NULL',
  },
  isActive: {
    type:         DataTypes.BOOLEAN,
    defaultValue: true,
  },
}, {
  tableName:   'account_plans',
  underscored: true,
  hooks: {
    /**
     * Auto-deriva tipo, naturaleza y nivel a partir del código SAT.
     * Corre antes de la validación, igual que el pre('validate') de Mongoose.
     */
    beforeValidate(instance) {
      if (instance.codigo) {
        if (!instance.nivel) {
          instance.nivel = codigoToNivel(instance.codigo);
        }
        const inferred = inferTipoNat(instance.codigo);
        instance.tipo       = inferred.tipo;
        instance.naturaleza = inferred.naturaleza;
      }
    },
  },
});

// Exponer helpers como estáticos (misma API que el modelo Mongoose)
AccountPlan.computeNivel = codigoToNivel;
AccountPlan.inferTipoNat = inferTipoNat;

module.exports = AccountPlan;
