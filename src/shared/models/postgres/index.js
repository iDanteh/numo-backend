'use strict';

/**
 * shared/models/postgres/index.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Punto de entrada para todos los modelos Sequelize.
 * Define asociaciones y expone `syncModels` para el bootstrap de la app.
 *
 * Orden de sincronización:
 *   1. Tablas sin dependencias externas (User, BankConfig, Entity)
 *   2. Tablas con auto-referencia (AccountPlan → parent_id)
 *   3. Tablas con FK a otras tablas ya creadas (PeriodoFiscal → users)
 */

const User          = require('./User');
const BankConfig    = require('./BankConfig');
const BankRule      = require('./BankRule');
const AccountPlan   = require('./AccountPlan');
const Entity        = require('./Entity');
const PeriodoFiscal = require('./PeriodoFiscal');
const Permission    = require('./Permission');
const Role          = require('./Role');

// ── Asociaciones ──────────────────────────────────────────────────────────────

/** Árbol de cuentas contables */
AccountPlan.belongsTo(AccountPlan, { foreignKey: 'parentId', as: 'parent'   });
AccountPlan.hasMany  (AccountPlan, { foreignKey: 'parentId', as: 'children' });

/** Períodos fiscales creados por usuarios */
PeriodoFiscal.belongsTo(User, { foreignKey: 'createdBy', as: 'creator' });
User.hasMany(PeriodoFiscal,  { foreignKey: 'createdBy', as: 'periodos' });

// ── Sincronización ────────────────────────────────────────────────────────────

/**
 * Crea (o actualiza) las tablas en PostgreSQL.
 *
 * En desarrollo: `alter: true` ajusta columnas sin borrar datos.
 * En producción: `alter: false` — los cambios de schema deben hacerse
 *                mediante migraciones Sequelize CLI.
 */
async function syncModels() {
  const isProd = process.env.NODE_ENV === 'production';

  // Tablas sin FK externas primero
  await Promise.all([
    User.sync({ alter: !isProd }),
    BankConfig.sync({ alter: !isProd }),
    BankRule.sync({ alter: !isProd }),
    Entity.sync({ alter: !isProd }),
    Permission.sync({ alter: !isProd }),
    Role.sync({ alter: !isProd }),
  ]);

  // AccountPlan se auto-referencia → debe existir antes de crear la FK
  await AccountPlan.sync({ alter: !isProd });

  // PeriodoFiscal depende de users
  await PeriodoFiscal.sync({ alter: !isProd });
}

module.exports = { User, BankConfig, BankRule, AccountPlan, Entity, PeriodoFiscal, Permission, Role, syncModels };
