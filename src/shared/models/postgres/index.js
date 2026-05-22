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

const User              = require('./User');
const BankConfig        = require('./BankConfig');
const BankRule          = require('./BankRule');
const AccountPlan       = require('./AccountPlan');
const Entity            = require('./Entity');
const PeriodoFiscal     = require('./PeriodoFiscal');
const Permission        = require('./Permission');
const Role              = require('./Role');
const Poliza            = require('./Poliza');
const PolizaMovimiento  = require('./PolizaMovimiento');
const CfdiMappingRule   = require('./CfdiMappingRule');
const CentroCosto       = require('./CentroCosto');

// ── Asociaciones ──────────────────────────────────────────────────────────────

/** Árbol de cuentas contables */
AccountPlan.belongsTo(AccountPlan, { foreignKey: 'parentId', as: 'parent'   });
AccountPlan.hasMany  (AccountPlan, { foreignKey: 'parentId', as: 'children' });

/** Períodos fiscales creados por usuarios */
PeriodoFiscal.belongsTo(User, { foreignKey: 'createdBy', as: 'creator' });
User.hasMany(PeriodoFiscal,  { foreignKey: 'createdBy', as: 'periodos' });

/** Pólizas contables */
Poliza.hasMany        (PolizaMovimiento, { foreignKey: 'polizaId', as: 'movimientos', onDelete: 'CASCADE' });
PolizaMovimiento.belongsTo(Poliza,       { foreignKey: 'polizaId', as: 'poliza' });
PolizaMovimiento.belongsTo(AccountPlan,  { foreignKey: 'cuentaId', as: 'cuenta' });
AccountPlan.hasMany   (PolizaMovimiento, { foreignKey: 'cuentaId', as: 'movimientos' });

/** Centros de costo */
PolizaMovimiento.belongsTo(CentroCosto, { foreignKey: 'centroCostoId', as: 'centroCostoObj' });
CentroCosto.hasMany(PolizaMovimiento,   { foreignKey: 'centroCostoId', as: 'movimientos' });

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

  // Columna intercompañía en entidades (idempotente)
  await Poliza.sequelize.query(`
    ALTER TABLE entities
      ADD COLUMN IF NOT EXISTS es_intercompania BOOLEAN NOT NULL DEFAULT FALSE
  `).catch(e => console.warn('[syncModels] ADD COLUMN es_intercompania:', e.message));

  // CentroCosto: force:false para evitar conflictos con alter en primera ejecución.
  // La columna centro_costo_id en poliza_movimientos se agrega vía raw SQL más abajo.
  await CentroCosto.sync({ force: false });

  // AccountPlan se auto-referencia → debe existir antes de crear la FK
  await AccountPlan.sync({ alter: !isProd });

  // PeriodoFiscal depende de users
  await PeriodoFiscal.sync({ alter: !isProd });

  // Pólizas: force:false para no tocar ENUMs ni datos existentes.
  await Poliza.sync({ force: false });
  await PolizaMovimiento.sync({ force: false });

  // Reglas de mapeo CFDI (sin ENUMs problemáticos excepto tipoComprobante)
  await CfdiMappingRule.sync({ alter: !isProd });

  // Agregar columnas de auditoría si no existen (seguro correrlo múltiples veces)
  await Poliza.sequelize.query(`
    ALTER TABLE polizas
      ADD COLUMN IF NOT EXISTS contabilizado_por  VARCHAR(150),
      ADD COLUMN IF NOT EXISTS contabilizada_at   TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS cancelado_por      VARCHAR(150),
      ADD COLUMN IF NOT EXISTS cancelada_at       TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS revertido_por      VARCHAR(150),
      ADD COLUMN IF NOT EXISTS revertida_at       TIMESTAMPTZ
  `).catch(() => {});

  await Poliza.sequelize.query(`
    ALTER TABLE poliza_movimientos
      ADD COLUMN IF NOT EXISTS rfc_tercero VARCHAR(13)
  `).catch(() => {});

  // Centro de costo FK en movimientos (idempotente)
  await Poliza.sequelize.query(`
    ALTER TABLE poliza_movimientos
      ADD COLUMN IF NOT EXISTS centro_costo_id INTEGER REFERENCES centros_costo(id)
  `).catch(e => console.warn('[syncModels] ADD COLUMN centro_costo_id:', e.message));

  // Permitir cuentaId nulo (movimientos con cuenta faltante en catálogo)
  await Poliza.sequelize.query(
    `ALTER TABLE poliza_movimientos ALTER COLUMN cuenta_id DROP NOT NULL`
  ).catch(e => console.warn('[syncModels] DROP NOT NULL cuenta_id:', e.message));

  await Poliza.sequelize.query(
    `ALTER TABLE poliza_movimientos ADD COLUMN IF NOT EXISTS cuenta_faltante BOOLEAN NOT NULL DEFAULT FALSE`
  ).catch(e => console.warn('[syncModels] ADD COLUMN cuenta_faltante:', e.message));

  // Campos SAT del CFDI en movimientos (tipoComprobante, metodoPago, formaPago, folio, rfcEmisor, rfcReceptor)
  await Poliza.sequelize.query(`
    ALTER TABLE poliza_movimientos
      ADD COLUMN IF NOT EXISTS tipo_comprobante VARCHAR(1),
      ADD COLUMN IF NOT EXISTS metodo_pago      VARCHAR(3),
      ADD COLUMN IF NOT EXISTS forma_pago       VARCHAR(3),
      ADD COLUMN IF NOT EXISTS folio            VARCHAR(40),
      ADD COLUMN IF NOT EXISTS rfc_emisor       VARCHAR(13),
      ADD COLUMN IF NOT EXISTS rfc_receptor     VARCHAR(13)
  `).catch(e => console.warn('[syncModels] ADD COLUMN SAT fields:', e.message));

  // Motivo de cancelación/reversión + tipo Cheque (idempotente)
  await Poliza.sequelize.query(`
    ALTER TABLE polizas
      ADD COLUMN IF NOT EXISTS motivo_cancelacion VARCHAR(500),
      ADD COLUMN IF NOT EXISTS motivo_reversion   VARCHAR(500)
  `).catch(() => {});

  await Poliza.sequelize.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_enum e JOIN pg_type t ON e.enumtypid = t.oid
        WHERE t.typname = 'enum_polizas_tipo' AND e.enumlabel = 'C'
      ) THEN ALTER TYPE "enum_polizas_tipo" ADD VALUE 'C'; END IF;
      IF NOT EXISTS (
        SELECT 1 FROM pg_enum e JOIN pg_type t ON e.enumtypid = t.oid
        WHERE t.typname = 'enum_polizas_tipo' AND e.enumlabel = 'A'
      ) THEN ALTER TYPE "enum_polizas_tipo" ADD VALUE 'A'; END IF;
    END$$;
  `).catch(() => {});
}

module.exports = { User, BankConfig, BankRule, AccountPlan, Entity, PeriodoFiscal, Permission, Role, Poliza, PolizaMovimiento, CfdiMappingRule, CentroCosto, syncModels };
