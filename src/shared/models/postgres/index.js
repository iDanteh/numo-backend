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
const ClienteCatalogo   = require('./ClienteCatalogo');
const CobroSucursalPendiente = require('./CobroSucursalPendiente');

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

/** Regla de mapeo CFDI usada al generar el movimiento */
PolizaMovimiento.belongsTo(CfdiMappingRule, { foreignKey: 'reglaId', as: 'regla' });
CfdiMappingRule.hasMany(PolizaMovimiento,   { foreignKey: 'reglaId', as: 'movimientosGenerados' });

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

  // Tablas sin FK externas primero. Se sincronizan una por una (no en
  // Promise.all): varios ALTER TABLE concurrentes contra el mismo Postgres
  // agotan la memoria compartida de locks/catálogo (error "memoria compartida
  // agotada" / "out of shared memory", típico con max_locks_per_transaction
  // bajo en instancias de desarrollo).
  await User.sync({ alter: !isProd });
  await BankConfig.sync({ alter: !isProd });
  await BankRule.sync({ alter: !isProd });
  await Entity.sync({ alter: !isProd });
  await Permission.sync({ alter: !isProd });
  await Role.sync({ alter: !isProd });

  // Columna intercompañía en entidades (idempotente)
  await Poliza.sequelize.query(`
    ALTER TABLE entities
      ADD COLUMN IF NOT EXISTS es_intercompania BOOLEAN NOT NULL DEFAULT FALSE
  `).catch(e => console.warn('[syncModels] ADD COLUMN es_intercompania:', e.message));

  // CentroCosto/ClienteCatalogo: force:false para evitar conflictos con alter en primera ejecución.
  // La columna centro_costo_id en poliza_movimientos se agrega vía raw SQL más abajo.
  await CentroCosto.sync({ force: false });
  await ClienteCatalogo.sync({ force: false });

  // Cola de cobros cruzados de sucursal (ver CobroSucursalPendiente.js) —
  // tabla nueva, force:false para solo crearla si no existe.
  await CobroSucursalPendiente.sync({ force: false });

  // AccountPlan se auto-referencia → debe existir antes de crear la FK
  await AccountPlan.sync({ alter: !isProd });

  // PeriodoFiscal depende de users
  await PeriodoFiscal.sync({ alter: !isProd });

  // Reglas de mapeo CFDI deben existir antes de poliza_movimientos (FK regla_id)
  await CfdiMappingRule.sync({ alter: !isProd });

  // Pólizas: force:false para no tocar ENUMs ni datos existentes.
  await Poliza.sync({ force: false });
  await PolizaMovimiento.sync({ force: false });

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

  // Trazabilidad de regla de mapeo en movimientos
  await Poliza.sequelize.query(`
    ALTER TABLE poliza_movimientos
      ADD COLUMN IF NOT EXISTS regla_id     INTEGER REFERENCES cfdi_mapping_rules(id) ON DELETE SET NULL,
      ADD COLUMN IF NOT EXISTS regla_nombre VARCHAR(200)
  `).catch(e => console.warn('[syncModels] ADD COLUMN regla_id/regla_nombre:', e.message));

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

  // Clasificación de negocio en movimientos y reglas de mapeo
  await Poliza.sequelize.query(`
    ALTER TABLE poliza_movimientos
      ADD COLUMN IF NOT EXISTS tipo_origen VARCHAR(100)
  `).catch(e => console.warn('[syncModels] ADD COLUMN tipo_origen (movimientos):', e.message));

  await Poliza.sequelize.query(`
    ALTER TABLE cfdi_mapping_rules
      ADD COLUMN IF NOT EXISTS tipo_origen VARCHAR(100)
  `).catch(e => console.warn('[syncModels] ADD COLUMN tipo_origen (reglas):', e.message));

  await Poliza.sequelize.query(`
    ALTER TABLE cfdi_mapping_rules
      ADD COLUMN IF NOT EXISTS cuenta_iva_abono VARCHAR(20)
  `).catch(e => console.warn('[syncModels] ADD COLUMN cuenta_iva_abono (reglas):', e.message));

  await Poliza.sequelize.query(`
    ALTER TABLE cfdi_mapping_rules
      ADD COLUMN IF NOT EXISTS cuenta_cargo_mixto0 VARCHAR(20)
  `).catch(e => console.warn('[syncModels] ADD COLUMN cuenta_cargo_mixto0 (reglas):', e.message));

  await Poliza.sequelize.query(`
    ALTER TABLE cfdi_mapping_rules
      ADD COLUMN IF NOT EXISTS cuenta_iva_ppd  VARCHAR(20),
      ADD COLUMN IF NOT EXISTS cuenta_abono2   VARCHAR(20)
  `).catch(e => console.warn('[syncModels] ADD COLUMN cuenta_iva_ppd/cuenta_abono2 (reglas):', e.message));

  await Poliza.sequelize.query(`
    ALTER TABLE cfdi_mapping_rules
      ADD COLUMN IF NOT EXISTS veces_usada INTEGER NOT NULL DEFAULT 0
  `).catch(e => console.warn('[syncModels] ADD COLUMN veces_usada (reglas):', e.message));

  await BankRule.sequelize.query(`
    ALTER TABLE bank_rules ADD COLUMN IF NOT EXISTS estado_destino VARCHAR(30)
  `).catch(e => console.warn('[syncModels] ADD COLUMN estado_destino (bank_rules):', e.message));

  await BankRule.sequelize.query(`
    ALTER TABLE bank_rules ADD COLUMN IF NOT EXISTS ocultar_roles JSONB DEFAULT '[]'::jsonb
  `).catch(e => console.warn('[syncModels] ADD COLUMN ocultar_roles (bank_rules):', e.message));

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

  // Folio real asociado en CONTPAQi tras importar el export (idempotente)
  await Poliza.sequelize.query(`
    ALTER TABLE polizas
      ADD COLUMN IF NOT EXISTS contpaq_folio_contado INTEGER,
      ADD COLUMN IF NOT EXISTS contpaq_folio_credito INTEGER,
      ADD COLUMN IF NOT EXISTS contpaq_asociado_por  VARCHAR(150),
      ADD COLUMN IF NOT EXISTS contpaq_asociado_en   TIMESTAMPTZ
  `).catch(e => console.warn('[syncModels] ADD COLUMN contpaq_*:', e.message));

  // Correos de alerta por entidad (vencimiento de credenciales SAT)
  await Entity.sequelize.query(`
    ALTER TABLE entities
      ADD COLUMN IF NOT EXISTS emails_alerta TEXT[] DEFAULT '{}'
  `).catch(e => console.warn('[syncModels] ADD COLUMN emails_alerta:', e.message));

  // CFDIs sustitutos (tipoRelacion='04') excluidos automáticamente al generar
  // la póliza por riesgo de doble conteo (idempotente)
  await Poliza.sequelize.query(`
    ALTER TABLE polizas
      ADD COLUMN IF NOT EXISTS sustitutos_excluidos JSONB
  `).catch(e => console.warn('[syncModels] ADD COLUMN sustitutos_excluidos:', e.message));

  // Tickets de cajas con cobro real pero sin factura ligada, detectados al
  // generar la póliza (idempotente)
  await Poliza.sequelize.query(`
    ALTER TABLE polizas
      ADD COLUMN IF NOT EXISTS pendientes_por_facturar JSONB
  `).catch(e => console.warn('[syncModels] ADD COLUMN pendientes_por_facturar:', e.message));

  // El índice único (tipo, numero, rfc, ejercicio, periodo) bloqueaba para
  // siempre el folio de una póliza cancelada (la fila sigue existiendo,
  // solo con estado='cancelada') — impedía reutilizar ese folio en una
  // futura generación aunque el rango por sucursal ya lo diera por libre.
  // Se reemplaza por un índice PARCIAL (solo cubre filas no canceladas) para
  // que cancelar una póliza libere su folio de verdad (confirmado con el
  // usuario 2026-07-27). Sequelize no migra este cambio solo con la
  // definición del modelo — DROP + CREATE explícito, idempotente.
  await Poliza.sequelize.query(`
    DROP INDEX IF EXISTS polizas_tipo_numero_rfc_ejercicio_periodo
  `).catch(e => console.warn('[syncModels] DROP INDEX polizas_tipo_numero_rfc_ejercicio_periodo:', e.message));

  await Poliza.sequelize.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS polizas_tipo_numero_rfc_ejercicio_periodo
      ON polizas (tipo, numero, rfc, ejercicio, periodo)
      WHERE estado != 'cancelada'
  `).catch(e => console.warn('[syncModels] CREATE INDEX parcial polizas_tipo_numero_rfc_ejercicio_periodo:', e.message));

  // Empresas fijas por usuario individual (idempotente) — ver User.js. Se
  // asignan desde la pantalla de Roles (selección múltiple de usuarios +
  // empresa) — no hay default por rol. Un usuario puede tener varias, por
  // eso es ARRAY y no una sola columna VARCHAR (confirmado con el usuario
  // 2026-07-28, corrigiendo el diseño de un solo RFC del mismo día).
  await User.sequelize.query(`
    ALTER TABLE users
      DROP COLUMN IF EXISTS empresa_rfc
  `).catch(e => console.warn('[syncModels] DROP COLUMN empresa_rfc (users):', e.message));

  await User.sequelize.query(`
    ALTER TABLE users
      ADD COLUMN IF NOT EXISTS empresa_rfcs VARCHAR(20)[] NOT NULL DEFAULT '{}'
  `).catch(e => console.warn('[syncModels] ADD COLUMN empresa_rfcs (users):', e.message));
}

module.exports = { User, BankConfig, BankRule, AccountPlan, Entity, PeriodoFiscal, Permission, Role, Poliza, PolizaMovimiento, CfdiMappingRule, CentroCosto, ClienteCatalogo, CobroSucursalPendiente, syncModels };
