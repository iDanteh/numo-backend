'use strict';

/**
 * banks/scripts/migrate-bank-config-saldo.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Migración de schema: agrega las columnas de saldo inicial a bank_configs.
 *
 * Columnas añadidas:
 *   saldo_inicial              NUMERIC(18,2)  — monto del saldo inicial del banco
 *   saldo_inicial_fecha_corte  TIMESTAMPTZ    — momento en que se registró el saldo
 *
 * Idempotente: usa ADD COLUMN IF NOT EXISTS, seguro correrlo múltiples veces.
 *
 * Uso (desde el servidor):
 *   docker exec numo-backend node src/banks/scripts/migrate-bank-config-saldo.js
 *
 * Variables de entorno requeridas: POSTGRES_URI
 */

require('dotenv').config();

const { sequelize }      = require('../../config/database.postgres');
const { logger }         = require('../../shared/utils/logger');

async function run() {
  await sequelize.authenticate();
  logger.info('PostgreSQL conectado');

  await sequelize.query(`
    ALTER TABLE bank_configs
      ADD COLUMN IF NOT EXISTS saldo_inicial             NUMERIC(18, 2) DEFAULT NULL,
      ADD COLUMN IF NOT EXISTS saldo_inicial_fecha_corte TIMESTAMPTZ    DEFAULT NULL;
  `);

  logger.info('✓ bank_configs: columnas saldo_inicial y saldo_inicial_fecha_corte verificadas');

  await sequelize.close();
  process.exit(0);
}

run().catch((err) => {
  logger.error('Error en migración de bank_config saldo:', err);
  process.exit(1);
});
