'use strict';

/**
 * banks/scripts/migrate-polizas-tipo-pago.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Agrega el valor 'P' (Pago) al ENUM enum_polizas_tipo en PostgreSQL.
 *
 * Idempotente: seguro correrlo múltiples veces.
 *
 * Uso local:
 *   node src/banks/scripts/migrate-polizas-tipo-pago.js
 *
 * Uso en producción (Docker):
 *   docker exec numo-backend node src/banks/scripts/migrate-polizas-tipo-pago.js
 *
 * Variables de entorno requeridas: POSTGRES_URI
 */

require('dotenv').config();

const { sequelize } = require('../../config/database.postgres');
const { logger }    = require('../../shared/utils/logger');

async function run() {
  await sequelize.authenticate();
  logger.info('PostgreSQL conectado');

  await sequelize.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_enum e
        JOIN pg_type t ON e.enumtypid = t.oid
        WHERE t.typname = 'enum_polizas_tipo' AND e.enumlabel = 'P'
      ) THEN
        ALTER TYPE "enum_polizas_tipo" ADD VALUE 'P';
      END IF;
    END$$;
  `);
  logger.info('✓ enum_polizas_tipo: valor P (Pago) verificado');

  await sequelize.close();
  process.exit(0);
}

run().catch((err) => {
  logger.error('Error en migración polizas-tipo-pago:', err);
  process.exit(1);
});
