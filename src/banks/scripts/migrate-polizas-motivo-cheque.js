'use strict';

/**
 * banks/scripts/migrate-polizas-motivo-cheque.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Migración de schema para el módulo de Pólizas.
 *
 * Cambios aplicados:
 *   1. enum_polizas_tipo — agrega el valor 'C' (Cheque) al tipo ENUM.
 *   2. polizas.motivo_cancelacion VARCHAR(500) — motivo al cancelar.
 *   3. polizas.motivo_reversion  VARCHAR(500) — motivo al revertir a borrador.
 *
 * Idempotente: seguro correrlo múltiples veces.
 *
 * Uso local:
 *   node src/banks/scripts/migrate-polizas-motivo-cheque.js
 *
 * Uso en producción (Docker):
 *   docker exec numo-backend node src/banks/scripts/migrate-polizas-motivo-cheque.js
 *
 * Variables de entorno requeridas: POSTGRES_URI
 */

require('dotenv').config();

const { sequelize } = require('../../config/database.postgres');
const { logger }    = require('../../shared/utils/logger');

async function run() {
  await sequelize.authenticate();
  logger.info('PostgreSQL conectado');

  // 1. Agregar 'C' al ENUM solo si aún no existe
  await sequelize.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_enum e
        JOIN pg_type t ON e.enumtypid = t.oid
        WHERE t.typname = 'enum_polizas_tipo' AND e.enumlabel = 'C'
      ) THEN
        ALTER TYPE "enum_polizas_tipo" ADD VALUE 'C';
      END IF;
    END$$;
  `);
  logger.info('✓ enum_polizas_tipo: valor C (Cheque) verificado');

  // 2. Columnas de motivo
  await sequelize.query(`
    ALTER TABLE polizas
      ADD COLUMN IF NOT EXISTS motivo_cancelacion VARCHAR(500) DEFAULT NULL,
      ADD COLUMN IF NOT EXISTS motivo_reversion   VARCHAR(500) DEFAULT NULL;
  `);
  logger.info('✓ polizas: columnas motivo_cancelacion y motivo_reversion verificadas');

  await sequelize.close();
  process.exit(0);
}

run().catch((err) => {
  logger.error('Error en migración polizas-motivo-cheque:', err);
  process.exit(1);
});
