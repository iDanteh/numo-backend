'use strict';

/**
 * banks/scripts/migrate-erp-autsNorm.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Backfill idempotente: calcula y escribe el campo _autsNorm en todos los
 * documentos ErpCuentaPendiente que aún no lo tienen (o lo tienen vacío).
 *
 * A partir de este script, el campo se mantiene actualizado automáticamente
 * en cada sync (erp-sync.service.js → extraerAutsNorm).
 *
 * Es seguro ejecutarlo varias veces; por defecto solo toca documentos con
 * _autsNorm vacío.
 *
 * Uso:
 *   node src/banks/scripts/migrate-erp-autsNorm.js
 *   node src/banks/scripts/migrate-erp-autsNorm.js --force   # recalcula TODOS
 *
 * --force: recalcula _autsNorm en TODOS los documentos con movimientos,
 * incluso los que ya lo tenían poblado. Necesario tras el fix 2026-09-11
 * (extraerAutsNorm ahora incluye TODOS los números de autorización de cada
 * formaPago, no solo el primero — ver erp-auth.utils.js/normalizarAuthLista);
 * sin --force, los documentos ya sincronizados antes del fix se quedarían
 * con el _autsNorm viejo (incompleto) para siempre, porque el modo normal
 * los salta.
 *
 * Variables de entorno requeridas: MONGODB_URI
 */

require('dotenv').config();

const mongoose           = require('mongoose');
const ErpCuentaPendiente = require('../domains/erp/ErpCuentaPendiente.model');
const { extraerAutsNorm } = require('../domains/erp/erp-auth.utils');

const MONGODB_URI = process.env.MONGODB_URI;
const BATCH_SIZE  = 500;
const FORCE       = process.argv.includes('--force');

async function run() {
  if (!MONGODB_URI) {
    console.error('ERROR: MONGODB_URI no está configurado.');
    process.exit(1);
  }

  await mongoose.connect(MONGODB_URI);
  console.log(`Conectado a MongoDB.${FORCE ? ' (--force: recalculando TODOS los documentos)' : ''}`);

  // Modo normal: solo documentos sin _autsNorm o con array vacío (idempotente).
  // Modo --force: TODOS los documentos con movimientos, sin importar si ya
  // tenían _autsNorm poblado (necesario para aplicar el fix de multi-autorización).
  const filtro = FORCE
    ? { movimientos: { $exists: true, $ne: [] } }
    : {
        $or: [
          { _autsNorm: { $exists: false } },
          { _autsNorm: { $size: 0 } },
        ],
        movimientos: { $exists: true, $ne: [] },
      };

  const cursor = ErpCuentaPendiente
    .find(filtro)
    .select('_id movimientos')
    .lean()
    .cursor();

  let processed = 0;
  let updated   = 0;
  let batch     = [];

  for await (const doc of cursor) {
    const autsNorm = extraerAutsNorm(doc.movimientos);
    if (autsNorm.length > 0) {
      batch.push({
        updateOne: {
          filter: { _id: doc._id },
          update: { $set: { _autsNorm: autsNorm } },
        },
      });
    }
    processed++;

    if (batch.length >= BATCH_SIZE) {
      const result = await ErpCuentaPendiente.bulkWrite(batch, { ordered: false });
      updated += result.modifiedCount;
      console.log(`  Procesados: ${processed} | Actualizados: ${updated}`);
      batch = [];
    }
  }

  // Último lote parcial
  if (batch.length > 0) {
    const result = await ErpCuentaPendiente.bulkWrite(batch, { ordered: false });
    updated += result.modifiedCount;
  }

  console.log(`\nMigración completada. Documentos procesados: ${processed} | Actualizados: ${updated}`);
  await mongoose.disconnect();
}

run().catch(err => {
  console.error('Error en la migración:', err);
  process.exit(1);
});
