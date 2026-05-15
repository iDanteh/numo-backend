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
 * Es seguro ejecutarlo varias veces; solo toca documentos con _autsNorm vacío.
 *
 * Uso:
 *   node src/banks/scripts/migrate-erp-autsNorm.js
 *
 * Variables de entorno requeridas: MONGODB_URI
 */

require('dotenv').config();

const mongoose           = require('mongoose');
const ErpCuentaPendiente = require('../domains/erp/ErpCuentaPendiente.model');
const { extraerAutsNorm } = require('../domains/erp/erp-auth.utils');

const MONGODB_URI = process.env.MONGODB_URI;
const BATCH_SIZE  = 500;

async function run() {
  if (!MONGODB_URI) {
    console.error('ERROR: MONGODB_URI no está configurado.');
    process.exit(1);
  }

  await mongoose.connect(MONGODB_URI);
  console.log('Conectado a MongoDB.');

  // Solo documentos sin _autsNorm o con array vacío pero que sí tienen movimientos.
  // Los que ya tienen _autsNorm poblado se omiten (idempotente).
  const cursor = ErpCuentaPendiente
    .find({
      $or: [
        { _autsNorm: { $exists: false } },
        { _autsNorm: { $size: 0 } },
      ],
      // Solo vale la pena procesar los que potencialmente tienen auths
      movimientos: { $exists: true, $ne: [] },
    })
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
