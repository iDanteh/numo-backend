'use strict';

/**
 * banks/scripts/resync-bank-movement-counter.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Resincroniza counters.bankMovement contra el folio máximo real presente en
 * bank_movements. Hace falta cuando una copia PARCIAL de la base (ej. Test, con
 * menos capacidad que producción) trae documentos de bank_movements con folios
 * altos pero no trae (o trae desactualizado) el contador correspondiente —
 * los próximos imports reservan folios que ya existen y Mongo los rechaza por
 * el índice único (E11000 folio_1), descartando movimientos reales en silencio
 * (ver comentario "Hallazgo real 2026-07-30" en bank.service.js#importFile).
 *
 * Idempotente / seguro de re-ejecutar: solo SUBE el contador si está detrás del
 * máximo real, nunca lo baja. Si ya está sincronizado, no escribe nada.
 *
 * Uso:
 *   node src/banks/scripts/resync-bank-movement-counter.js          (dry-run, no escribe nada)
 *   node src/banks/scripts/resync-bank-movement-counter.js --apply  (escribe de verdad)
 *
 * Variable de entorno requerida: MONGODB_URI (o MONGO_URI)
 */

require('dotenv').config();

const mongoose = require('mongoose');
const BankMovement = require('../domains/banks/BankMovement.model');
const Counter       = require('../shared/models/Counter');

async function run({
  apply      = process.argv.includes('--apply'),
  mongodbUri = process.env.MONGODB_URI || process.env.MONGO_URI,
} = {}) {
  if (!mongodbUri) {
    throw new Error('Falta MONGODB_URI (o MONGO_URI) en el entorno.');
  }

  await mongoose.connect(mongodbUri);

  try {
    const maxFolioDoc = await BankMovement.findOne({ folio: { $ne: null } })
      .sort({ folio: -1 })
      .select('folio')
      .lean();
    const maxFolio = maxFolioDoc ? parseInt(maxFolioDoc.folio, 10) : 0;

    const counterDoc = await Counter.findById('bankMovement').lean();
    const seqActual = counterDoc?.seq ?? 0;

    console.log(`Folio máximo real en bank_movements: ${maxFolio || '(sin movimientos)'}`);
    console.log(`counters.bankMovement.seq actual:    ${seqActual}`);

    if (seqActual >= maxFolio) {
      console.log('Ya está sincronizado (o por delante) — nada que hacer.');
      return { seqActual, maxFolio, actualizado: false };
    }

    console.log(`Desincronizado: el contador está ${maxFolio - seqActual} folio(s) por detrás.`);

    if (!apply) {
      console.log(`DRY-RUN: pasaría counters.bankMovement.seq de ${seqActual} a ${maxFolio}. Usa --apply para escribir.`);
      return { seqActual, maxFolio, actualizado: false };
    }

    await Counter.findOneAndUpdate(
      { _id: 'bankMovement' },
      { $set: { seq: maxFolio } },
      { upsert: true },
    );
    console.log(`Listo: counters.bankMovement.seq actualizado a ${maxFolio}.`);
    return { seqActual, maxFolio, actualizado: true };
  } finally {
    await mongoose.disconnect();
  }
}

if (require.main === module) {
  run().catch(err => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { run };
