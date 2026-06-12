'use strict';

/**
 * migrate-pagos-montos.js
 *
 * Actualiza los CFDIs de Pago (tipo 'P') almacenados con total=0 / subTotal=0
 * usando los valores reales que ya están en complementoPago.totales:
 *   subTotal ← totalTrasladosBaseIVA16
 *   total    ← montoTotalPagos
 *
 * Uso:
 *   node scripts/migrate-pagos-montos.js [--dry-run]
 *
 * --dry-run: solo reporta cuántos documentos se actualizarían, sin escribir.
 */

require('dotenv').config();
const mongoose = require('mongoose');

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/cfdi_comparator';
const DRY_RUN    = process.argv.includes('--dry-run');

async function main() {
  await mongoose.connect(MONGODB_URI);
  console.log(`Conectado a MongoDB${DRY_RUN ? ' [DRY-RUN]' : ''}\n`);

  const CFDI = require('../src/models/CFDI');

  // Solo CFDIs de Pago con total=0 que ya tienen complementoPago.totales guardado
  const cursor = CFDI.find({
    tipoDeComprobante: 'P',
    total: 0,
    'complementoPago.totales': { $exists: true },
  }).select('_id uuid complementoPago.totales total subTotal').lean().cursor();

  let procesados = 0;
  let actualizados = 0;
  let sinCambio = 0;

  for await (const doc of cursor) {
    procesados++;
    const t            = doc.complementoPago?.totales ?? {};
    const nuevoTotal   = t.montoTotalPagos         ?? 0;
    const nuevoSubTotal = t.totalTrasladosBaseIVA16 ?? 0;

    if (nuevoTotal === 0 && nuevoSubTotal === 0) {
      sinCambio++;
      continue; // totales también son 0 en SAT — nada que hacer
    }

    console.log(`  UUID: ${doc.uuid}  total: ${doc.total} → ${nuevoTotal}  subTotal: ${doc.subTotal} → ${nuevoSubTotal}`);

    if (!DRY_RUN) {
      await CFDI.updateOne(
        { _id: doc._id },
        { $set: { total: nuevoTotal, subTotal: nuevoSubTotal } }
      );
    }

    actualizados++;
  }

  console.log(`\nResultado:`);
  console.log(`  Procesados : ${procesados}`);
  console.log(`  Actualizados: ${actualizados}${DRY_RUN ? ' (simulado)' : ''}`);
  console.log(`  Sin cambio  : ${sinCambio} (totales pago20 = 0)`);

  await mongoose.disconnect();
  console.log('\nDesconectado. Fin.');
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
