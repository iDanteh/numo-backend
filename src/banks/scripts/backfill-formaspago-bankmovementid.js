'use strict';

/**
 * banks/scripts/backfill-formaspago-bankmovementid.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Backfill histórico para multi-bank-movement
 * (sdd/collection-request-multi-bank-movement — D1/D2 en el diseño).
 *
 * Antes de este cambio TODA solicitud identificada tenía un único
 * CollectionRequest.bankMovementId (campo raíz) — nunca existió la noción de
 * "N movimientos" por solicitud, así que todas las formasPago[] de una
 * solicitud histórica comparten, por definición, ese mismo y único
 * movimiento (no hay ambigüedad de "a cuál forma de pago le toca cuál
 * banco"). Este script copia ese valor raíz a CADA entrada de formasPago[]
 * que todavía no tenga su propio bankMovementId.
 *
 * Sin este paso, movimientosDe() (collection-request-asignaciones.js) sigue
 * funcionando vía su fallback al campo raíz para documentos pre-backfill,
 * pero cualquier código que lea formasPago[].bankMovementId directamente
 * (reportes/consultas futuras) vería null en documentos históricos. El campo
 * raíz NUNCA se borra ni se modifica — sigue @deprecated pero legible (D1).
 *
 * Idempotente / re-ejecutable: solo se consideran "candidatas" las
 * solicitudes con AL MENOS una formaPago sin bankMovementId propio (null o
 * ausente) — una solicitud ya migrada por completo, o corrida dos veces, no
 * vuelve a contarse ni a escribirse.
 *
 * Uso:
 *   node src/banks/scripts/backfill-formaspago-bankmovementid.js          (dry-run, no escribe nada)
 *   node src/banks/scripts/backfill-formaspago-bankmovementid.js --run    (escribe de verdad)
 *
 * IMPORTANTE — orden de despliegue: correr DESPUÉS de que el esquema de
 * formasPago[].bankMovementId (PR1) y la lógica de identificar()/populate
 * (PR2) ya estén desplegados en el ambiente donde se corre — el script asume
 * que el campo ya existe en el esquema activo. Primero --dry-run, revisar el
 * conteo de candidatos, luego --run.
 *
 * Variable de entorno requerida: MONGODB_URI
 */

require('dotenv').config();

const mongoose          = require('mongoose');
const CollectionRequest = require('../domains/collection-requests/CollectionRequest.model');

// _necesitaBackfill — pura: ¿al menos una formaPago sigue sin bankMovementId
// propio? (null o ausente — mismo criterio que el arrayFilters de Mongo usa
// más abajo para decidir qué entradas tocar: `{ 'f.bankMovementId': null }`
// coincide con ambos casos). Documentos ya migrados por completo se excluyen
// aquí para que el script sea idempotente/re-ejecutable sin volver a
// contarlos como candidatos.
function _necesitaBackfill(cr) {
  return (cr.formasPago ?? []).some(f => f.bankMovementId == null);
}

async function run({
  dryRun     = !process.argv.includes('--run'),
  mongodbUri = process.env.MONGODB_URI,
} = {}) {
  if (!mongodbUri) {
    console.error('ERROR: MONGODB_URI no está configurado.');
    process.exit(1);
    return;
  }

  await mongoose.connect(mongodbUri);
  console.log(`Conectado a MongoDB. Modo: ${dryRun ? 'DRY-RUN (no escribe nada)' : 'RUN (escribe de verdad)'}`);

  const solicitudes = await CollectionRequest.find({
    status:         'identificada',
    bankMovementId: { $ne: null },
  }).select('_id bankMovementId formasPago.bankMovementId').lean();

  console.log(`Solicitudes identificadas con movimiento vinculado: ${solicitudes.length}`);

  const candidatas = solicitudes.filter(_necesitaBackfill);
  console.log(`Candidatos (con al menos 1 formaPago sin bankMovementId propio): ${candidatas.length}`);

  let actualizados = 0, sinCambio = 0, errores = 0;

  for (const cr of candidatas) {
    console.log(`  ${dryRun ? '[dry-run] ' : ''}solicitud ${cr._id}: formasPago[].bankMovementId ← ${cr.bankMovementId}`);

    try {
      if (!dryRun) {
        const result = await CollectionRequest.updateOne(
          { _id: cr._id },
          { $set: { 'formasPago.$[f].bankMovementId': cr.bankMovementId } },
          { arrayFilters: [{ 'f.bankMovementId': null }] },
        );
        if (result.modifiedCount > 0) actualizados++;
        else sinCambio++; // ya migrada por una corrida concurrente entre el find y este updateOne
      } else {
        actualizados++; // dry-run: cuenta como "sería actualizada"
      }
    } catch (err) {
      errores++;
      console.warn(`  ERROR solicitud ${cr._id}: ${err.message}`);
    }
  }

  console.log(
    `\nBackfill ${dryRun ? '(dry-run) ' : ''}completado. ` +
    `Candidatos: ${candidatas.length} | Actualizados: ${actualizados} | ` +
    `Sin cambio: ${sinCambio} | Errores: ${errores}`,
  );
  await mongoose.disconnect();
}

if (require.main === module) {
  run().catch(err => {
    console.error('Error en el backfill:', err);
    process.exit(1);
  });
}

module.exports = { _necesitaBackfill, run };
