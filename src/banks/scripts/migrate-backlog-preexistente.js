'use strict';

/**
 * banks/scripts/migrate-backlog-preexistente.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Migración de UNA SOLA CORRIDA: estampa `BankMovement.backlogPreExistente = true`
 * en todos los movimientos que YA estaban pendientes (status no_identificado o
 * reclasificado — BACKLOG_STATUSES en bank-indicadores.service.js) en el momento del
 * deploy real del split "backlog histórico / backlog nuevo" del indicador de
 * tiempo de identificación (ver bank-indicadores.service.js).
 *
 * Por qué una marca fija en vez de comparar `createdAt` contra una fecha de
 * corte dinámica: una fecha de corte se corrompe con reimportaciones tardías
 * de Excels viejos (createdAt quedaría después del corte para un movimiento
 * que en realidad es histórico), y un movimiento revertido a
 * `no_identificado` después del corte debe seguir contando como "histórico"
 * si ya existía antes — no como "nuevo". Una marca estampada una sola vez, en
 * el momento real del deploy, no tiene ninguno de esos dos problemas.
 *
 * ⚠️ CRÍTICO — correr UNA SOLA VEZ, exactamente en el momento del deploy real
 * a producción de este split. NO antes, NO "para probar" en producción real
 * (correrla en local/dev para probar la feature está bien). Volver a
 * correrla en producción después de la primera vez corrompería la marca:
 * movimientos que ya pasaron a "nuevo" legítimamente (identificados y
 * revertidos después del deploy real) quedarían marcados como "histórico".
 * Por eso el script trae un guard de seguridad que ABORTA automáticamente si
 * detecta que ya hay documentos marcados — no hay bandera para forzarlo.
 *
 * Idempotente solo en el sentido de "no destructivo si se corre 2 veces
 * seguidas sin escribir nada en el medio" (el guard lo impide activamente);
 * NO es re-ejecutable a propósito, a diferencia de otros scripts de este
 * directorio.
 *
 * Uso — local (lee .env del repo vía dotenv):
 *   node src/banks/scripts/migrate-backlog-preexistente.js          (dry-run, no escribe nada)
 *   node src/banks/scripts/migrate-backlog-preexistente.js --run    (escribe de verdad)
 *
 * Uso — producción (contenedor Docker "numo-backend", mismo patrón que el resto de
 * scripts de este directorio, ej. migrate-bank-config-saldo.js):
 *   docker exec numo-backend node src/banks/scripts/migrate-backlog-preexistente.js
 *   docker exec numo-backend node src/banks/scripts/migrate-backlog-preexistente.js --run
 *
 * Variable de entorno requerida: MONGODB_URI
 */

require('dotenv').config();

const mongoose      = require('mongoose');
const BankMovement  = require('../domains/banks/BankMovement.model');

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

  // Guard de seguridad crítico: si ya hay documentos marcados, esta migración
  // ya se corrió antes — abortar sin tocar nada, nunca continuar automáticamente.
  const yaMarcados = await BankMovement.countDocuments({ backlogPreExistente: true });
  if (yaMarcados > 0) {
    console.error(
      `ERROR: Esta migración ya se corrió antes (${yaMarcados} documentos ya marcados con ` +
      'backlogPreExistente:true) — volver a correrla corrompería la marca del backlog ' +
      'histórico. Si estás seguro de que necesitás re-correrla, hacelo manualmente ' +
      'entendiendo las consecuencias.',
    );
    await mongoose.disconnect();
    process.exit(1);
    return;
  }

  const BACKLOG_STATUSES = ['no_identificado', 'reclasificado'];

  const candidatos = await BankMovement.countDocuments({
    status: { $in: BACKLOG_STATUSES },
    backlogPreExistente: { $ne: true },
  });
  console.log(`Candidatos (status en [${BACKLOG_STATUSES.join(', ')}], aún sin marcar): ${candidatos}`);

  let modificados = 0;
  if (!dryRun) {
    const result = await BankMovement.updateMany(
      { status: { $in: BACKLOG_STATUSES }, backlogPreExistente: { $ne: true } },
      { $set: { backlogPreExistente: true } },
    );
    modificados = result.modifiedCount ?? 0;
  } else {
    modificados = candidatos; // dry-run: cuenta como "se marcarían"
  }

  console.log(
    `\nMigración ${dryRun ? '(dry-run) ' : ''}completada. ` +
    `Candidatos: ${candidatos} | Marcados como histórico: ${modificados}`,
  );
  await mongoose.disconnect();
}

if (require.main === module) {
  run().catch(err => {
    console.error('Error en la migración:', err);
    process.exit(1);
  });
}

module.exports = { run };
