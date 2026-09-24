'use strict';

/**
 * banks/scripts/backfill-anticipo-erp-link.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Backfill puntual para los AnticipoGenerado que ya quedaron correlacionados
 * (correlacionAutomatica:true, solicitudCobroId seteado) ANTES de que existiera
 * _vincularAnticipoAlDeposito() (anticipo-generado.service.js) — su erpLink en
 * el BankMovement correspondiente nunca se creó, aunque la correlación en sí ya
 * era correcta. Caso real que lo disparó: OPA-00370 (ONESIMO ANTONIO RENDON
 * ILESCAS), correlacionado por reconciliarAnticiposPendientes() antes de que
 * esta funcionalidad existiera.
 *
 * reconciliarAnticiposPendientes() NO va a volver a tocar estos documentos por
 * su cuenta — su query excluye explícitamente correlacionAutomatica:true (ver
 * VENTANA_RECONCILIACION_MS en anticipo-generado.service.js) — por eso hace
 * falta este backfill de una sola vez, no un cron.
 *
 * Idempotente / re-ejecutable: solo se consideran "candidatos" los anticipos
 * con AL MENOS un BankMovement que todavía no tiene un erpLink con ese
 * anticipoIdErp — uno ya vinculado (por este mismo script o por el flujo
 * normal) no se vuelve a tocar.
 *
 * Uso:
 *   node src/banks/scripts/backfill-anticipo-erp-link.js          (dry-run, no escribe nada)
 *   node src/banks/scripts/backfill-anticipo-erp-link.js --run    (escribe de verdad)
 *
 * Variable de entorno requerida: MONGODB_URI
 */

require('dotenv').config();

const mongoose = require('mongoose');
const AnticipoGenerado = require('../domains/collection-requests/AnticipoGenerado.model');
const CollectionRequest = require('../domains/collection-requests/CollectionRequest.model');
const BankMovement = require('../domains/banks/BankMovement.model');
const { _vincularAnticipoAlDeposito } = require('../domains/collection-requests/anticipo-generado.service');

// _necesitaBackfill — pura: ¿algún BankMovement de este anticipo TODAVÍA no
// tiene un erpLink con su anticipoIdErp? Recibe los movimientos YA cargados
// (bankMovements) para no acoplar esta función a hacer su propia consulta.
function _necesitaBackfill(anticipo, bankMovements) {
  return bankMovements.some(mov => !(mov.erpLinks || []).some(l => l.erpId === anticipo.anticipoIdErp));
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

  const anticipos = await AnticipoGenerado.find({
    correlacionAutomatica: true,
    solicitudCobroId: { $ne: null },
  }).lean();

  console.log(`Anticipos correlacionados: ${anticipos.length}`);

  let vinculados = 0, sinCambio = 0, errores = 0;

  for (const anticipo of anticipos) {
    try {
      const bankMovementIds = anticipo.bankMovementIds ?? [];
      if (bankMovementIds.length === 0) { sinCambio++; continue; }

      // eslint-disable-next-line no-await-in-loop
      const bankMovements = await BankMovement.find({ _id: { $in: bankMovementIds } }).select('erpLinks').lean();

      if (!_necesitaBackfill(anticipo, bankMovements)) {
        console.log(`  [ya vinculado]   anticipo ${anticipo.anticipoIdErp}`);
        sinCambio++;
        continue;
      }

      console.log(`  ${dryRun ? '[dry-run] ' : '[vincular]'} anticipo ${anticipo.anticipoIdErp} → movimiento(s) ${bankMovementIds.join(', ')}`);

      if (!dryRun) {
        // eslint-disable-next-line no-await-in-loop
        const cr = await CollectionRequest.findById(anticipo.solicitudCobroId).lean();
        if (!cr) {
          console.warn(`  ERROR anticipo ${anticipo.anticipoIdErp}: solicitudCobroId ${anticipo.solicitudCobroId} ya no existe`);
          errores++;
          continue;
        }
        // eslint-disable-next-line no-await-in-loop
        await _vincularAnticipoAlDeposito(anticipo, cr);
      }
      vinculados++;
    } catch (err) {
      errores++;
      console.warn(`  ERROR anticipo ${anticipo.anticipoIdErp}: ${err.message}`);
    }
  }

  console.log(
    `\nBackfill ${dryRun ? '(dry-run) ' : ''}completado. ` +
    `Candidatos: ${anticipos.length} | Vinculados: ${vinculados} | ` +
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
