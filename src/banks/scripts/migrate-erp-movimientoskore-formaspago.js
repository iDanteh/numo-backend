'use strict';

/**
 * banks/scripts/migrate-erp-movimientoskore-formaspago.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Backfill idempotente: refresca erpLinks[].movimientosKore (que ahora incluye
 * formasPago[], ver erp.routes.js#_movimientosKoreDesde) en links que YA
 * quedaron finalizados (conciliacionFinalizadaAt !== null) ANTES de que se
 * agregara ese campo — el job de sync normal salta para siempre cualquier
 * link finalizado, así que sin este backfill esos links nunca reciben el dato
 * nuevo.
 *
 * SOLO escribe erpLinks[].movimientosKore, por link individual (arrayFilters,
 * nunca reescribe el arreglo completo) — jamás toca conciliacionFinalizadaAt,
 * saldoErpAportado, saldoErp, status, tieneRetencion, montoRetenido, ni ningún
 * otro link del mismo movimiento. Cero riesgo sobre conciliación ya cerrada.
 *
 * Idempotente: salta cualquier link cuyo movimientosKore ya tenga al menos una
 * entrada con formasPago poblado (ya migrado en una corrida previa).
 *
 * Uso:
 *   node src/banks/scripts/migrate-erp-movimientoskore-formaspago.js
 *
 * Variables de entorno requeridas: MONGODB_URI, ERP_CAJA_BASE_URL, ERP_TOKEN
 */

require('dotenv').config();

const mongoose     = require('mongoose');
const BankMovement = require('../domains/banks/BankMovement.model');
const erpRoutes     = require('../domains/erp/erp.routes');

const MONGODB_URI  = process.env.MONGODB_URI;
const SYNC_DELAY_MS = erpRoutes.SYNC_DELAY_MS ?? 1000;

const _sleep = ms => new Promise(r => setTimeout(r, ms));

async function run() {
  if (!MONGODB_URI) {
    console.error('ERROR: MONGODB_URI no está configurado.');
    process.exit(1);
  }

  await mongoose.connect(MONGODB_URI);
  console.log('Conectado a MongoDB.');

  const movements = await BankMovement.find({
    erpLinks: {
      $elemMatch: {
        serie:                    { $ne: null },
        folioExterno:             { $ne: null },
        conciliacionFinalizadaAt: { $ne: null },
      },
    },
  }).select('_id folio erpLinks').lean();

  console.log(`Movimientos con al menos un link finalizado: ${movements.length}`);

  let linksProcesados = 0, linksActualizados = 0, linksOmitidos = 0, errores = 0;

  for (const mov of movements) {
    for (const link of mov.erpLinks) {
      if (!link.serie || !link.folioExterno || !link.conciliacionFinalizadaAt) continue;

      const yaMigrado = (link.movimientosKore ?? []).some(
        m => Array.isArray(m.formasPago) && m.formasPago.length > 0,
      );
      if (yaMigrado) { linksOmitidos++; continue; }

      const rango = erpRoutes._rangoDesdeFollo(link.folioExterno);
      if (!rango) { linksOmitidos++; continue; }

      linksProcesados++;
      try {
        let { raw } = await erpRoutes._sincronizarConRetry({
          serieExterna: link.serie,
          folioExterno: String(link.folioExterno),
          fechaDesde:   rango.fechaDesde,
          fechaHasta:   rango.fechaHasta,
        });

        if (raw.length === 0) {
          const spillover = erpRoutes._rangoSpilloverSiguienteMes(link.folioExterno);
          if (spillover) {
            await _sleep(SYNC_DELAY_MS);
            const retryRes = await erpRoutes._sincronizarConRetry({
              serieExterna: link.serie,
              folioExterno: String(link.folioExterno),
              fechaDesde:   spillover.fechaDesde,
              fechaHasta:   spillover.fechaHasta,
            });
            if (retryRes.raw.length > 0) raw = retryRes.raw;
          }
        }

        const raw0 = raw[0];
        if (raw0) {
          // Actualización quirúrgica: solo movimientosKore de ESTE link, vía
          // arrayFilters — nunca se lee/reescribe el arreglo erpLinks completo,
          // así que no hay riesgo de pisar un cambio concurrente en otro link
          // del mismo movimiento (ej. un cobro aplicándose en paralelo).
          await BankMovement.updateOne(
            { _id: mov._id },
            { $set: { 'erpLinks.$[elem].movimientosKore': erpRoutes._movimientosKoreDesde(raw0) } },
            { arrayFilters: [{ 'elem.erpId': link.erpId }] },
          );
          linksActualizados++;
        } else {
          console.warn(`  Sin datos en Kore: erpId=${link.erpId} folioExterno=${link.folioExterno} (mov ${mov.folio ?? mov._id})`);
        }
      } catch (err) {
        errores++;
        console.warn(`  ERROR erpId=${link.erpId} folioExterno=${link.folioExterno} (mov ${mov.folio ?? mov._id}): ${err.message}`);
      }

      await _sleep(SYNC_DELAY_MS);

      if (linksProcesados % 20 === 0) {
        console.log(`  Procesados: ${linksProcesados} | actualizados: ${linksActualizados} | omitidos: ${linksOmitidos} | errores: ${errores}`);
      }
    }
  }

  console.log(`\nBackfill completado. Procesados: ${linksProcesados} | Actualizados: ${linksActualizados} | Omitidos (ya migrados): ${linksOmitidos} | Errores: ${errores}`);
  await mongoose.disconnect();
}

run().catch(err => {
  console.error('Error en el backfill:', err);
  process.exit(1);
});
