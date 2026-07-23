'use strict';

/**
 * banks/scripts/recompute-saldo-erp-todas-formas-pago.js
 * ─────────────────────────────────────────────────────────────────────────────
 * NOTA: esta lógica (y la de migrate-erp-movimientoskore-formaspago.js) ya vive
 * también en el botón "Recalcular saldo ERP" del panel Admin (una sola consulta
 * a Kore por link, con checkpoint propio vía recomputedFormasPagoAt — ver
 * erp.routes.js#_recomputeErpKoreJob). Este script se conserva para uso desde
 * CLI fuera de la app; para operación normal, usar el botón.
 *
 * Recalcula saldoErpAportado/saldoErp/status de erpLinks YA finalizados
 * (conciliacionFinalizadaAt !== null) e identificados por un HUMANO, aplicando
 * el criterio nuevo de _montoSaldoLink (erp.routes.js): suma TODAS las formas
 * de pago de la CxC, no solo transferencia/cheque/depósito en efectivo —
 * decisión del usuario, 2026-07-17. Antes de ese cambio, un depósito bancario
 * real pagado con una forma de pago que Kore no clasificaba como "bancaria"
 * (ej. "EFECTIVO" en vez de "DEPOSITO EN EFECTIVO") quedaba con
 * saldoErpAportado=0 y el movimiento en no_identificado pese a ser un depósito
 * legítimo. Esos links ya están finalizados (el job normal nunca los vuelve a
 * tocar), así que sin este backfill se quedarían mal para siempre.
 *
 * Vínculos de MOTOR (erp-auto/aut-match) se dejan intactos — su cálculo
 * (_montoSaldoLinkPorAutorizacion, por número de autorización bancaria) no
 * cambió con esta decisión.
 *
 * Reescribe erpLinks completo del movimiento (mismo patrón que
 * _syncErpKoreJob, no arrayFilters) SOLO si algún link humano cambia de valor.
 * conciliacionFinalizadaAt se deja tal cual (el link sigue "cerrado"), pero
 * conciliacionRunId se actualiza al runId de esta corrida en los links que
 * cambiaron — y saldoErp/status quedan auditados en _changelog con
 * via:'erp-sync', el MISMO shape que ya usa el job normal. Esto es a propósito:
 * si algo sale mal, esta corrida se puede revertir con el endpoint que YA
 * existe, sin tooling nuevo:
 *   POST /api/erp/sync-erp-kore/<runId impreso al final>/revert
 *
 * Uso:
 *   node src/banks/scripts/recompute-saldo-erp-todas-formas-pago.js [--dry-run]
 *
 * --dry-run: no escribe nada en Mongo, solo reporta qué cambiaría.
 *
 * Variables de entorno requeridas: MONGODB_URI, ERP_CAJA_BASE_URL, ERP_TOKEN
 */

require('dotenv').config();

const mongoose          = require('mongoose');
const BankMovement      = require('../domains/banks/BankMovement.model');
const erpRoutes         = require('../domains/erp/erp.routes');
const { ERP_TOLERANCE } = require('../domains/banks/bank.service');

const MONGODB_URI  = process.env.MONGODB_URI;
const SYNC_DELAY_MS = erpRoutes.SYNC_DELAY_MS ?? 1000;
const DRY_RUN       = process.argv.includes('--dry-run');
const RUN_ID        = `recompute-todas-formas-${Date.now()}`;

const _sleep = ms => new Promise(r => setTimeout(r, ms));

async function run() {
  if (!MONGODB_URI) {
    console.error('ERROR: MONGODB_URI no está configurado.');
    process.exit(1);
  }

  await mongoose.connect(MONGODB_URI);
  console.log(`Conectado a MongoDB. runId=${RUN_ID}${DRY_RUN ? ' — DRY RUN, no se escribe nada' : ''}`);

  const movements = await BankMovement.find({
    erpLinks: { $elemMatch: { conciliacionFinalizadaAt: { $ne: null } } },
  }).select('_id folio deposito retiro ficha saldoErp status erpLinks identificadoPor').lean();

  console.log(`Movimientos con al menos un link finalizado: ${movements.length}`);

  let linksRevisados = 0, linksCambiados = 0, movimientosActualizados = 0, errores = 0;

  for (const mov of movements) {
    const linksActualizados = mov.erpLinks.map(l => ({ ...l }));
    let huboLinkCambiado = false;

    for (let i = 0; i < linksActualizados.length; i++) {
      const link = linksActualizados[i];
      if (!link.conciliacionFinalizadaAt || !link.serie || !link.folioExterno) continue;

      // Vínculos de motor: sin cambios, su cálculo (por autorización) no se tocó.
      const esHumano = erpRoutes._erpIdIdentificadoPorHumano(mov.identificadoPor, link.erpId);
      if (!esHumano) continue;

      const rango = erpRoutes._rangoDesdeFollo(link.folioExterno);
      if (!rango) continue;

      linksRevisados++;
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
        if (!raw0) {
          console.warn(`  Sin datos en Kore: erpId=${link.erpId} folioExterno=${link.folioExterno} (mov ${mov.folio ?? mov._id})`);
          await _sleep(SYNC_DELAY_MS);
          continue;
        }

        const aporteNuevo = erpRoutes._montoSaldoLink(raw0);
        const aporteViejo = link.saldoErpAportado ?? 0;

        if (Math.abs(aporteNuevo - aporteViejo) > 0.01) {
          console.log(`  CAMBIO erpId=${link.erpId} folioExterno=${link.folioExterno} (mov ${mov.folio ?? mov._id}): ${aporteViejo} → ${aporteNuevo}`);
          linksActualizados[i] = {
            ...link,
            saldoErpAportado:  aporteNuevo,
            movimientosKore:   erpRoutes._movimientosKoreDesde(raw0),
            conciliacionRunId: RUN_ID,
          };
          huboLinkCambiado = true;
          linksCambiados++;
        }
      } catch (err) {
        errores++;
        console.warn(`  ERROR erpId=${link.erpId} folioExterno=${link.folioExterno} (mov ${mov.folio ?? mov._id}): ${err.message}`);
      }

      await _sleep(SYNC_DELAY_MS);
    }

    if (!huboLinkCambiado) continue;

    // Mismo criterio de recálculo que _syncErpKoreJob (erp.routes.js): solo si
    // hay algún aporte determinado, respetando la excepción de `ficha` física.
    const hayAlgunAporteDeterminado = linksActualizados.some(l => l.saldoErpAportado != null);
    if (!hayAlgunAporteDeterminado) continue;

    const saldoErpNuevo = linksActualizados.reduce((s, l) => s + (l.saldoErpAportado ?? 0), 0);
    const bankAmount    = Math.abs(mov.deposito ?? mov.retiro ?? 0);
    let statusNuevo     = saldoErpNuevo >= bankAmount - ERP_TOLERANCE ? 'identificado' : 'no_identificado';
    if (mov.ficha && statusNuevo === 'no_identificado') statusNuevo = 'identificado';

    if (saldoErpNuevo === (mov.saldoErp ?? null) && statusNuevo === mov.status) continue;

    console.log(`MOVIMIENTO ${mov.folio ?? mov._id}: saldoErp ${mov.saldoErp} → ${saldoErpNuevo}, status ${mov.status} → ${statusNuevo}`);
    movimientosActualizados++;

    if (DRY_RUN) continue;

    await BankMovement.updateOne(
      { _id: mov._id },
      {
        $set: { erpLinks: linksActualizados, saldoErp: saldoErpNuevo, status: statusNuevo },
        $push: {
          _changelog: {
            at: new Date(), via: 'erp-sync', campo: 'saldoErp+status',
            de: { saldoErp: mov.saldoErp ?? null, status: mov.status },
            a:  { saldoErp: saldoErpNuevo, status: statusNuevo },
            runId: RUN_ID, revertedAt: null,
          },
        },
      },
    );
  }

  console.log(`\nCompletado. Links revisados (humanos, ya finalizados): ${linksRevisados} | Cambiados: ${linksCambiados} | Movimientos actualizados: ${movimientosActualizados} | Errores: ${errores}`);
  if (!DRY_RUN && movimientosActualizados > 0) {
    console.log(`\nPara revertir esta corrida completa (usa el MISMO endpoint que el sync normal):`);
    console.log(`  POST /api/erp/sync-erp-kore/${RUN_ID}/revert`);
  }
  await mongoose.disconnect();
}

run().catch(err => {
  console.error('Error en el recompute:', err);
  process.exit(1);
});
