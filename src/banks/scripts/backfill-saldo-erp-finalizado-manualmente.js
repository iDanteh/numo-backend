'use strict';

/**
 * banks/scripts/backfill-saldo-erp-finalizado-manualmente.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Rescate puntual para el bug encontrado 2026-09-14 (10 movimientos confirmados en
 * producción, uno de $196,431.39, folio 038309): la rama de finalización de
 * _syncErpKoreJob (erp.routes.js) recalculaba saldoErpAportado de CUALQUIER vínculo
 * humano en cuanto Kore reportaba la CxC cerrada, sin respetar `finalizadoManualmente`
 * (a diferencia de _recomputeErpKoreJob, que ya lo hacía vía _debeRecalcularAporte) —
 * pisando con un 0 falso un saldo ya correcto, fijado por un cobro real aplicado en Numo
 * (cobro-panel/Solicitudes de Cobro). Causa raíz exacta: _montoSaldoLinkPorMovimiento es
 * un acumulador plano por movimiento, sin saber de otros depósitos que compartan la
 * misma CxC — si el kardex de Kore trae una reversión sin tag de identidad cuyo monto
 * coincide por casualidad con lo ya acumulado, neta a 0 exacto aunque el pago sea real
 * (mismo bug documentado 2026-08-21, folioExterno 260800164/260800166).
 *
 * Ya arreglado hacia adelante (ver _aporteParaFinalizacionSync en erp.routes.js, usa el
 * mismo _debeRecalcularAporte que ya tenía _recomputeErpKoreJob); este script solo
 * rescata lo que quedó atrapado con el bug viejo. Ver memoria
 * project_sync_erp_kore_finalizado_manualmente_gap.md para el análisis completo.
 *
 * Alcance: recorre BankMovement con erpIds, encuentra las CxC (erpId) donde algún link
 * tiene saldoErpAportado===0 o saldoPagadoTotal===0 mientras saldoPagado (bancario, ya
 * correcto) tiene un monto real (>1) — el patrón exacto del bug. Para cada CxC afectada,
 * agrupa TODOS los movimientos que hoy la tienen vinculada (mismo criterio que
 * erp-reversion.service.js#procesarReversionKore), reconsulta Kore en vivo UNA vez por
 * CxC, y recalcula el aporte correcto con _aportesPorErpIdCronologico — la MISMA función
 * ya probada que corrige este bug en el flujo de reversiones de Kore, no un cálculo
 * nuevo. Solo escribe erpLinks[].saldoErpAportado/saldoPagadoTotal/saldoPagado y
 * mov.saldoErp/status (recalculado con aplicarLogicaErp(), la fuente de verdad real de
 * toda la app) cuando el aporte recalculado difiere del guardado.
 *
 * Uso:
 *   node src/banks/scripts/backfill-saldo-erp-finalizado-manualmente.js          (dry-run, no escribe nada)
 *   node src/banks/scripts/backfill-saldo-erp-finalizado-manualmente.js --run    (escribe de verdad)
 *
 * Variables de entorno requeridas: MONGODB_URI + lo que necesite Kore (ver Configuraciones
 * Globales / erp-sync.service.js) — correr desde el ambiente real (Test o Producción)
 * donde Kore responda con los datos correctos; un ambiente sin acceso al Kore real de
 * ese folio simplemente no encuentra nada que hacer para él (se salta, no rompe nada).
 */

require('dotenv').config();

const mongoose      = require('mongoose');
const BankMovement  = require('../domains/banks/BankMovement.model');
const ErpReversion  = require('../domains/erp/ErpReversion.model');
const erpRoutes     = require('../domains/erp/erp.routes');
const { aplicarLogicaErp } = require('../domains/banks/bank.service');

const MONGODB_URI   = process.env.MONGODB_URI;
const SYNC_DELAY_MS = erpRoutes.SYNC_DELAY_MS ?? 1000;
const DRY_RUN       = !process.argv.includes('--run');

const _sleep = ms => new Promise(r => setTimeout(r, ms));

function fmtMoney(n) {
  return n == null ? 'null' : n.toFixed(2);
}

async function run() {
  if (!MONGODB_URI) {
    console.error('ERROR: MONGODB_URI no está configurado.');
    process.exit(1);
  }

  await mongoose.connect(MONGODB_URI);
  console.log(`Conectado a MongoDB. Modo: ${DRY_RUN ? 'DRY-RUN (no escribe nada)' : 'RUN (escribe de verdad)'}`);

  const conErpLinks = await BankMovement.find({ erpIds: { $exists: true, $ne: [] } })
    .select('_id folio banco fecha deposito status saldoErp erpLinks identificadoPor numeroAutorizacion')
    .lean();

  const erpIdsAfectados = new Set();
  for (const mov of conErpLinks) {
    for (const l of (mov.erpLinks ?? [])) {
      if ((l.saldoErpAportado === 0 || l.saldoPagadoTotal === 0) && (l.saldoPagado ?? 0) > 1) {
        erpIdsAfectados.add(l.erpId);
      }
    }
  }
  console.log(`CxC (erpId) afectadas encontradas: ${erpIdsAfectados.size}`);

  let totalLinksConCambio = 0;
  let totalMovimientosEscritos = 0;

  for (const erpId of erpIdsAfectados) {
    const movsDelGrupo = await BankMovement.find({ erpIds: erpId }).lean();
    const linkRef = movsDelGrupo.map(m => (m.erpLinks ?? []).find(l => l.erpId === erpId)).find(Boolean);
    if (!linkRef) { console.log(`\nerpId=${erpId}: no se encontró el link de referencia, se salta.`); continue; }

    console.log(`\n=== erpId=${erpId} (${linkRef.serie}-${linkRef.folioExterno}) — grupo de ${movsDelGrupo.length} movimiento(s) ===`);

    const rango = erpRoutes._rangoDesdeFollo(linkRef.folioExterno);
    if (!rango) { console.log('  No se pudo derivar el rango de fecha del folioExterno — se salta.'); continue; }

    let raw0;
    try {
      let { raw } = await erpRoutes._sincronizarConRetry({
        serieExterna: linkRef.serie, folioExterno: String(linkRef.folioExterno),
        fechaDesde: rango.fechaDesde, fechaHasta: rango.fechaHasta,
      });
      if (raw.length === 0) {
        const spillover = erpRoutes._rangoSpilloverSiguienteMes(linkRef.folioExterno);
        if (spillover) {
          await _sleep(SYNC_DELAY_MS);
          const retryRes = await erpRoutes._sincronizarConRetry({
            serieExterna: linkRef.serie, folioExterno: String(linkRef.folioExterno),
            fechaDesde: spillover.fechaDesde, fechaHasta: spillover.fechaHasta,
          });
          if (retryRes.raw.length > 0) raw = retryRes.raw;
        }
      }
      raw0 = raw[0];
    } catch (err) {
      console.log(`  Error consultando Kore: ${err.message} — se salta.`);
      continue;
    }
    if (!raw0) { console.log('  Kore no devolvió datos para este folio (ni con spillover de fin de mes) — se salta.'); continue; }
    await _sleep(SYNC_DELAY_MS);

    const referenciasConocidas = new Set(
      await ErpReversion.find({ erpId, referencia: { $ne: null } }).distinct('referencia'),
    );

    const esHumanoPorMov = movsDelGrupo.map(m => erpRoutes._erpIdIdentificadoPorHumano(m.identificadoPor, erpId));
    const movsHumanos    = movsDelGrupo.filter((_, i) => esHumanoPorMov[i]);

    const aportesTodasFormas = erpRoutes._aportesPorErpIdCronologico(raw0, movsHumanos, () => true, referenciasConocidas);
    const aportesBancario    = erpRoutes._aportesPorErpIdCronologico(
      raw0, movsHumanos, fp => erpRoutes._esFormaPagoBancariaKore(fp.nombreFormaPago), referenciasConocidas,
    );

    for (let i = 0; i < movsDelGrupo.length; i++) {
      if (!esHumanoPorMov[i]) continue;
      const movLean = movsDelGrupo[i];
      const idxEnHumanos = movsHumanos.indexOf(movLean);
      const link = movLean.erpLinks.find(l => l.erpId === erpId);

      const nuevoTodasFormas = aportesTodasFormas.get(idxEnHumanos) ?? null;
      const nuevoBancario    = aportesBancario.get(idxEnHumanos) ?? null;

      const cambiaAporte = nuevoTodasFormas != null
        && Math.abs(nuevoTodasFormas - (link.saldoErpAportado ?? -1)) > 0.01;

      console.log(`\n  mov=${movLean._id} folio=${movLean.folio} banco=${movLean.banco} deposito=${fmtMoney(movLean.deposito)} status actual=${movLean.status} saldoErp actual=${fmtMoney(movLean.saldoErp)}`);
      console.log(`    saldoErpAportado: ${fmtMoney(link.saldoErpAportado)} -> ${fmtMoney(nuevoTodasFormas)}${cambiaAporte ? '   *** CAMBIA ***' : '   (sin cambio)'}`);
      console.log(`    saldoPagadoTotal: ${fmtMoney(link.saldoPagadoTotal)} -> ${fmtMoney(nuevoTodasFormas)}`);
      console.log(`    saldoPagado (bancario, ya era correcto): ${fmtMoney(link.saldoPagado)} -> ${fmtMoney(nuevoBancario)}`);

      if (!cambiaAporte) continue;
      totalLinksConCambio++;

      // Recalcular saldoErp/status del MOVIMIENTO completo con la MISMA función real que
      // usa toda la app (aplicarLogicaErp) — nunca se reimplementa esa prioridad acá.
      const linksActualizados = movLean.erpLinks.map(l =>
        l.erpId === erpId ? { ...l, saldoErpAportado: nuevoTodasFormas, saldoPagadoTotal: nuevoTodasFormas, saldoPagado: nuevoBancario } : l,
      );
      const { saldoErp: saldoErpNuevo, status: statusNuevo } = aplicarLogicaErp({ ...movLean, erpLinks: linksActualizados });

      console.log(`    => movimiento completo: saldoErp ${fmtMoney(movLean.saldoErp)} -> ${fmtMoney(saldoErpNuevo)} | status "${movLean.status}" -> "${statusNuevo}"`);

      if (!DRY_RUN) {
        await BankMovement.updateOne(
          { _id: movLean._id, 'erpLinks.erpId': erpId },
          {
            $set: {
              'erpLinks.$.saldoErpAportado': nuevoTodasFormas,
              'erpLinks.$.saldoPagadoTotal': nuevoTodasFormas,
              'erpLinks.$.saldoPagado':      nuevoBancario,
              saldoErp: saldoErpNuevo,
              status:   statusNuevo,
            },
            $push: {
              _changelog: {
                at: new Date(), via: 'backfill-finalizado-manualmente-2026-09-14', campo: 'saldoErp+status+erpLinks',
                de: { saldoErp: movLean.saldoErp ?? null, status: movLean.status, saldoErpAportado: link.saldoErpAportado ?? null },
                a:  { saldoErp: saldoErpNuevo, status: statusNuevo, saldoErpAportado: nuevoTodasFormas },
                runId: 'backfill-finalizado-manualmente-2026-09-14', revertedAt: null,
              },
            },
          },
        );
        totalMovimientosEscritos++;
        console.log('    >>> ESCRITO en la base de datos.');
      }
    }
  }

  console.log('\n=== RESUMEN ===');
  console.log(`Links con cambio real detectado: ${totalLinksConCambio}`);
  if (!DRY_RUN) console.log(`Movimientos efectivamente actualizados: ${totalMovimientosEscritos}`);
  else console.log('Nada escrito (dry-run) — correr de nuevo con --run para aplicar estos cambios.');

  await mongoose.connection.close();
}

run().catch(err => { console.error(err); process.exit(1); });
