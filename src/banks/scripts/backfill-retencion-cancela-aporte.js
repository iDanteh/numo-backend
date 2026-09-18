'use strict';

/**
 * banks/scripts/backfill-retencion-cancela-aporte.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Rescate puntual para el bug encontrado 2026-09-18 (folio 038309, $196,431.71,
 * reportado por el usuario tras re-vincular manualmente y ver que _syncErpKoreJob lo
 * volvía a resetear a no_identificado/saldoErp:0 en cada corrida): _montoSaldoLinkPorMovimiento
 * y _aportesPorErpIdCronologico (erp.routes.js) trataban una línea del kardex de Kore SIN
 * tag de identidad (Numo/Aut/Num Recibo) que resuelve una retención (ej. "SALDO A FAVOR")
 * como una reversa que cancela el abono real, cuando su monto coincidía por casualidad con
 * lo ya acumulado — un abono real tageado quedaba en $0.
 *
 * Ya arreglado hacia adelante (_esResolucionDeRetencionGenuina, ver erp.routes.js): una
 * línea sin tag cuyo monto coincide con una línea de RETENCIÓN GENUINA (formasPago vacío,
 * mismo criterio que _retencionVigente) del mismo kardex ya no cancela nada. Este script
 * rescata lo que quedó atrapado con el bug viejo.
 *
 * A diferencia de backfill-saldo-erp-finalizado-manualmente.js (2026-09-14), este script
 * NO necesita pegarle a Kore — el fix es puramente de cálculo sobre datos que YA están
 * guardados en erpLinks[].movimientosKore (el snapshot de la última corrida de
 * _syncErpKoreJob). Alcance: usa _montoSaldoLinkPorMovimiento (no la variante cronológica
 * entre-movimientos) porque los 10 casos conocidos de este bug son cada uno un ÚNICO
 * erpLink humano sobre su propio movimiento — no hay evidencia de una CxC compartida entre
 * 2+ BankMovement en este lote; si apareciera un caso así en el futuro, revisar si hace
 * falta agrupar como sí hace el backfill de 2026-09-14.
 *
 * Alcance: BankMovement con isActive/status:'no_identificado'/saldoErp:0 y al menos un
 * erpLink de un vínculo HUMANO (mismo criterio que _erpIdIdentificadoPorHumano) con
 * movimientosKore guardado. Recalcula con la función YA CORREGIDA — si el nuevo valor es
 * mayor al actual, escribe erpLinks[].saldoErpAportado/saldoPagadoTotal y
 * mov.saldoErp/status (recalculado con aplicarLogicaErp(), la fuente de verdad real de
 * toda la app). Si sigue en null/0 (ej. los ciclos reales ABO→RAB→ABO, folios
 * 042623/041140/042622 — una reversión real, no relacionada con retención), lo reporta
 * como "sin cambio" y no lo toca.
 *
 * Uso:
 *   node src/banks/scripts/backfill-retencion-cancela-aporte.js          (dry-run, no escribe nada)
 *   node src/banks/scripts/backfill-retencion-cancela-aporte.js --run    (escribe de verdad)
 */

require('dotenv').config();

const mongoose      = require('mongoose');
const BankMovement  = require('../domains/banks/BankMovement.model');
const erpRoutes     = require('../domains/erp/erp.routes');
const { aplicarLogicaErp } = require('../domains/banks/bank.service');

const MONGODB_URI = process.env.MONGODB_URI;
const DRY_RUN      = !process.argv.includes('--run');

function fmtMoney(n) {
  return n == null ? 'null' : n.toFixed(2);
}

// Reconstruye la forma `raw0.movimientos[]` que espera _montoSaldoLinkPorMovimiento a
// partir del snapshot guardado (erpLinks[].movimientosKore) — mismos campos que la función
// realmente lee (formasPago[].adicionales, total), no hace falta reconsultar Kore.
function raw0DesdeSnapshot(movimientosKore) {
  return {
    movimientos: (movimientosKore ?? []).map(m => ({
      total: m.total,
      formasPago: (m.formasPago ?? []).map(fp => ({ adicionales: fp.adicionales ?? [], monto: fp.monto })),
    })),
  };
}

async function run() {
  if (!MONGODB_URI) {
    console.error('ERROR: MONGODB_URI no está configurado.');
    process.exit(1);
  }

  await mongoose.connect(MONGODB_URI);
  console.log(`Conectado a MongoDB. Modo: ${DRY_RUN ? 'DRY-RUN (no escribe nada)' : 'RUN (escribe de verdad)'}`);

  const candidatos = await BankMovement.find({
    isActive: true,
    status: 'no_identificado',
    saldoErp: 0,
    erpLinks: { $exists: true, $ne: [] },
  }).lean();

  console.log(`Movimientos no_identificado/saldoErp=0 con al menos un erpLink: ${candidatos.length}`);

  let revisados = 0;
  let cambiaron = 0;
  let sinCambio = 0;

  for (const mov of candidatos) {
    for (const link of mov.erpLinks ?? []) {
      const esHumano = (mov.identificadoPor ?? []).some(ip => ip.erpId === link.erpId && ip.userId);
      if (!esHumano || !link.movimientosKore?.length) continue;
      revisados++;

      const raw0 = raw0DesdeSnapshot(link.movimientosKore);
      const nuevoAporte = erpRoutes._montoSaldoLinkPorMovimiento(raw0, mov);

      const actual = link.saldoErpAportado ?? null;
      const cambia = nuevoAporte != null && (actual == null || nuevoAporte > actual + 0.01);

      console.log(`\nmov=${mov._id} folio=${mov.folio} banco=${mov.banco} depósito=${fmtMoney(mov.deposito)} erpId=${link.erpId} folioExterno=${link.folioExterno}`);
      console.log(`  saldoErpAportado: ${fmtMoney(actual)} -> ${fmtMoney(nuevoAporte)}${cambia ? '   *** CAMBIA ***' : '   (sin cambio)'}`);

      if (!cambia) { sinCambio++; continue; }
      cambiaron++;

      const linksActualizados = mov.erpLinks.map(l =>
        l.erpId === link.erpId ? { ...l, saldoErpAportado: nuevoAporte, saldoPagadoTotal: nuevoAporte } : l,
      );
      const { saldoErp: saldoErpNuevo, status: statusNuevo } = aplicarLogicaErp({ ...mov, erpLinks: linksActualizados });

      console.log(`  => movimiento completo: saldoErp ${fmtMoney(mov.saldoErp)} -> ${fmtMoney(saldoErpNuevo)} | status "${mov.status}" -> "${statusNuevo}"`);

      if (!DRY_RUN) {
        await BankMovement.updateOne(
          { _id: mov._id, 'erpLinks.erpId': link.erpId },
          {
            $set: {
              'erpLinks.$.saldoErpAportado': nuevoAporte,
              'erpLinks.$.saldoPagadoTotal': nuevoAporte,
              saldoErp: saldoErpNuevo,
              status:   statusNuevo,
            },
            $push: {
              _changelog: {
                at: new Date(), via: 'backfill-retencion-cancela-aporte-2026-09-18', campo: 'saldoErp+status+erpLinks',
                campos: [], importFile: null,
                de: { saldoErp: mov.saldoErp ?? null, status: mov.status, saldoErpAportado: actual },
                a:  { saldoErp: saldoErpNuevo, status: statusNuevo, saldoErpAportado: nuevoAporte },
                runId: 'backfill-retencion-cancela-aporte-2026-09-18', revertedAt: null,
              },
            },
          },
        );
        console.log('  >>> ESCRITO en la base de datos.');
      }
    }
  }

  console.log('\n=== RESUMEN ===');
  console.log(`Links revisados: ${revisados}`);
  console.log(`Cambiaron: ${cambiaron}`);
  console.log(`Sin cambio (ej. ciclos ABO→RAB→ABO, reversión real no relacionada con retención): ${sinCambio}`);
  if (DRY_RUN) console.log('Nada escrito (dry-run) — correr de nuevo con --run para aplicar estos cambios.');

  await mongoose.connection.close();
}

run().catch(err => { console.error(err); process.exit(1); });
