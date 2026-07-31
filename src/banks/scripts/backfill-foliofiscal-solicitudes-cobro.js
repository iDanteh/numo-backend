'use strict';

/**
 * banks/scripts/backfill-foliofiscal-solicitudes-cobro.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Rescate puntual para el bug encontrado 2026-07-31 (folio 038139 y cualquier
 * otro con el mismo síntoma): buildErpLinksParaCobro()
 * (collection-request-erp-links.js) guardaba erpLinks[].folioFiscal a partir
 * de la foto de CollectionRequest.cxcs tomada al CREAR la solicitud — si Kore
 * timbraba el CFDI DESPUÉS de crearla pero ANTES de autorizarla, ese
 * folioFiscal quedaba null para siempre. Ya arreglado hacia adelante (ver
 * identificar() paso 2b + buildErpLinksParaCobro() en
 * collection-request-erp-links.js); este script solo rescata lo que quedó
 * atrapado con el bug viejo — nunca toca un link con folioFiscal ya resuelto.
 *
 * Distinto de migrate-erp-movimientoskore-formaspago.js /
 * recompute-saldo-erp-todas-formas-pago.js: esos cubren links de conciliación
 * MANUAL (conciliacionFinalizadaAt !== null, por eso los toma el botón
 * "Recalcular saldo ERP"). Los links creados por Solicitudes de Cobro
 * (identificar()) NUNCA tocan ese campo, así que quedan completamente fuera
 * del alcance de ese botón — de ahí que hiciera falta este script aparte.
 *
 * Alcance: recorre CollectionRequest con status 'identificada' y
 * bankMovementId — para cada cxc cuyo LINK en el movimiento (no cr.cxcs, que
 * es la foto vieja) sigue sin folioFiscal, reconsulta Kore
 * (/cuentas-pendientes, mismo endpoint/helper que ya usan los otros scripts de
 * este dominio) acotado al folioExterno/serie de esa CxC. Solo escribe
 * erpLinks[].folioFiscal por link individual (arrayFilters) y, si el
 * movimiento seguía sin uuidXML, lo completa — nunca toca saldoErp, status,
 * saldoPagado(Total) ni ningún otro campo/link del movimiento.
 *
 * Uso:
 *   node src/banks/scripts/backfill-foliofiscal-solicitudes-cobro.js             (dry-run, no escribe nada)
 *   node src/banks/scripts/backfill-foliofiscal-solicitudes-cobro.js --run       (escribe de verdad)
 *
 * Variables de entorno requeridas: MONGODB_URI, ERP_CAJA_BASE_URL, ERP_TOKEN
 */

require('dotenv').config();

const mongoose          = require('mongoose');
const CollectionRequest = require('../domains/collection-requests/CollectionRequest.model');
const BankMovement      = require('../domains/banks/BankMovement.model');
const erpRoutes         = require('../domains/erp/erp.routes');

const MONGODB_URI   = process.env.MONGODB_URI;
const SYNC_DELAY_MS = erpRoutes.SYNC_DELAY_MS ?? 1000;
const DRY_RUN       = !process.argv.includes('--run');

const _sleep = ms => new Promise(r => setTimeout(r, ms));

async function run() {
  if (!MONGODB_URI) {
    console.error('ERROR: MONGODB_URI no está configurado.');
    process.exit(1);
  }

  await mongoose.connect(MONGODB_URI);
  console.log(`Conectado a MongoDB. Modo: ${DRY_RUN ? 'DRY-RUN (no escribe nada)' : 'RUN (escribe de verdad)'}`);

  const solicitudes = await CollectionRequest.find({
    status:         'identificada',
    bankMovementId: { $ne: null },
  }).select('_id bankMovementId cxcs').lean();

  console.log(`Solicitudes identificadas con movimiento vinculado: ${solicitudes.length}`);

  let candidatos = 0, actualizados = 0, sinCambio = 0, errores = 0;

  for (const cr of solicitudes) {
    const mov = await BankMovement.findById(cr.bankMovementId).select('_id folio uuidXML erpLinks');
    if (!mov) {
      console.warn(`  Sin BankMovement para solicitud ${cr._id} (bankMovementId=${cr.bankMovementId})`);
      continue;
    }

    for (const cxc of cr.cxcs) {
      const link = mov.erpLinks.find(l => l.erpId === cxc.erpId);
      if (!link || link.folioFiscal) continue; // ya resuelto, o no vinculado (no debería pasar)

      const rango = erpRoutes._rangoDesdeFollo(cxc.folioExterno);
      if (!rango) {
        console.warn(`  folioExterno con formato inesperado: erpId=${cxc.erpId} folioExterno=${cxc.folioExterno} (mov ${mov.folio})`);
        continue;
      }

      candidatos++;
      try {
        const { raw } = await erpRoutes._sincronizarConRetry({
          serieExterna: cxc.serie, folioExterno: String(cxc.folioExterno),
          fechaDesde: rango.fechaDesde, fechaHasta: rango.fechaHasta,
        });
        const encontrada = raw.find(c =>
          String(c.folioExterno) === String(cxc.folioExterno) &&
          (!cxc.serie || String(c.serieExterna) === String(cxc.serie)),
        );

        if (encontrada?.folioFiscal) {
          const folioFiscalNorm = String(encontrada.folioFiscal).trim().toUpperCase();
          console.log(`  ${DRY_RUN ? '[dry-run] ' : ''}mov ${mov.folio} erpId=${cxc.erpId}: folioFiscal → ${folioFiscalNorm}`);
          if (!DRY_RUN) {
            await BankMovement.updateOne(
              { _id: mov._id },
              { $set: { 'erpLinks.$[elem].folioFiscal': folioFiscalNorm } },
              { arrayFilters: [{ 'elem.erpId': cxc.erpId }] },
            );
            if (!mov.uuidXML) {
              await BankMovement.updateOne({ _id: mov._id, uuidXML: null }, { $set: { uuidXML: folioFiscalNorm } });
              mov.uuidXML = folioFiscalNorm; // evita pisarlo dos veces si el mismo mov tiene más de una CxC en esta corrida
            }
          }
          actualizados++;
        } else {
          sinCambio++;
        }
      } catch (err) {
        errores++;
        console.warn(`  ERROR erpId=${cxc.erpId} folioExterno=${cxc.folioExterno} (mov ${mov.folio}): ${err.message}`);
      }

      await _sleep(SYNC_DELAY_MS);
    }
  }

  console.log(
    `\nBackfill ${DRY_RUN ? '(dry-run) ' : ''}completado. ` +
    `Candidatos: ${candidatos} | ${DRY_RUN ? 'Recuperables ahora' : 'Actualizados'}: ${actualizados} | ` +
    `Sin folioFiscal todavía en Kore: ${sinCambio} | Errores: ${errores}`,
  );
  await mongoose.disconnect();
}

run().catch(err => {
  console.error('Error en el backfill:', err);
  process.exit(1);
});
