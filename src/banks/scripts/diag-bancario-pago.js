'use strict';

/**
 * diag-bancario-pago.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Diagnóstico puntual: dado un CFDI de Pago (serie+folio o uuid), muestra qué
 * trae `construirVerdadBancaria` (poliza.service.js) para él — si hay un
 * BankMovement ligado por erpLinks.folioFiscal, qué categoría/banco/monto
 * trae, o si simplemente no hay ningún match. Ayuda a distinguir "el sistema
 * no encontró nada" de "encontró algo pero sin categoría confirmada".
 *
 * Uso:
 *   node src/banks/scripts/diag-bancario-pago.js <rfc> <serie> <folio>
 *   node src/banks/scripts/diag-bancario-pago.js <rfc> --uuid <uuid>
 */
require('dotenv').config();
const mongoose = require('mongoose');
const CFDI = require('../../visor/models/CFDI');
const BankMovement = require('../domains/banks/BankMovement.model');
const { _construirVerdadBancaria } = require('../domains/polizas/poliza.service');

async function main() {
  const [rfc, a, b] = process.argv.slice(2);
  if (!rfc || !a) {
    console.error('Uso: node diag-bancario-pago.js <rfc> <serie> <folio>');
    console.error('  o: node diag-bancario-pago.js <rfc> --uuid <uuid>');
    process.exit(1);
  }

  await mongoose.connect(process.env.MONGODB_URI);

  let cfdi;
  if (a === '--uuid') {
    cfdi = await CFDI.findOne({ uuid: b }).lean();
  } else {
    cfdi = await CFDI.findOne({ 'emisor.rfc': rfc, serie: a, folio: String(b), tipoDeComprobante: 'P', source: 'SAT' }).lean();
  }

  if (!cfdi) {
    console.error('No se encontró el CFDI de Pago con esos datos.');
    process.exit(1);
  }

  console.log('--- CFDI de Pago ---');
  console.log('uuid:', cfdi.uuid);
  console.log('serie-folio:', cfdi.serie, cfdi.folio);
  console.log('formaPago (header):', cfdi.formaPago);
  console.log('formaDePagoP (complemento):', cfdi.complementoPago?.pagos?.[0]?.formaDePagoP);
  console.log('doctosRelacionados:', (cfdi.complementoPago?.pagos ?? []).flatMap(p => p.doctosRelacionados ?? []).map(d => `${d.serie}-${d.folio} $${d.impPagado}`));

  console.log('\n--- BankMovement ligados por erpLinks.folioFiscal (case-insensitive) ---');
  const movs = await BankMovement.find({
    'erpLinks.folioFiscal': new RegExp(`^${cfdi.uuid}$`, 'i'),
  }).lean();
  if (!movs.length) {
    console.log('NINGUNO — no hay ningún BankMovement ligado a este UUID. El sistema no tiene forma de saber a qué banco/folio corresponde.');
  } else {
    for (const m of movs) {
      console.log(JSON.stringify({
        _id: m._id, banco: m.banco, fecha: m.fecha, deposito: m.deposito,
        categoria: m.categoria, numeroAutorizacion: m.numeroAutorizacion,
        referenciaNumerica: m.referenciaNumerica, erpLinks: m.erpLinks,
      }, null, 2));
    }
  }

  console.log('\n--- Resultado de construirVerdadBancaria() para este CFDI ---');
  const verdad = await _construirVerdadBancaria([{ cfdiUuid: cfdi.uuid, serie: `${cfdi.serie}-${cfdi.folio}` }], rfc);
  const info = verdad.get(cfdi.uuid.toUpperCase());
  console.log(info ?? 'undefined (sin match en absoluto)');

  // El match por erpLinks.folioFiscal se hace contra el UUID del PAGO — pero
  // la conciliación bancaria se armó originalmente para Ingreso, así que el
  // depósito real bien podría estar ligado al UUID de la FACTURA que este
  // Pago liquida, no al del Pago mismo. Se revisa cada doctoRelacionado.
  const doctos = (cfdi.complementoPago?.pagos ?? []).flatMap(p => p.doctosRelacionados ?? []);
  for (const d of doctos) {
    console.log(`\n--- Factura liquidada ${d.serie}-${d.folio} ($${d.impPagado}) ---`);
    const facturaCfdi = await CFDI.findOne({ 'emisor.rfc': rfc, serie: d.serie, folio: String(d.folio) }).lean();
    if (!facturaCfdi) {
      console.log('No se encontró el CFDI de esta factura en Mongo.');
      continue;
    }
    console.log('uuid de la factura:', facturaCfdi.uuid);
    const movsFactura = await BankMovement.find({
      'erpLinks.folioFiscal': new RegExp(`^${facturaCfdi.uuid}$`, 'i'),
    }).lean();
    if (!movsFactura.length) {
      console.log('NINGUNO ligado al UUID de la factura tampoco.');
    } else {
      for (const m of movsFactura) {
        console.log('¡ENCONTRADO ligado a la FACTURA, no al Pago!', JSON.stringify({
          _id: m._id, banco: m.banco, fecha: m.fecha, deposito: m.deposito,
          categoria: m.categoria, numeroAutorizacion: m.numeroAutorizacion,
          referenciaNumerica: m.referenciaNumerica, erpLinks: m.erpLinks,
        }, null, 2));
      }
    }
  }

  // Última red: cualquier BankMovement con depósito de monto muy similar al
  // total del Pago, cerca de la fecha del CFDI — para descartar que el
  // movimiento exista pero sin NINGÚN erpLinks poblado (ni al Pago ni a la
  // factura), lo que apuntaría a un problema de sincronización más amplio.
  const totalPago = Number(cfdi.complementoPago?.totales?.montoTotalPagos
    ?? doctos.reduce((s, d) => s + Number(d.impPagado || 0), 0));
  if (totalPago > 0) {
    console.log(`\n--- BankMovement con depósito ≈ $${totalPago.toFixed(2)} (±$1), sin filtrar por erpLinks ---`);
    const candidatos = await BankMovement.find({
      deposito: { $gte: totalPago - 1, $lte: totalPago + 1 },
    }).limit(10).lean();
    if (!candidatos.length) {
      console.log('NINGUNO — no existe ningún BankMovement con ese monto en absoluto.');
    } else {
      for (const m of candidatos) {
        console.log(JSON.stringify({ _id: m._id, banco: m.banco, fecha: m.fecha, deposito: m.deposito, categoria: m.categoria, erpLinks: m.erpLinks }, null, 2));
      }

      // Cada candidato trae su propio erpLinks.folioFiscal, que puede NO ser
      // ni el del Pago ni el de la factura buscada por serie/folio. Se resuelve
      // qué CFDI es realmente ese UUID en Mongo (podría ser un SUSTITUTO de la
      // factura original — misma operación, UUID distinto tras cancelación) y
      // si trae cfdiRelacionados apuntando de vuelta a la factura original.
      console.log('\n--- ¿A qué CFDI corresponde el erpLinks.folioFiscal de cada candidato? ---');
      const uuidsVistos = new Set();
      for (const m of candidatos) {
        for (const link of (m.erpLinks || [])) {
          const uuidLink = link.folioFiscal;
          if (!uuidLink || uuidsVistos.has(uuidLink.toUpperCase())) continue;
          uuidsVistos.add(uuidLink.toUpperCase());
          const cfdiLink = await CFDI.findOne({ uuid: new RegExp(`^${uuidLink}$`, 'i') }).lean();
          if (!cfdiLink) {
            console.log(`UUID ${uuidLink}: no se encontró ningún CFDI con ese uuid en Mongo.`);
            // El UUID del erpLinks no existe, pero el link también trae
            // serie/folioExterno del ERP. Se busca si HOY existe un CFDI con
            // esa misma serie+folio pero OTRO uuid — indicaría que la factura
            // fue cancelada/sustituida y el registro se sobrescribió in-place.
            for (const l of (m.erpLinks || [])) {
              if (l.folioFiscal !== uuidLink) continue;
              if (!l.serie || !l.folioExterno) continue;
              const actual = await CFDI.findOne({ 'emisor.rfc': rfc, serie: l.serie, folio: String(l.folioExterno) }).lean();
              if (actual) {
                console.log(`  Hoy existe un CFDI con esa misma serie-folio (${l.serie}-${l.folioExterno}) pero uuid distinto: ${actual.uuid} (estatus=${actual.estatus ?? actual.status ?? '?'}) -> probable sustituto/reemplazo in-place.`);
              } else {
                console.log(`  Tampoco existe ningún CFDI con serie-folio ${l.serie}-${l.folioExterno} en Mongo.`);
              }
            }
            continue;
          }
          console.log(`UUID ${uuidLink} => CFDI ${cfdiLink.tipoDeComprobante} ${cfdiLink.serie}-${cfdiLink.folio}, estatus=${cfdiLink.estatus ?? cfdiLink.status ?? '?'}`);
          const relacionados = cfdiLink.cfdiRelacionados ?? cfdiLink.CfdiRelacionados ?? [];
          if (relacionados.length) {
            console.log('  cfdiRelacionados:', JSON.stringify(relacionados));
          }
          if (String(cfdiLink.uuid).toUpperCase() !== String(cfdi.uuid).toUpperCase()) {
            const esRelacionadoDirecto = (Array.isArray(relacionados) ? relacionados : [])
              .some(r => String(r.UUID || r.uuid || '').toUpperCase() === String(doctos[0]?.uuid || '').toUpperCase());
            console.log(esRelacionadoDirecto ? '  -> SÍ referencia directamente a la factura buscada (sustituto confirmado).' : '  -> revisar manualmente si es sustituto de la factura buscada.');
          }
        }
      }
    }
  }

  await mongoose.disconnect();
}

main().catch(e => { console.error(e); process.exit(1); });
