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

  await mongoose.disconnect();
}

main().catch(e => { console.error(e); process.exit(1); });
