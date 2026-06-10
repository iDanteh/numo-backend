'use strict';

/**
 * migrate-cp10-traslados-dr.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Infiere la tasa IVA para CFDIs tipo P con Complemento de Pago 1.0 (sin
 * <Totales>) que no tienen xmlContent (descargados con versión anterior del
 * parser). Guarda el resultado en cfdi.tasaIvaInferida para que _detectTasaIva
 * lo use como fallback y clasifique en la cuenta contable correcta.
 *
 * Estrategia:
 *   1. Recoger los UUIDs de cfdiRelacionados
 *   2. Buscar esas facturas originales en MongoDB (tipo I/E — datos SAT)
 *   3. Calcular tasa dominante de sus conceptos
 *   4. Guardar tasaIvaInferida = '16' | '0' | 'mixto' | null
 *
 * Nota: el fallback ERP (erp_cuentas_pendientes) se aplica en tiempo de
 * generación de balanza/balance (balanza-preliminar.service.js) para no
 * ensuciar el documento CFDI original con datos del ERP.
 *
 * Uso:
 *   node src/banks/scripts/migrate-cp10-traslados-dr.js
 *   node src/banks/scripts/migrate-cp10-traslados-dr.js --dry-run
 */

require('dotenv').config();

const mongoose = require('mongoose');
const CFDI     = require('../../visor/models/CFDI');
const config   = require('../../config/env');

const DRY_RUN = process.argv.includes('--dry-run');
const BATCH   = 100;

// ── Detecta tasa IVA de una factura tipo I/E desde sus conceptos ──────────────
function tasaDesdeConceptos(cfdi) {
  let tiene16 = false;
  let tiene0  = false;

  for (const c of (cfdi.conceptos || [])) {
    for (const t of (c.impuestos?.traslados || [])) {
      if ((t.impuesto || '') !== '002') continue;
      if ((t.tasaOCuota || 0) > 0) tiene16 = true;
      else tiene0 = true;
    }
  }

  // Fallback a totalImpuestosTrasladados del header
  if (!tiene16 && !tiene0) {
    const tot = cfdi.impuestos?.totalImpuestosTrasladados;
    if (tot != null) {
      if (tot > 0) tiene16 = true;
      else tiene0 = true;
    }
  }

  if (tiene16 && tiene0) return 'mixto';
  if (tiene16) return '16';
  if (tiene0)  return '0';
  return null;
}

// ── Combina tasas de múltiples facturas relacionadas ─────────────────────────
function combinartasas(tasas) {
  const validas = tasas.filter(Boolean);
  if (!validas.length) return null;
  const tiene16  = validas.some(t => t === '16' || t === 'mixto');
  const tiene0   = validas.some(t => t === '0'  || t === 'mixto');
  if (tiene16 && tiene0) return 'mixto';
  if (tiene16) return '16';
  if (tiene0)  return '0';
  return null;
}

async function run() {
  await mongoose.connect(config.db.uri);
  console.log('MongoDB conectado.');
  if (DRY_RUN) console.log('Modo: DRY-RUN (sin escrituras)\n');

  const filtro = {
    tipoDeComprobante: 'P',
    source:            'SAT',
    'complementoPago': { $exists: false },
    'cfdiRelacionados.0': { $exists: true },
  };

  const total = await CFDI.countDocuments(filtro);
  console.log(`CFDIs Metadata con cfdiRelacionados: ${total}\n`);

  let procesados    = 0;
  let actualizados  = 0;
  let sinDoctos     = 0;
  let sinFacturas   = 0;
  let tasaNula      = 0;
  const distribucion = { '16': 0, '0': 0, mixto: 0 };

  const cursor = CFDI.find(filtro)
    .select('uuid cfdiRelacionados tasaIvaInferida')
    .lean()
    .cursor({ batchSize: BATCH });

  const bulk = [];

  for await (const cfdi of cursor) {
    procesados++;

    // Los UUIDs relacionados pueden venir pipe-separados en una sola entrada
    // p.ej. "UUID1 | UUID2 | UUID3" — dividir y normalizar
    const uuids = (cfdi.cfdiRelacionados ?? [])
      .flatMap(r => r.uuids ?? [])
      .flatMap(u => u.split(/\s*\|\s*/))
      .map(u => u.trim().toUpperCase())
      .filter(u => u.length >= 32); // descartar basura

    if (!uuids.length) {
      sinDoctos++;
      _progreso(procesados, total, actualizados, sinDoctos, sinFacturas, tasaNula);
      continue;
    }

    // Buscar facturas originales (tipo I o E) en MongoDB
    const facturas = await CFDI.find(
      { uuid: { $in: uuids }, tipoDeComprobante: { $in: ['I', 'E'] } },
      { conceptos: 1, impuestos: 1, tipoDeComprobante: 1 },
    ).lean();

    if (!facturas.length) {
      sinFacturas++;
      _progreso(procesados, total, actualizados, sinDoctos, sinFacturas, tasaNula);
      continue;
    }

    const tasasRelacionadas = facturas.map(tasaDesdeConceptos);
    const tasaInferida      = combinartasas(tasasRelacionadas);

    if (tasaInferida === null) {
      tasaNula++;
      _progreso(procesados, total, actualizados, sinDoctos, sinFacturas, tasaNula);
      continue;
    }

    distribucion[tasaInferida] = (distribucion[tasaInferida] || 0) + 1;

    if (!DRY_RUN) {
      bulk.push({
        updateOne: {
          filter: { _id: cfdi._id },
          update: { $set: { tasaIvaInferida: tasaInferida } },
        },
      });

      if (bulk.length >= BATCH) {
        await CFDI.bulkWrite(bulk, { ordered: false });
        actualizados += bulk.length;
        bulk.length = 0;
      }
    } else {
      actualizados++;
    }

    _progreso(procesados, total, actualizados, sinDoctos, sinFacturas, tasaNula);
  }

  if (bulk.length > 0) {
    await CFDI.bulkWrite(bulk, { ordered: false });
    actualizados += bulk.length;
  }

  _progreso(procesados, total, actualizados, sinDoctos, sinFacturas, tasaNula);
  console.log('\n\n─────────────────────────────────────────────');
  console.log(`Total procesados:         ${procesados}`);
  console.log(`Actualizados:             ${actualizados}`);
  console.log(`  tasa 16%:               ${distribucion['16']}`);
  console.log(`  tasa 0%:                ${distribucion['0']}`);
  console.log(`  mixto:                  ${distribucion['mixto'] || 0}`);
  console.log(`Sin doctosRelacionados:   ${sinDoctos}`);
  console.log(`Facturas orig. no found:  ${sinFacturas}  (→ fallback ERP en balanza)`);
  console.log(`Tasa no determinable:     ${tasaNula}`);
  if (DRY_RUN) console.log('\n(DRY-RUN: ningún documento fue modificado)');
  console.log('─────────────────────────────────────────────');

  await mongoose.disconnect();
  process.exit(0);
}

function _progreso(proc, total, act, sinD, sinF, nula) {
  const pct = ((proc / total) * 100).toFixed(1);
  process.stdout.write(
    `\r  ${proc}/${total} (${pct}%) | actualizados: ${act} | sin doctos: ${sinD} | sin facturas: ${sinF} | tasa null: ${nula}   `
  );
}

run().catch((err) => {
  console.error('\nError fatal:', err);
  process.exit(1);
});
