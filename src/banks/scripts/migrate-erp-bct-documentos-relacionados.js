'use strict';

/**
 * migrate-erp-bct-documentos-relacionados.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Resuelve la diferencia de Club Tuberos entre NUMO y ContPAQ:
 *
 * PROBLEMA:
 *   El campo documentosRelacionados en CFDIs del ERP (source='ERP') puede estar
 *   vacío si el registro fue importado antes de que el transformer mapeara
 *   DocumentosRelacionados. Sin ese campo, la detección BCT falla cuando no hay
 *   tipoOrigen='Bonificación Club Tuberos' explícito en el ERP.
 *
 * LO QUE HACE ESTE SCRIPT:
 *   Paso 1 — Diagnóstico: cuenta las 3 categorías de ERP CFDIs:
 *     A) documentosRelacionados tiene Serie='BCT' → ya detectado ✓
 *     B) documentosRelacionados vacío, pero tipoOrigen='Bonificación Club Tuberos'
 *        → detectado via fallback, pero campo incompleto → REPARAR
 *     C) documentosRelacionados vacío Y tipoOrigen sin BCT
 *        → sin indicador alguno → requiere re-import del ERP
 *
 *   Paso 2 — Fix: para los de categoría B, agrega {Serie:'BCT', Folio:''} a
 *     documentosRelacionados para que ambas rutas de detección funcionen.
 *
 *   Paso 3 — Exporta a bct-sin-indicador.json los UUIDs de categoría C para
 *     que el usuario sepa cuáles re-importar desde el ERP.
 *
 * NOTA: No toca CFDIs SAT (source='SAT'). Solo corrige registros ERP.
 * NOTA: Es idempotente; re-ejecutar no duplica entradas BCT.
 *
 * Uso:
 *   node src/banks/scripts/migrate-erp-bct-documentos-relacionados.js
 *   node src/banks/scripts/migrate-erp-bct-documentos-relacionados.js --dry-run
 *   node src/banks/scripts/migrate-erp-bct-documentos-relacionados.js --solo-diagnostico
 */

require('dotenv').config();

const mongoose = require('mongoose');
const path     = require('path');
const fs       = require('fs');
const CFDI     = require('../../visor/models/CFDI');
const config   = require('../../config/env');

const DRY_RUN         = process.argv.includes('--dry-run');
const SOLO_DIAGNOSTICO = process.argv.includes('--solo-diagnostico');
const BATCH           = 200;

// ── Helpers ───────────────────────────────────────────────────────────────────

const tieneBCT = (doc) =>
  Array.isArray(doc.documentosRelacionados) &&
  doc.documentosRelacionados.some(d => d.Serie === 'BCT');

const esBonificacion = (doc) =>
  (doc.tipoOrigen || '').trim() === 'Bonificación Club Tuberos';

// ── Main ──────────────────────────────────────────────────────────────────────

async function run() {
  await mongoose.connect(config.db.uri);
  console.log('MongoDB conectado.\n');

  if (DRY_RUN)          console.log('Modo: DRY-RUN (sin escrituras)\n');
  if (SOLO_DIAGNOSTICO) console.log('Modo: solo diagnóstico\n');

  // ── PASO 1: diagnóstico global ───────────────────────────────────────────

  console.log('── Diagnóstico ERP CFDIs ────────────────────────────────────────────');

  const totalERP = await CFDI.countDocuments({ source: 'ERP' });

  const conBCT = await CFDI.countDocuments({
    source: 'ERP',
    'documentosRelacionados.Serie': 'BCT',
  });

  const sinBCTConTipoOrigen = await CFDI.countDocuments({
    source:     'ERP',
    tipoOrigen: 'Bonificación Club Tuberos',
    'documentosRelacionados.Serie': { $ne: 'BCT' },
  });

  const sinAmbos = await CFDI.countDocuments({
    source: 'ERP',
    'documentosRelacionados.Serie': { $ne: 'BCT' },
    $or: [
      { tipoOrigen: { $ne: 'Bonificación Club Tuberos' } },
      { tipoOrigen: { $exists: false } },
    ],
  });

  console.log(`  Total CFDIs ERP:                   ${totalERP}`);
  console.log(`  A) Con documentosRelacionados BCT: ${conBCT}   (detectados ✓)`);
  console.log(`  B) Solo tipoOrigen Bonificación:   ${sinBCTConTipoOrigen}   (fallback OK, campo incompleto → reparar)`);
  console.log(`  C) Sin ningún indicador BCT:       ${sinAmbos}   (gap — requiere re-import ERP)\n`);

  if (SOLO_DIAGNOSTICO) {
    await mongoose.disconnect();
    return;
  }

  // ── PASO 2: reparar categoría B ──────────────────────────────────────────

  if (sinBCTConTipoOrigen === 0) {
    console.log('── Fix: nada que reparar en categoría B.\n');
  } else {
    console.log(`── Fix: reparando ${sinBCTConTipoOrigen} CFDIs ERP de categoría B ──────────────────`);

    const cursor = CFDI.find({
      source:     'ERP',
      tipoOrigen: 'Bonificación Club Tuberos',
      'documentosRelacionados.Serie': { $ne: 'BCT' },
    })
      .select('_id uuid documentosRelacionados tipoOrigen')
      .lean()
      .cursor({ batchSize: BATCH });

    let reparados = 0;
    let bulk = [];

    for await (const erp of cursor) {
      // Asegurar que documentosRelacionados es array antes de empujar
      const drActual = Array.isArray(erp.documentosRelacionados)
        ? erp.documentosRelacionados
        : [];

      // Solo agregar si no existe ya (idempotente)
      if (!drActual.some(d => d.Serie === 'BCT')) {
        const drNuevo = [...drActual, { Serie: 'BCT', Folio: '' }];

        bulk.push({
          updateOne: {
            filter: { _id: erp._id },
            update: { $set: { documentosRelacionados: drNuevo } },
          },
        });

        if (bulk.length >= BATCH) {
          if (!DRY_RUN) {
            await CFDI.bulkWrite(bulk, { ordered: false });
          }
          reparados += bulk.length;
          process.stdout.write(`\r  Reparados: ${reparados}   `);
          bulk = [];
        }
      }
    }

    if (bulk.length > 0) {
      if (!DRY_RUN) {
        await CFDI.bulkWrite(bulk, { ordered: false });
      }
      reparados += bulk.length;
    }

    process.stdout.write(`\r  Reparados: ${reparados}   \n`);
    console.log(DRY_RUN ? '  (DRY-RUN: sin escrituras)\n' : '  ✓ documentosRelacionados BCT escrito en todos los de categoría B.\n');
  }

  // ── PASO 3: exportar NCs ERP sin indicador BCT (candidatos al gap) ──────

  console.log('── Exportando NCs ERP sin indicador BCT (categoría C candidatos) ────');

  // Sólo exportamos tipo 'E' (notas de crédito) porque son los más probables
  // candidatos a ser bonificaciones BCT que no se detectaron.
  // El gap en 2103090002 es principalmente de NCs/bonificaciones, no facturas.
  const sinIndicadorNCs = await CFDI.find({
    source:             'ERP',
    tipoDeComprobante:  'E',
    'documentosRelacionados.Serie': { $ne: 'BCT' },
    $or: [
      { tipoOrigen: { $ne: 'Bonificación Club Tuberos' } },
      { tipoOrigen: { $exists: false } },
    ],
  })
    .select('uuid fecha serie folio tipoOrigen')
    .lean();

  const totalSinIndicador = sinIndicadorNCs.length;

  if (totalSinIndicador > 0) {
    const outPath = path.join(
      __dirname,
      '../../../../',
      'bct-nc-sin-indicador.json',
    );
    const data = {
      generado:   new Date().toISOString(),
      descripcion: 'NCs ERP (tipo E) sin documentosRelacionados BCT ni tipoOrigen=Bonificación. ' +
                   'Cruzar con cuenta 2103090002 de ContPAQ: los que aparezcan allá necesitan re-import.',
      total:      totalSinIndicador,
      registros:  sinIndicadorNCs.map(c => ({
        uuid:             c.uuid,
        fecha:            c.fecha,
        serie:            c.serie,
        folio:            c.folio,
        tipoOrigenActual: c.tipoOrigen ?? null,
      })),
    };
    fs.writeFileSync(outPath, JSON.stringify(data, null, 2), 'utf8');
    console.log(`  ${totalSinIndicador} NCs ERP sin indicador → ${outPath}`);
    console.log('  Cruza con los movimientos 2103090002 de ContPAQ para identificar cuáles re-importar.\n');
  } else {
    console.log('  Sin NCs ERP sin indicador BCT. Gap no proviene de NCs sin datos.\n');
  }

  // ── Resumen ───────────────────────────────────────────────────────────────

  console.log('── Resumen ──────────────────────────────────────────────────────────');
  console.log(`  Total ERP:       ${totalERP}`);
  console.log(`  Con BCT (prev):  ${conBCT}`);
  console.log(`  Reparados (B):   ${sinBCTConTipoOrigen}`);
  console.log(`  Gap (C):         ${totalSinIndicador} → requiere re-import ERP`);
  if (DRY_RUN) console.log('\n  DRY-RUN: ningún documento fue modificado.');
  else         console.log('\n  ✓ Regenera la balanza en NUMO para ver el efecto.');
  console.log('─────────────────────────────────────────────────────────────────────');

  await mongoose.disconnect();
  process.exit(0);
}

run().catch(err => {
  console.error('\nError fatal:', err.message);
  process.exit(1);
});
