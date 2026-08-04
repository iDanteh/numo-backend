'use strict';

/**
 * fix-periodo-facturas-agosto.js
 * Reclasifica CFDIs (cualquier tipoDeComprobante) de CCO011113663, ejercicio
 * 2026, que quedaron guardados con periodo=8 (agosto) pero cuya fecha real
 * cae en julio 2026 -- se subieron/procesaron en agosto, periodo=7 es el
 * correcto.
 *
 * Por defecto solo cuenta y muestra una muestra (dry-run). Para aplicar el
 * cambio real hay que pasar --apply.
 *
 * Uso:
 *   node src/banks/scripts/fix-periodo-facturas-agosto.js            (dry-run)
 *   node src/banks/scripts/fix-periodo-facturas-agosto.js --apply    (aplica el cambio)
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../../visor/models/CFDI');

const RFC = 'CCO011113663';
const EJERCICIO = 2026;

const apply = process.argv.includes('--apply');

const filtro = {
  $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
  ejercicio: EJERCICIO,
  periodo: 8,
  fecha: {
    $gte: new Date('2026-07-01T00:00:00.000Z'),
    $lt:  new Date('2026-08-01T00:00:00.000Z'),
  },
};

async function main() {
  await connectMongo();

  const total = await CFDI.countDocuments(filtro);
  console.log('CFDIs que coinciden con el criterio (periodo=8 pero fecha en julio): ' + total);

  if (total === 0) {
    console.log('Nada que cambiar.');
    await cerrar();
    return;
  }

  const muestra = await CFDI.find(filtro).select('uuid source tipoDeComprobante fecha periodo ejercicio').limit(5).lean();
  console.log('Muestra de 5:');
  console.log(muestra);

  if (!apply) {
    console.log('\nDRY-RUN: no se modifico nada. Corre con --apply para aplicar el cambio a estos ' + total + ' documentos.');
    await cerrar();
    return;
  }

  const result = await CFDI.updateMany(filtro, { $set: { periodo: 7 } });
  console.log('Documentos modificados: ' + result.modifiedCount);

  await cerrar();
}

async function cerrar() {
  await disconnectMongo();
  process.exit(0);
}

main().catch(function (err) { console.error(err); process.exit(1); });
