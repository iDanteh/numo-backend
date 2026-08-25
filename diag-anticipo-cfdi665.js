'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

async function main() {
  await connectMongo();

  const cfdis = await CFDI.find({ serie: 'C0', folio: '260701665' })
    .select('uuid serie folio source tipoDeComprobante total fecha formaPago metodoPago documentosRelacionados cfdiRelacionados')
    .lean();

  console.log(`Total registros para C0-260701665: ${cfdis.length}\n`);
  for (const c of cfdis) {
    console.log('---', c.uuid, '(source:', c.source, ') ---');
    console.log({
      tipoDeComprobante: c.tipoDeComprobante, total: c.total, fecha: c.fecha,
      formaPago: c.formaPago, metodoPago: c.metodoPago,
    });
    console.log('documentosRelacionados:', JSON.stringify(c.documentosRelacionados, null, 2));
    console.log('cfdiRelacionados:', JSON.stringify(c.cfdiRelacionados, null, 2));
    console.log();
  }

  // Tambien buscar cualquier CFDI cuyo documentosRelacionados mencione "OPA"
  console.log('=== Buscando cualquier CFDI con documentosRelacionados.Serie = OPA (rango amplio) ===');
  const conOPA = await CFDI.find({ 'documentosRelacionados.Serie': 'OPA' })
    .select('uuid serie folio tipoDeComprobante total fecha documentosRelacionados')
    .limit(10)
    .lean();
  console.log(`Encontrados: ${conOPA.length}`);
  for (const c of conOPA) {
    console.log({ uuid: c.uuid, serie: c.serie, folio: c.folio, tipo: c.tipoDeComprobante, total: c.total, fecha: c.fecha });
    console.log('documentosRelacionados:', JSON.stringify(c.documentosRelacionados));
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
