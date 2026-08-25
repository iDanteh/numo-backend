'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const FOLIOS = (process.env.DIAG_FOLIOS || '260800064,260800065').split(',');

async function main() {
  await connectMongo();

  for (const folio of FOLIOS) {
    const cfdis = await CFDI.find({
      $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
      serie: 'C0',
      folio,
    }).select('uuid serie folio tipoDeComprobante fecha total documentosRelacionados cfdiRelacionados receptor.nombre').lean();

    console.log(`\n=== CFDI C0-${folio}: ${cfdis.length} encontrado(s) ===\n`);
    for (const c of cfdis) {
      console.log({
        uuid: c.uuid, tipoDeComprobante: c.tipoDeComprobante, fecha: c.fecha, total: c.total,
        receptor: c.receptor?.nombre, cfdiRelacionados: c.cfdiRelacionados,
      });
      console.log('documentosRelacionados:', JSON.stringify(c.documentosRelacionados, null, 2));
    }
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
