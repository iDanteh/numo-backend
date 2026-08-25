'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const FOLIOS = (process.env.DIAG_FOLIOS || '260800064,260800065').split(',');

async function main() {
  await connectMongo();

  for (const folio of FOLIOS) {
    const cfdi = await CFDI.findOne({
      $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
      serie: 'C0', folio, source: 'ERP',
    }).select('uuid serie folio total cfdiRelacionados documentosRelacionados').lean();
    if (!cfdi) { console.log(`C0-${folio}: no encontrado (source ERP)`); continue; }

    console.log(`\n=== CFDI C0-${folio} (${cfdi.uuid}), total ${cfdi.total} ===`);
    console.log('cfdiRelacionados:', JSON.stringify(cfdi.cfdiRelacionados, null, 2));

    const uuidsRel = (cfdi.cfdiRelacionados ?? []).flatMap(r => r.uuids ?? []);
    for (const uuidRel of uuidsRel) {
      const rel = await CFDI.findOne({ uuid: uuidRel }).select('uuid serie folio folioSustitucion tipoDeComprobante total fecha documentosRelacionados metodoPago formaPago').lean();
      console.log(`\n--- CFDI relacionado ${uuidRel} ---`);
      console.log(rel ? {
        serie: rel.serie, folio: rel.folio, tipoDeComprobante: rel.tipoDeComprobante,
        total: rel.total, fecha: rel.fecha, metodoPago: rel.metodoPago, formaPago: rel.formaPago,
        documentosRelacionados: rel.documentosRelacionados,
      } : 'NO ENCONTRADO EN MONGO');
    }
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
