'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'B0';
const FOLIO = process.env.DIAG_FOLIO || '260801159';

async function main() {
  await connectMongo();

  const versiones = await CFDI.find({ 'emisor.rfc': RFC, serie: SERIE, folio: FOLIO }).lean();
  console.log(`Total versiones (todas las fuentes) del folio ${SERIE}-${FOLIO}:`, versiones.length);
  for (const v of versiones) {
    console.log(JSON.stringify({
      uuid: v.uuid, source: v.source, satStatus: v.satStatus, isActive: v.isActive,
      tipoDeComprobante: v.tipoDeComprobante, total: v.total, fecha: v.fecha,
      fechaCancelacion: v.fechaCancelacion, motivoCancelacion: v.motivoCancelacion,
      fechaTimbrado: v.fechaTimbrado,
    }, null, 2));
  }

  // Buscar si existe algun CFDI (NC o sustituto) que la referencie via cfdiRelacionados.
  const uuids = versiones.map(v => v.uuid);
  const relacionados = await CFDI.find({
    'emisor.rfc': RFC,
    'cfdiRelacionados.uuid': { $in: uuids },
  }).select('uuid serie folio tipoDeComprobante total cfdiRelacionados satStatus source').lean();
  console.log(`\nCFDIs que referencian este UUID via cfdiRelacionados:`, relacionados.length);
  for (const r of relacionados) console.log(JSON.stringify(r, null, 2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
