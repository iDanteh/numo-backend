'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'C0';

async function main() {
  await connectMongo();

  const cfdis = await CFDI.find({
    'emisor.rfc': RFC, serie: SERIE, tipoDeComprobante: 'I', source: 'ERP',
    fecha: { $gte: new Date('2026-08-13T00:00:00-06:00'), $lte: new Date('2026-08-13T23:59:59-06:00') },
  }).select('uuid serie folio total fecha documentosRelacionados').lean();

  console.log(`Total CFDIs tipo I de ${SERIE} el 13-ago:`, cfdis.length);
  for (const c of cfdis) {
    const ndocs = (c.documentosRelacionados ?? []).filter(d => d.Serie && d.Folio).length;
    console.log({ uuid: c.uuid, folio: c.folio, total: c.total, fecha: c.fecha, tickets: ndocs });
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
