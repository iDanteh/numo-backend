'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

const UUID = '89CF6A7F-8925-43ED-9DFB-DFFB72993245';

async function main() {
  await connectMongo();
  const docs = await CFDI.find({ uuid: UUID }).lean();
  console.log('Total documentos con este uuid:', docs.length);
  for (const d of docs) {
    console.log(JSON.stringify({
      _id: d._id, uuid: d.uuid, serie: d.serie, folio: d.folio, source: d.source,
      total: d.total, receptorNombre: d.receptor?.nombre, tipoDeComprobante: d.tipoDeComprobante,
      metodoPago: d.metodoPago, formaPago: d.formaPago,
      documentosRelacionadosCount: (d.documentosRelacionados ?? []).length,
    }, null, 2));
  }
  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
