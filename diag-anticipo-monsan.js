'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

async function main() {
  await connectMongo();
  const cfdi = await CFDI.findOne({ uuid: 'EDDCAB96-E49A-4742-A2F4-953799CD7EC0', source: 'SAT' }).lean();
  console.log('CFDI SAT:', JSON.stringify({
    uuid: cfdi?.uuid, serie: cfdi?.serie, folio: cfdi?.folio, total: cfdi?.total,
    cfdiRelacionados: cfdi?.cfdiRelacionados,
  }, null, 2));

  const cfdiErp = await CFDI.findOne({ uuid: 'EDDCAB96-E49A-4742-A2F4-953799CD7EC0', source: 'ERP' }).lean();
  console.log('CFDI ERP:', JSON.stringify({
    uuid: cfdiErp?.uuid, tipoOrigen: cfdiErp?.tipoOrigen,
    cfdiRelacionados: cfdiErp?.cfdiRelacionados,
    documentosRelacionados: cfdiErp?.documentosRelacionados,
  }, null, 2));

  if (cfdi?.cfdiRelacionados?.length) {
    for (const r of cfdi.cfdiRelacionados) {
      const uuids = r.uuids ?? (r.uuid ? [r.uuid] : []);
      for (const u of uuids) {
        const rel = await CFDI.findOne({ uuid: u }).select('uuid serie folio total tipoDeComprobante source').lean();
        console.log(`Relacionado tipoRelacion=${r.tipoRelacion} uuid=${u}:`, JSON.stringify(rel));
      }
    }
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
