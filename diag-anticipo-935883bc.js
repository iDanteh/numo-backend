'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

async function main() {
  await connectMongo();

  const u1 = await CFDI.find({ uuid: '935883BC-27E8-413B-B285-D52DBA42EE80' }).select('uuid source serie folio total tipoDeComprobante').lean();
  console.log('935883BC (todas las fuentes):', JSON.stringify(u1));

  const u2 = await CFDI.find({ uuid: '6037C46E-387E-4684-BA9D-885F702E4A52' }).select('uuid source serie folio total tipoDeComprobante').lean();
  console.log('6037C46E (todas las fuentes):', JSON.stringify(u2));

  const porFolio = await CFDI.find({ folio: '260201994' }).select('uuid source serie folio total tipoDeComprobante emisor.rfc receptor.nombre').lean();
  console.log('Folio 260201994 (cualquier serie):', JSON.stringify(porFolio));

  const porFolio2 = await CFDI.find({ folio: '260703205' }).select('uuid source serie folio total tipoDeComprobante emisor.rfc receptor.nombre').lean();
  console.log('Folio 260703205 (cualquier serie):', JSON.stringify(porFolio2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
