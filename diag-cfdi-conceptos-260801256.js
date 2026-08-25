'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

async function main() {
  await connectMongo();
  const cfdi = await CFDI.findOne({ serie: 'B0', folio: '260801256', source: 'SAT' }).lean();
  console.log('Campos de nivel superior:', Object.keys(cfdi));
  console.log('\nconceptos:', JSON.stringify(cfdi.conceptos, null, 2).slice(0, 3000));
  console.log('\ndocumentosRelacionados:', JSON.stringify(cfdi.documentosRelacionados));
  console.log('\ncomplemento (si existe):', JSON.stringify(cfdi.complemento)?.slice(0, 1000));
  console.log('\ninformacionGlobal:', JSON.stringify(cfdi.informacionGlobal));
  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
