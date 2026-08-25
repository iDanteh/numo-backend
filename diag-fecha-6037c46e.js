'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

async function main() {
  await connectMongo();
  const cfdi = await CFDI.findOne({ uuid: '6037C46E-387E-4684-BA9D-885F702E4A52', source: 'SAT' }).select('uuid fecha total folio').lean();
  console.log('CFDI 6037C46E fecha:', JSON.stringify(cfdi));
  console.log('BankMovement OPA-00763 fecha: 2026-07-28T00:00:00.000Z');
  if (cfdi?.fecha) {
    const diffDias = (new Date(cfdi.fecha) - new Date('2026-07-28T00:00:00.000Z')) / 86400000;
    console.log('Diferencia en dias:', diffDias);
  }
  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
