'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');

async function main() {
  await connectMongo();
  const bm = await BankMovement.find({ 'erpLinks.folioExterno': '260201994' }).select('fecha monto concepto erpLinks').lean();
  console.log('BankMovement con erpLinks.folioExterno=260201994:', JSON.stringify(bm, null, 2));

  const bm2 = await BankMovement.find({ 'erpLinks.folioExterno': '260703205' }).select('fecha monto concepto erpLinks').lean();
  console.log('BankMovement con erpLinks.folioExterno=260703205:', JSON.stringify(bm2, null, 2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
