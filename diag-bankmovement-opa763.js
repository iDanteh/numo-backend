'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');

async function main() {
  await connectMongo();

  const porFolioExt = await BankMovement.find({ 'erpLinks.folioExterno': '00763' }).select('fecha concepto erpLinks').lean();
  console.log(`erpLinks.folioExterno=00763: ${porFolioExt.length}`);
  for (const bm of porFolioExt) {
    for (const l of bm.erpLinks) {
      if (l.folioExterno === '00763') console.log(JSON.stringify({ fecha: bm.fecha, concepto: bm.concepto, serie: l.serie, folioExterno: l.folioExterno, total: l.total, folioFiscal: l.folioFiscal }));
    }
  }

  const porSerieOpa = await BankMovement.find({ 'erpLinks.serie': 'OPA' }).select('fecha concepto erpLinks').lean();
  console.log(`\nerpLinks.serie=OPA (cualquier folio): ${porSerieOpa.length}`);
  for (const bm of porSerieOpa) {
    for (const l of bm.erpLinks) {
      if (l.serie === 'OPA') console.log(JSON.stringify({ fecha: bm.fecha, concepto: bm.concepto, serie: l.serie, folioExterno: l.folioExterno, total: l.total, folioFiscal: l.folioFiscal }));
    }
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
