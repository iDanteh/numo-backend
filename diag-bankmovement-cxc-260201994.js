'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');

async function main() {
  await connectMongo();

  // Busqueda amplia: 260201994 en CUALQUIER campo de erpLinks o del documento.
  const porRegex = await BankMovement.find({
    $or: [
      { 'erpLinks.folioExterno': /260201994/ },
      { 'erpLinks.folioFiscal': /260201994/ },
      { 'erpLinks.serie': /260201994/ },
      { 'erpLinks.referencia': /260201994/ },
      { concepto: /260201994/ },
      { referencia: /260201994/ },
    ],
  }).lean();
  console.log(`Encontrados por regex amplio: ${porRegex.length}`);
  for (const bm of porRegex) console.log(JSON.stringify(bm));

  // Ver estructura completa de un erpLink de ejemplo para saber que campos tiene.
  const ejemplo = await BankMovement.findOne({ 'erpLinks.0': { $exists: true } }).select('erpLinks').lean();
  console.log('Ejemplo de estructura erpLinks:', JSON.stringify(ejemplo, null, 2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
