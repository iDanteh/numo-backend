'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');
const mongoose = require('mongoose');

async function main() {
  await connectMongo();

  console.log('Coleccion real:', BankMovement.collection.collectionName);

  // Buscar el string 260201994 en TODO el documento, sin asumir estructura.
  const db = mongoose.connection.db;
  const coll = db.collection(BankMovement.collection.collectionName);

  const porTexto = await coll.find({ $where: 'JSON.stringify(this).includes("260201994")' }).limit(5).toArray();
  console.log(`Por $where (texto libre): ${porTexto.length}`);
  for (const d of porTexto) console.log(JSON.stringify(d));

  // Por si acaso, buscar tambien por monto/nombre MONSAN.
  const porMonto = await coll.find({
    $or: [
      { 'erpLinks.total': { $gte: 5859.17, $lte: 5859.19 } },
      { 'erpLinks.total': { $gte: 5051.01, $lte: 5051.03 } },
      { concepto: /MONSAN/i },
    ],
  }).limit(10).toArray();
  console.log(`\nPor monto/nombre MONSAN: ${porMonto.length}`);
  for (const d of porMonto) console.log(JSON.stringify(d));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
