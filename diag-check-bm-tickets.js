'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');

const FOLIOS = ['260800249','260800262','260800254','260800277','260800263','260800257','260800251','260800241','260800234','260800256'];

async function main() {
  await connectMongo();

  const movs = await BankMovement.find(
    { $or: FOLIOS.map(f => ({ 'erpLinks.serie': 'O0', 'erpLinks.folioExterno': f })) },
    { banco: 1, fecha: 1, deposito: 1, numeroAutorizacion: 1, referenciaNumerica: 1, erpLinks: 1 },
  ).lean();

  console.log('Total BankMovements encontrados para estos folios:', movs.length);
  for (const m of movs) {
    console.log({
      banco: m.banco, fecha: m.fecha, deposito: m.deposito,
      numeroAutorizacion: m.numeroAutorizacion, referenciaNumerica: m.referenciaNumerica,
      erpLinksSerieFolio: (m.erpLinks ?? []).map(l => `${l.serie}-${l.folioExterno}`),
    });
  }
  if (!movs.length) console.log('Ninguno de estos folios tiene un BankMovement ligado todavia.');

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
