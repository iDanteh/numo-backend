'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');

async function main() {
  await connectMongo();

  console.log('\n=== BankMovements con erpLinks.serie = O0, fecha en agosto 2026 ===\n');
  const movs = await BankMovement.find(
    {
      'erpLinks.serie': 'O0',
      fecha: { $gte: new Date('2026-08-01'), $lte: new Date('2026-08-31T23:59:59') },
    },
    { banco: 1, fecha: 1, deposito: 1, numeroAutorizacion: 1, erpLinks: 1 },
  ).sort({ fecha: 1 }).lean();

  console.log('Total encontrados en todo agosto:', movs.length);
  for (const m of movs.slice(0, 20)) {
    console.log({
      banco: m.banco, fecha: m.fecha, deposito: m.deposito, numeroAutorizacion: m.numeroAutorizacion,
      erpLinksSerieFolio: (m.erpLinks ?? []).filter(l => l.serie === 'O0').map(l => l.folioExterno),
    });
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
