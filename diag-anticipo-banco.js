'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');

const SERIE = process.env.DIAG_SERIE || 'C0';
const FOLIOS = (process.env.DIAG_FOLIOS || '260701665').split(',');

async function main() {
  await connectMongo();

  const movs = await BankMovement.find(
    { $or: FOLIOS.map(f => ({ 'erpLinks.serie': SERIE, 'erpLinks.folioExterno': f })) },
    { banco: 1, fecha: 1, deposito: 1, numeroAutorizacion: 1, referenciaNumerica: 1, erpLinks: 1 },
  ).lean();

  console.log(`Total BankMovements para ${SERIE}-[${FOLIOS.join(',')}]:`, movs.length);
  for (const m of movs) {
    console.log({
      banco: m.banco, fecha: m.fecha, deposito: m.deposito,
      numeroAutorizacion: m.numeroAutorizacion, referenciaNumerica: m.referenciaNumerica,
      erpLinksSerieFolio: (m.erpLinks ?? []).map(l => `${l.serie}-${l.folioExterno}`),
    });
  }
  if (!movs.length) console.log('Ningun BankMovement ligado a este folio todavia.');

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
