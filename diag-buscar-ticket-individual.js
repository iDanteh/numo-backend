'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacen, obtenerSaldosFavor } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = 'CCO011113663';

async function main() {
  await connectMongo();
  const [rA, rS] = await Promise.all([
    obtenerDesglosesCobroAlmacen({ rfc: RFC, series: ['B0'], folios: ['260802904'] }),
    obtenerSaldosFavor({ rfc: RFC, series: ['B0'], folios: ['260802904'] }),
  ]);
  console.log('obtenerDesglosesCobroAlmacen (por serie/folio EXACTO del ticket):', JSON.stringify(rA, null, 2));
  console.log('\nobtenerSaldosFavor:', JSON.stringify(rS, null, 2));
  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
