'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const { obtenerDesglosesCobroAlmacen, obtenerSaldosFavor } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'C0';
const FOLIOS = (process.env.DIAG_FOLIOS || '260800064,260800065,260701665').split(',');

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  console.log(`\n=== obtenerDesglosesCobroAlmacen para ${CENTRO}: ${FOLIOS.join(', ')} ===\n`);
  const almacen = await obtenerDesglosesCobroAlmacen({ rfc: RFC, series: FOLIOS.map(() => CENTRO), folios: FOLIOS });
  console.log(JSON.stringify(almacen, null, 2));

  console.log(`\n=== obtenerSaldosFavor (por si tambien aparece ahi) ===\n`);
  const saldos = await obtenerSaldosFavor({ rfc: RFC, series: FOLIOS.map(() => CENTRO), folios: FOLIOS });
  console.log(JSON.stringify(saldos, null, 2));

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
