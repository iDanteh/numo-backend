'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');
const { obtenerDesglosesCobroAlmacen } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'C0';
const FOLIO = process.env.DIAG_FOLIO || '260802499';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  console.log(`=== obtenerDesglosesCobroAlmacen para ${SERIE}-${FOLIO} ===\n`);
  const almacen = await obtenerDesglosesCobroAlmacen({ rfc: RFC, series: [SERIE], folios: [FOLIO] });
  console.log(JSON.stringify(almacen, null, 2));

  console.log(`\n=== BankMovement con erpLinks ${SERIE}-${FOLIO} ===\n`);
  const movs = await BankMovement.find(
    { 'erpLinks.serie': SERIE, 'erpLinks.folioExterno': FOLIO },
    { banco: 1, fecha: 1, deposito: 1, numeroAutorizacion: 1, referenciaNumerica: 1, erpLinks: 1 },
  ).lean();
  console.log('Total:', movs.length);
  for (const m of movs) {
    console.log({
      banco: m.banco, fecha: m.fecha, deposito: m.deposito,
      numeroAutorizacion: m.numeroAutorizacion, referenciaNumerica: m.referenciaNumerica,
      erpLinks: (m.erpLinks ?? []).map(l => ({ serie: l.serie, folioExterno: l.folioExterno, total: l.total })),
    });
  }

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
