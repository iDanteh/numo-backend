'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { obtenerSaldosFavor } = require('./src/banks/domains/erp/erp-sync.service');

const UUID = process.env.DIAG_UUID || '89CF6A7F-8925-43ED-9DFB-DFFB72993245';
const RFC = process.env.DIAG_RFC || 'CCO011113663';

const TIPO_MARCADORES_DEV = ['BCT', 'BON', 'DEV', 'DVE', 'CAC', 'CANCELACION', 'BEP', 'BXC', 'BN', 'ANN', 'CES'];

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const cfdi = await CFDI.findOne({ uuid: UUID }).select('documentosRelacionados serie folio').lean();
  console.log('\n=== documentosRelacionados del CFDI ===\n');
  console.log(JSON.stringify(cfdi.documentosRelacionados, null, 2));

  const marcador = (cfdi.documentosRelacionados ?? []).find(
    d => TIPO_MARCADORES_DEV.includes((d.Serie ?? '').toUpperCase()) && d.Folio,
  );
  console.log('\nMarcador de devolucion encontrado:', marcador);

  if (marcador) {
    console.log(`\n=== Saldos a favor generados/usados para venta 090 y para el marcador ${marcador.Serie}-${marcador.Folio} ===\n`);
    const resultado = await obtenerSaldosFavor({ rfc: RFC, series: [marcador.Serie, 'C0'], folios: [marcador.Folio, '260800090'] });
    console.log(JSON.stringify(resultado, null, 2));
  }

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
