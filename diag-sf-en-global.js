'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { obtenerSaldosFavor } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const UUID = '89CF6A7F-8925-43ED-9DFB-DFFB72993245';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const cfdi = await CFDI.findOne({ uuid: UUID, source: 'ERP' }).select('documentosRelacionados').lean();
  const docs = (cfdi.documentosRelacionados ?? []).filter(d => d.Serie && d.Folio);
  console.log('Total documentos relacionados (tickets) en esta Global:', docs.length);

  const LOTE = 150;
  let totalGenerado = 0;
  let totalUsado = 0;
  for (let i = 0; i < docs.length; i += LOTE) {
    const lote = docs.slice(i, i + LOTE);
    const resultado = await obtenerSaldosFavor({
      rfc: RFC, series: lote.map(d => d.Serie), folios: lote.map(d => d.Folio),
    });
    for (const cuenta of resultado) {
      if ((cuenta.saldosFavorGenerados ?? []).length) {
        console.log('\n--- GENERADO en ticket', cuenta.serieVenta, cuenta.folioVenta, '---');
        console.log(JSON.stringify(cuenta.saldosFavorGenerados, null, 2));
        totalGenerado += cuenta.saldosFavorGenerados.reduce((s, g) => s + (Number(g.monto) || 0), 0);
      }
      if ((cuenta.saldosFavorUsados ?? []).length) {
        console.log('\n--- USADO en ticket', cuenta.serieVenta, cuenta.folioVenta, '---');
        console.log(JSON.stringify(cuenta.saldosFavorUsados, null, 2));
        totalUsado += cuenta.saldosFavorUsados.reduce((s, u) => s + (Math.abs(Number(u.montoUsado)) || 0), 0);
      }
    }
  }
  console.log('\n=== TOTALES ===');
  console.log('Total generado en toda la Global:', totalGenerado);
  console.log('Total usado en toda la Global:', totalUsado);

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
