'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { _prefetchAjustesFacturaPropia } = require('./src/banks/domains/cfdi-mapping/cfdi-poliza-generator.service.js');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'C0';
const FECHA = process.env.DIAG_FECHA || '2026-08-13';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const desde = new Date(`${FECHA}T00:00:00-06:00`);
  const hasta = new Date(`${FECHA}T23:59:59.999-06:00`);
  const cfdis = await CFDI.find({
    'emisor.rfc': RFC, serie: SERIE, tipoDeComprobante: 'I', source: 'ERP',
    fecha: { $gte: desde, $lte: hasta },
  }).lean();
  console.log(`Total CFDIs del batch (${SERIE}, ${FECHA}):`, cfdis.length);

  const cfdiConRegla = cfdis.map(cfdi => ({ cfdi, rule: { cuentaCargo: '1101010003' } }));

  const { cobrosCobradoraDirecta, usoCaminoPorCentro } = await _prefetchAjustesFacturaPropia(cfdiConRegla, RFC, {
    centroPropioClave: SERIE, fechaDesde: desde, fechaHasta: hasta,
  });
  console.log('usoCaminoPorCentro:', usoCaminoPorCentro);
  console.log('Total cobrosCobradoraDirecta:', cobrosCobradoraDirecta.length);

  const porClave = {};
  for (const c of cobrosCobradoraDirecta) porClave[c.claveSat] = (porClave[c.claveSat] || 0) + c.monto;
  console.log('Suma por claveSat:', porClave);

  const efectivo = porClave['01'] || 0;
  const tarjeta = (porClave['28'] || 0) + (porClave['04'] || 0);
  const transferencia = porClave['03'] || 0;
  console.log(`\nEfectivo (01): ${efectivo.toFixed(2)}`);
  console.log(`Tarjeta (28+04): ${tarjeta.toFixed(2)}`);
  console.log(`Transferencia (03): ${transferencia.toFixed(2)}`);

  console.log('\nDetalle:');
  for (const c of cobrosCobradoraDirecta) console.log(c);

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
