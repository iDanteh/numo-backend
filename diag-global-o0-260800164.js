'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');

const RFC = process.env.DIAG_RFC || 'CCO011113663';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  console.log('\n=== 1. CFDI O0-260800164 (es Factura Global?) ===\n');
  const cfdi = await CFDI.findOne({ 'emisor.rfc': RFC, serie: 'O0', folio: '260800164' })
    .select('uuid serie folio total metodoPago formaPago documentosRelacionados receptor.nombre').lean();
  if (cfdi) {
    console.log(JSON.stringify({
      uuid: cfdi.uuid, total: cfdi.total, metodoPago: cfdi.metodoPago, formaPago: cfdi.formaPago,
      cantidadDocsRelacionados: (cfdi.documentosRelacionados ?? []).length,
      receptor: cfdi.receptor?.nombre,
    }, null, 2));
  } else {
    console.log('No encontrado');
  }

  if (cfdi?.uuid) {
    console.log('\n=== 2. BankMovements ligados a este UUID (erpLinks.folioFiscal) ===\n');
    const movsPorUuid = await BankMovement.find(
      { 'erpLinks.folioFiscal': new RegExp(`^${cfdi.uuid}$`, 'i') },
      { banco: 1, fecha: 1, deposito: 1, categoria: 1, folio: 1, numeroAutorizacion: 1, referenciaNumerica: 1, erpLinks: 1 },
    ).lean();
    console.log('Total encontrados:', movsPorUuid.length);
    for (const m of movsPorUuid) {
      console.log({
        banco: m.banco, fecha: m.fecha, deposito: m.deposito, categoria: m.categoria,
        folio: m.folio, numeroAutorizacion: m.numeroAutorizacion, referenciaNumerica: m.referenciaNumerica,
        erpLinksCount: (m.erpLinks ?? []).length,
        erpLinksSerieFolio: (m.erpLinks ?? []).map(l => `${l.serie}-${l.folioExterno}`),
      });
    }

    console.log('\n=== 3. BankMovements ligados via erpLinks.serie+folioExterno = O0-260800164 (fallback) ===\n');
    const movsPorSerieFolio = await BankMovement.find(
      { 'erpLinks.serie': 'O0', 'erpLinks.folioExterno': '260800164' },
      { banco: 1, fecha: 1, deposito: 1, categoria: 1, folio: 1, numeroAutorizacion: 1, referenciaNumerica: 1 },
    ).lean();
    console.log('Total encontrados:', movsPorSerieFolio.length);
    for (const m of movsPorSerieFolio) {
      console.log({
        banco: m.banco, fecha: m.fecha, deposito: m.deposito, categoria: m.categoria,
        folio: m.folio, numeroAutorizacion: m.numeroAutorizacion, referenciaNumerica: m.referenciaNumerica,
      });
    }
  }

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
