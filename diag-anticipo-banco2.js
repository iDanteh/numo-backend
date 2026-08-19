'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');

const UUID = 'B132E2A6-46A7-44EB-A9C6-0452A45D9510';
const MONTO = 689000;

async function main() {
  await connectMongo();

  console.log('=== 1. Por uuidXML / erpLinks.folioFiscal ===');
  const porUuid = await BankMovement.find({
    $or: [{ uuidXML: UUID }, { 'erpLinks.folioFiscal': UUID }],
  }).lean();
  console.log('Encontrados:', porUuid.length);
  for (const m of porUuid) {
    console.log({
      banco: m.banco, fecha: m.fecha, deposito: m.deposito, numeroAutorizacion: m.numeroAutorizacion,
      erpLinks: (m.erpLinks ?? []).map(l => ({ serie: l.serie, folioExterno: l.folioExterno, folioFiscal: l.folioFiscal, total: l.total })),
    });
  }

  console.log('\n=== 2. Por monto ~689000 cerca de 2026-07-30 ===');
  const desde = new Date('2026-07-25T00:00:00Z');
  const hasta = new Date('2026-08-05T00:00:00Z');
  const porMonto = await BankMovement.find({
    fecha: { $gte: desde, $lte: hasta },
    deposito: { $gte: MONTO - 1, $lte: MONTO + 1 },
  }).lean();
  console.log('Encontrados:', porMonto.length);
  for (const m of porMonto) {
    console.log({
      banco: m.banco, fecha: m.fecha, deposito: m.deposito, numeroAutorizacion: m.numeroAutorizacion,
      erpLinks: (m.erpLinks ?? []).map(l => ({ serie: l.serie, folioExterno: l.folioExterno, folioFiscal: l.folioFiscal, total: l.total })),
    });
  }

  console.log('\n=== 3. Cualquier erpLinks con folioExterno que contenga 260701665 o 260701668 ===');
  const porFolioParcial = await BankMovement.find({
    $or: [
      { 'erpLinks.folioExterno': { $regex: '260701665' } },
      { 'erpLinks.folioExterno': { $regex: '260701668' } },
    ],
  }).lean();
  console.log('Encontrados:', porFolioParcial.length);
  for (const m of porFolioParcial) {
    console.log({
      banco: m.banco, fecha: m.fecha, deposito: m.deposito, numeroAutorizacion: m.numeroAutorizacion,
      erpLinks: (m.erpLinks ?? []).map(l => ({ serie: l.serie, folioExterno: l.folioExterno, folioFiscal: l.folioFiscal, total: l.total })),
    });
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
