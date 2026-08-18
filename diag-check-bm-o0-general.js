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

  console.log('\n=== Solo los que caen en fecha 6-9 agosto (relevantes para la poliza del 7) ===\n');
  const relevantes = movs.filter(m => m.fecha >= new Date('2026-08-06') && m.fecha <= new Date('2026-08-09T23:59:59'));
  console.log('Total en ese rango de fechas:', relevantes.length);
  for (const m of relevantes) {
    console.log({
      banco: m.banco, fecha: m.fecha, deposito: m.deposito, numeroAutorizacion: m.numeroAutorizacion,
      erpLinksSerieFolio: (m.erpLinks ?? []).filter(l => l.serie === 'O0').map(l => l.folioExterno),
    });
  }

  console.log('\n=== Solo los que ligan a un folio en el rango 260800230-260800290 ===\n');
  const porFolio = movs.filter(m => (m.erpLinks ?? []).some(l => {
    const n = parseInt(l.folioExterno, 10);
    return l.serie === 'O0' && n >= 260800230 && n <= 260800290;
  }));
  console.log('Total en ese rango de folio:', porFolio.length);
  for (const m of porFolio) {
    console.log({
      banco: m.banco, fecha: m.fecha, deposito: m.deposito, numeroAutorizacion: m.numeroAutorizacion,
      erpLinksSerieFolio: (m.erpLinks ?? []).filter(l => l.serie === 'O0').map(l => l.folioExterno),
    });
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
