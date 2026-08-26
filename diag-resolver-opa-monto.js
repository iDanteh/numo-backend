'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');

async function main() {
  await connectMongo();

  const c = { uuid: '6037C46E-387E-4684-BA9D-885F702E4A52', fecha: '2026-07-29T11:04:42.000Z', total: 51128.89 };
  const totalAnticipo = Number(c.total) || 0;
  const fechaAnticipo = new Date(c.fecha);
  const VENTANA_MS = 5 * 24 * 3600 * 1000;
  const filtro = {
    fecha: { $gte: new Date(fechaAnticipo.getTime() - VENTANA_MS), $lte: new Date(fechaAnticipo.getTime() + VENTANA_MS) },
    'erpLinks.total': { $gte: totalAnticipo - 0.01, $lte: totalAnticipo + 0.01 },
  };
  console.log('Filtro:', JSON.stringify(filtro));
  const bm = await BankMovement.findOne(filtro).select('erpLinks fecha').lean();
  console.log('Resultado findOne:', JSON.stringify(bm));

  const bmAll = await BankMovement.find(filtro).select('erpLinks fecha').lean();
  console.log(`Resultado find (todos): ${bmAll.length}`);
  for (const d of bmAll) console.log(JSON.stringify({ fecha: d.fecha, erpLinksTotales: d.erpLinks.map(l => l.total) }));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
