'use strict';
require('dotenv').config();
const { Poliza } = require('./src/shared/models/postgres');
const { Op } = require('sequelize');

async function main() {
  const polizas = await Poliza.findAll({
    where: {
      fecha: '2026-08-07',
      estado: { [Op.ne]: 'cancelada' },
      createdAt: { [Op.gte]: new Date('2026-08-24T00:00:00-06:00') },
    },
    attributes: ['id', 'tipo', 'numero', 'fecha', 'centroCosto', 'rfc', 'estado', 'folio', 'concepto', 'createdAt'],
    order: [['createdAt', 'ASC']],
    raw: true,
  });
  console.log(`Polizas del 7-ago-2026, creadas hoy (24-ago), no canceladas: ${polizas.length}`);
  for (const p of polizas) {
    console.log(JSON.stringify(p));
  }
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
