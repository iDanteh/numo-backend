'use strict';
require('dotenv').config();
const { Poliza } = require('./src/shared/models/postgres');
const { Op } = require('sequelize');
const service = require('./src/banks/domains/polizas/poliza.service');

async function main() {
  const poliza = await Poliza.findOne({
    where: { fecha: '2026-08-11', estado: { [Op.ne]: 'cancelada' } },
    order: [['createdAt', 'DESC']],
    raw: true,
  });
  if (!poliza) { console.log('No hay poliza activa para 2026-08-11'); process.exit(0); }
  console.log('Poliza activa a cancelar:', JSON.stringify(poliza));
  const user = { nombre: 'Claude-diagnostico', role: 'admin' };
  const result = await service.cancel(poliza.id, user, 'Cancelada para regenerar con fix de anticipo OPA (MONSAN B0-260801098)');
  console.log('Cancelada OK:', JSON.stringify(result));
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
