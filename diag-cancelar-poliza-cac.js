'use strict';
require('dotenv').config();
const { PolizaMovimiento, Poliza } = require('./src/shared/models/postgres');
const { Op } = require('sequelize');
const service = require('./src/banks/domains/polizas/poliza.service');

async function main() {
  const mov = await PolizaMovimiento.findOne({
    where: { cfdiUuid: 'CCDE51C4-099B-41E8-AF2B-613361E58444' },
    order: [['id', 'DESC']],
    raw: true,
  });
  if (!mov) { console.log('No se encontro movimiento con ese cfdiUuid'); process.exit(0); }
  const poliza = await Poliza.findByPk(mov.polizaId, { raw: true });
  console.log('Poliza mas reciente con este CFDI:', JSON.stringify(poliza));
  if (poliza.estado === 'cancelada') { console.log('Ya estaba cancelada.'); process.exit(0); }
  const user = { nombre: 'Claude-diagnostico', role: 'admin' };
  const result = await service.cancel(poliza.id, user, 'Cancelada para regenerar con instrumentacion de debug del bug CAC-077472 duplicado en Caja (ronda 2)');
  console.log('Cancelada OK:', JSON.stringify(result));
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
