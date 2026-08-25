'use strict';
require('dotenv').config();
const { Poliza, PolizaMovimiento, AccountPlan } = require('./src/shared/models/postgres');
const { Op } = require('sequelize');

async function main() {
  const cuenta = await AccountPlan.findOne({ where: { codigo: '1102012001' }, raw: true });
  console.log('Cuenta 1102012001:', JSON.stringify(cuenta));

  const activa = await Poliza.findOne({
    where: { fecha: '2026-08-11', estado: { [Op.ne]: 'cancelada' } },
    order: [['createdAt', 'DESC']],
    raw: true,
  });
  if (!activa || !cuenta) { process.exit(0); }

  const movs = await PolizaMovimiento.findAll({
    where: { polizaId: activa.id, cuentaId: cuenta.id },
    order: [['orden', 'ASC']],
    raw: true,
  });
  console.log(`Movimientos en cuenta 1102012001 (poliza ${activa.id}): ${movs.length}`);
  for (const m of movs) {
    console.log(JSON.stringify({ id: m.id, debe: m.debe, haber: m.haber, serie: m.serie, concepto: m.concepto, tipoOrigen: m.tipoOrigen, reglaNombre: m.reglaNombre, cfdiUuid: m.cfdiUuid, formaPago: m.formaPago }));
  }
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
