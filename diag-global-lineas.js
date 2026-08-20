'use strict';
require('dotenv').config();
const { sequelize } = require('./src/config/database.postgres');
const { Op } = require('sequelize');
const PolizaMovimiento = require('./src/shared/models/postgres/PolizaMovimiento');
const AccountPlan = require('./src/shared/models/postgres/AccountPlan');
const Poliza = require('./src/shared/models/postgres/Poliza');

const UUID_GLOBAL = process.env.DIAG_UUID || '23503D5C-99D0-481C-9D6F-82C052EEAE50';

async function main() {
  await sequelize.authenticate();

  const poliza = await Poliza.findOne({ where: { estado: 'borrador' }, order: [['createdAt', 'DESC']], attributes: ['id', 'createdAt', 'estado'], raw: true });
  console.log('Poliza usada: id=', poliza?.id, 'estado=', poliza?.estado, 'createdAt=', poliza?.createdAt);

  const movs = await PolizaMovimiento.findAll({
    where: { polizaId: poliza.id, cfdiUuid: UUID_GLOBAL },
    attributes: ['id', 'cuentaId', 'debe', 'haber', 'tipoOrigen', 'reglaNombre', 'concepto', 'formaPago', 'serie'],
    raw: true,
  });
  console.log(`\nTotal lineas para uuid=${UUID_GLOBAL} en poliza ${poliza.id}:`, movs.length);

  const cuentaIds = [...new Set(movs.map(m => m.cuentaId).filter(Boolean))];
  const cuentas = await AccountPlan.findAll({ where: { id: { [Op.in]: cuentaIds } }, attributes: ['id', 'codigo', 'nombre'], raw: true });
  const cuentaMap = new Map(cuentas.map(c => [c.id, `${c.codigo} ${c.nombre}`]));

  let totalDebe = 0, totalHaber = 0;
  for (const m of movs) {
    totalDebe += Number(m.debe || 0);
    totalHaber += Number(m.haber || 0);
    console.log(`  cuenta=${cuentaMap.get(m.cuentaId) ?? m.cuentaId} debe=${m.debe} haber=${m.haber} tipoOrigen=${m.tipoOrigen} reglaNombre=${m.reglaNombre} formaPago=${m.formaPago} serie=${m.serie} concepto=${(m.concepto||'').slice(0,60)}`);
  }
  console.log(`\nTotal debe=${totalDebe.toFixed(2)} total haber=${totalHaber.toFixed(2)} diff=${(totalDebe-totalHaber).toFixed(2)}`);

  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
