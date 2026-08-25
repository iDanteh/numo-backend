'use strict';
require('dotenv').config();
const { PolizaMovimiento, AccountPlan, Poliza } = require('./src/shared/models/postgres');
const { Op } = require('sequelize');

async function main() {
  const movs = await PolizaMovimiento.findAll({
    where: { concepto: { [Op.iLike]: '%OPA-260201994%' } },
    order: [['polizaId', 'DESC'], ['orden', 'ASC']],
    raw: true,
  });
  console.log(`Encontrados: ${movs.length}`);
  if (!movs.length) { process.exit(0); }
  const polizaIds = [...new Set(movs.map(m => m.polizaId))];
  console.log(`Polizas distintas: ${polizaIds.length} -> ${polizaIds.slice(0, 10).join(',')}`);
  const polizas = await Poliza.findAll({ where: { id: polizaIds }, attributes: ['id', 'fecha', 'estado', 'concepto'], raw: true });
  for (const p of polizas) console.log('Poliza:', JSON.stringify(p));

  const cuentaIds = [...new Set(movs.map(m => m.cuentaId).filter(Boolean))];
  const cuentas = await AccountPlan.findAll({ where: { id: cuentaIds }, raw: true });
  const cuentaPorId = new Map(cuentas.map(c => [c.id, c]));
  for (const p of movs) {
    const c = cuentaPorId.get(p.cuentaId);
    console.log(JSON.stringify({
      id: p.id, polizaId: p.polizaId, cuenta: c?.codigo, cuentaNombre: c?.nombre,
      debe: p.debe, haber: p.haber, serie: p.serie, concepto: p.concepto,
      tipoOrigen: p.tipoOrigen, reglaNombre: p.reglaNombre, cfdiUuid: p.cfdiUuid,
    }));
  }
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
