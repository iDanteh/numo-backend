'use strict';
require('dotenv').config();
const { Poliza, PolizaMovimiento, AccountPlan } = require('./src/shared/models/postgres');
const { Op } = require('sequelize');

async function main() {
  const activa = await Poliza.findOne({
    where: { fecha: '2026-08-11', estado: { [Op.ne]: 'cancelada' } },
    order: [['createdAt', 'DESC']],
    raw: true,
  });
  if (!activa) { console.log('NO hay poliza activa (no cancelada) para 2026-08-11'); process.exit(0); }
  console.log('Poliza activa:', JSON.stringify(activa));

  const movs = await PolizaMovimiento.findAll({ where: { polizaId: activa.id }, order: [['orden', 'ASC']], raw: true });
  console.log(`Total movimientos en poliza activa: ${movs.length}`);

  const opaMovs = movs.filter(m => (m.concepto || '').includes('OPA-260201994') || (m.cfdiUuid || '').toUpperCase() === 'EDDCAB96-E49A-4742-A2F4-953799CD7EC0');
  console.log(`Movimientos OPA-260201994 en poliza activa: ${opaMovs.length}`);

  const cacMovs = movs.filter(m => (m.cfdiUuid || '').toUpperCase() === 'CCDE51C4-099B-41E8-AF2B-613361E58444');
  console.log(`Movimientos CCDE51C4 (CAC-077472) en poliza activa: ${cacMovs.length}`);

  const cuentaIds = [...new Set([...opaMovs, ...cacMovs].map(m => m.cuentaId).filter(Boolean))];
  const cuentas = await AccountPlan.findAll({ where: { id: cuentaIds }, raw: true });
  const cuentaPorId = new Map(cuentas.map(c => [c.id, c]));
  console.log('\n--- OPA ---');
  for (const p of opaMovs) {
    const c = cuentaPorId.get(p.cuentaId);
    console.log(JSON.stringify({ id: p.id, cuenta: c?.codigo, cuentaNombre: c?.nombre, debe: p.debe, haber: p.haber, concepto: p.concepto, tipoOrigen: p.tipoOrigen, reglaNombre: p.reglaNombre }));
  }
  console.log('\n--- CAC-077472 ---');
  for (const p of cacMovs) {
    const c = cuentaPorId.get(p.cuentaId);
    console.log(JSON.stringify({ id: p.id, cuenta: c?.codigo, cuentaNombre: c?.nombre, debe: p.debe, haber: p.haber, concepto: p.concepto, tipoOrigen: p.tipoOrigen, reglaNombre: p.reglaNombre }));
  }
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
