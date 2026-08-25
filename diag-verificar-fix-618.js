'use strict';
require('dotenv').config();
const { Poliza, PolizaMovimiento, AccountPlan } = require('./src/shared/models/postgres');

async function main() {
  const p = await Poliza.findOne({ where: { fecha: '2026-08-11' }, order: [['id', 'DESC']] });
  console.log('Poliza activa mas reciente 11-ago:', p ? { id: p.id, estado: p.estado, createdAt: p.createdAt } : null);
  if (!p) { process.exit(0); }

  const movs = await PolizaMovimiento.findAll({ where: { polizaId: p.id, cfdiUuid: 'E48070D3-0456-4B1A-83FA-630BF7ACE3A6' }, raw: true });
  const cuentaIds = [...new Set(movs.map(m => m.cuentaId).filter(Boolean))];
  const cuentas = await AccountPlan.findAll({ where: { id: cuentaIds }, raw: true });
  const cuentaPorId = new Map(cuentas.map(c => [c.id, c]));
  console.log('Movimientos CFDI 260801224 en poliza activa:');
  for (const m of movs) {
    const c = cuentaPorId.get(m.cuentaId);
    console.log(JSON.stringify({ cuenta: c?.codigo, cuentaNombre: c?.nombre, debe: m.debe, haber: m.haber, tipoOrigen: m.tipoOrigen, reglaNombre: m.reglaNombre }));
  }

  const filaTarjeta = await PolizaMovimiento.sum('debe', {
    where: { polizaId: p.id, cuentaId: cuentaPorId.size ? [...cuentaPorId.values()].find(c => c.codigo === '1102011005')?.id : null },
  });
  console.log(`Suma total debe cuenta 1102011005 (Bancos) en poliza ${p.id}: ${filaTarjeta}`);

  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
