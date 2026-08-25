'use strict';
require('dotenv').config();
const { PolizaMovimiento, AccountPlan } = require('./src/shared/models/postgres');

async function main() {
  const movs = await PolizaMovimiento.findAll({
    where: { cfdiUuid: '11EBEF39-5A5E-453D-BF8A-7EF9FAC23CF1' },
    order: [['polizaId', 'DESC'], ['orden', 'ASC']],
    raw: true,
  });
  console.log(`Total movimientos: ${movs.length}`);
  const polizaIds = [...new Set(movs.map(m => m.polizaId))];
  const ultimaPolizaId = polizaIds[0];
  const movsUltima = movs.filter(m => m.polizaId === ultimaPolizaId);
  console.log(`Poliza mas reciente: ${ultimaPolizaId}, ${movsUltima.length} lineas`);
  const cuentaIds = [...new Set(movsUltima.map(m => m.cuentaId).filter(Boolean))];
  const cuentas = await AccountPlan.findAll({ where: { id: cuentaIds }, raw: true });
  const cuentaPorId = new Map(cuentas.map(c => [c.id, c]));
  for (const m of movsUltima) {
    const c = cuentaPorId.get(m.cuentaId);
    console.log(JSON.stringify({ id: m.id, cuenta: c?.codigo, cuentaNombre: c?.nombre, debe: m.debe, haber: m.haber, serie: m.serie, concepto: m.concepto, tipoOrigen: m.tipoOrigen, reglaNombre: m.reglaNombre }));
  }
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
