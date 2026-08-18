'use strict';
require('dotenv').config();
const { sequelize } = require('./src/config/database.postgres');
const repo = require('./src/banks/domains/polizas/repositories/poliza.repository');

const POLIZA_ID = Number(process.env.DIAG_POLIZA_ID) || 419;

async function main() {
  await sequelize.authenticate();

  const poliza = await repo.findByIdLight(POLIZA_ID);
  console.log('Total movimientos cargados por Sequelize:', poliza.movimientos.length);

  const matching = poliza.movimientos.filter(m => (m.concepto || '').includes('260705759'));
  console.log(`\nLineas que mencionan 260705759: ${matching.length}\n`);
  for (const m of matching) {
    console.log({
      id: m.id, orden: m.orden, concepto: m.concepto, debe: m.debe, haber: m.haber,
      tipoOrigen: m.tipoOrigen, reglaNombre: m.reglaNombre, cuentaCodigo: m.cuenta?.codigo,
    });
  }

  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
