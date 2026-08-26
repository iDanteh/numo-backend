'use strict';
require('dotenv').config();
const service = require('./src/banks/domains/polizas/poliza.service');

const POLIZA_ID = Number(process.env.DIAG_POLIZA_ID || 507);

async function main() {
  const user = { nombre: 'Claude-diagnostico', role: 'admin' };
  const result = await service.cancel(POLIZA_ID, user, 'Cancelada para regenerar con instrumentacion de debug del bug CAC-077472 duplicado en Caja');
  console.log('Cancelada OK:', JSON.stringify(result));
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
