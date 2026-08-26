'use strict';
require('dotenv').config();
const service = require('./src/banks/domains/polizas/poliza.service');

const POLIZA_ID = Number(process.env.DIAG_POLIZA_ID || 499);

async function main() {
  const user = { nombre: 'Claude-diagnostico', role: 'admin' };
  const result = await service.cancel(POLIZA_ID, user, 'Poliza generada con fix roto durante pruebas del 24-ago, se cancela para permitir regeneracion limpia');
  console.log('Cancelada OK:', JSON.stringify(result));
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
