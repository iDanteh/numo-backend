'use strict';

/**
 * diag-cancelar-poliza.js
 * Cancela una poliza puntual (mismo efecto que el boton "Cancelar" de la UI).
 * Uso: node src/banks/scripts/diag-cancelar-poliza.js <polizaId>
 */

require('dotenv').config();

const { sequelize } = require('../../config/database.postgres');
const polizaService = require('../domains/polizas/poliza.service');

const polizaId = Number(process.argv[2]);
if (!polizaId) { console.error('Uso: node diag-cancelar-poliza.js <polizaId>'); process.exit(1); }

async function main() {
  await sequelize.authenticate();
  const user = { dbId: 0, nombre: 'diag-script', email: 'diag@local' };
  const poliza = await polizaService.cancel(polizaId, user);
  console.log('Cancelada:', { id: poliza.id, estado: poliza.estado });
  process.exit(0);
}

main().catch(err => { console.error('ERROR:', err.stack || err.message); process.exit(1); });
