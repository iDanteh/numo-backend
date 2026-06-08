'use strict';

/**
 * banks/scripts/add-devoluciones-accounts.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Agrega las cuentas de Devoluciones Sobre Ventas (4200010000) que separan
 * devoluciones de descuentos en el grupo 4200000000.
 *
 * Seguro de ejecutar múltiples veces (upsert por codigo).
 *
 * Uso:
 *   node src/banks/scripts/add-devoluciones-accounts.js
 */

require('dotenv').config();

const { AccountPlan } = require('../../shared/models/postgres');

const CUENTAS_DEVOLUCIONES = [
  { codigo: '4200010000', nombre: 'Devoluciones Sobre Ventas',              ctaMayor: '4200000000' },
  { codigo: '4200010001', nombre: 'Devoluciones Sobre Ventas Tasa 16%',     ctaMayor: '4200010000' },
  { codigo: '4200010002', nombre: 'Devoluciones Sobre Ventas Tasa 0%',      ctaMayor: '4200010000' },
  { codigo: '4200010003', nombre: 'Devoluciones Sobre Ventas Otros Servicios', ctaMayor: '4200010000' },
];

async function run() {
  console.log('Agregando cuentas de Devoluciones Sobre Ventas...\n');

  for (const c of CUENTAS_DEVOLUCIONES) {
    const [, created] = await AccountPlan.upsert(
      { codigo: c.codigo, nombre: c.nombre, ctaMayor: c.ctaMayor },
      { conflictFields: ['codigo'] },
    );
    console.log(`  ${created ? 'CREADA  ' : 'YA EXISTE'} ${c.codigo}  ${c.nombre}`);
  }

  // Resolver parentId a partir de ctaMayor
  const todas  = await AccountPlan.findAll({ attributes: ['id', 'codigo'], raw: true });
  const mapaId = Object.fromEntries(todas.map(r => [r.codigo, r.id]));

  for (const c of CUENTAS_DEVOLUCIONES) {
    if (c.ctaMayor && mapaId[c.ctaMayor] && mapaId[c.codigo]) {
      await AccountPlan.update(
        { parentId: mapaId[c.ctaMayor] },
        { where: { codigo: c.codigo } },
      );
    }
  }

  console.log('\nListo. Cuentas de devoluciones creadas/verificadas.');
  console.log('\nPróximo paso: actualiza las reglas de mapeo CFDI para apuntar');
  console.log('  las notas de crédito (tipo E) a la cuenta 4200010001 / 4200010002');
  console.log('  en lugar de 4200020001 / 4200020002 (descuentos).');
}

if (require.main === module) {
  const { connectPostgres, disconnectPostgres } = require('../../config/database.postgres');
  connectPostgres()
    .then(async () => {
      await run();
      await disconnectPostgres();
      process.exit(0);
    })
    .catch(err => {
      console.error('[add-devoluciones-accounts] Error:', err.message);
      process.exit(1);
    });
}

module.exports = run;
