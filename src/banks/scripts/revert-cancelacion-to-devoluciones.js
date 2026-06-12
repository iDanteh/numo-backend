'use strict';

/**
 * revert-cancelacion-to-devoluciones.js
 * Revierte fix-cancelacion-to-descuentos.js:
 * TO-CAN / CLT-CAN cuentaCargo vuelve a Devoluciones (4200010001/02)
 *
 * Uso:
 *   node src/banks/scripts/revert-cancelacion-to-devoluciones.js
 *   node src/banks/scripts/revert-cancelacion-to-devoluciones.js --dry-run
 */

require('dotenv').config();

const { sequelize }       = require('../../config/database.postgres');
const { CfdiMappingRule } = require('../../shared/models/postgres');
const { Op }              = require('sequelize');

const DRY_RUN = process.argv.includes('--dry-run');

async function run() {
  await sequelize.authenticate();
  console.log(`PostgreSQL conectado.${DRY_RUN ? ' (DRY-RUN)' : ''}\n`);

  const reglas = await CfdiMappingRule.findAll({
    where: {
      nombre:   { [Op.or]: [{ [Op.like]: 'TO-CAN%' }, { [Op.like]: 'Reg CLT-CAN%' }] },
      isActive: true,
    },
  });

  let actualizadas = 0;
  for (const r of reglas) {
    let destino = null;
    if (r.cuentaCargo === '4200020001') destino = '4200010001';
    if (r.cuentaCargo === '4200020002') destino = '4200010002';

    if (!destino) {
      console.log(`  [sin cambio]  ${r.nombre}  (${r.cuentaCargo})`);
      continue;
    }
    console.log(`  [revertir]    ${r.nombre}  ${r.cuentaCargo} → ${destino}`);
    if (!DRY_RUN) await r.update({ cuentaCargo: destino });
    actualizadas++;
  }

  console.log(`\n✓ ${actualizadas} reglas revertidas a Devoluciones.`);
  if (DRY_RUN) console.log('DRY-RUN. Ejecuta sin --dry-run para aplicar.');
  else console.log('Regenera la balanza en NUMO.');

  await sequelize.close();
  process.exit(0);
}

run().catch(err => { console.error(err.message); process.exit(1); });
