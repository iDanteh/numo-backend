'use strict';

/**
 * fix-clt-efectivo.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Gestiona las reglas CC-CLT-EF (Club Tuberos formaPago=01 → Caja).
 *
 * HISTORIA: Se crearon pensando que formaPago=01 = cash refund → Caja.
 * DIAGNÓSTICO: Las 256 NCs tienen formaPago=01 pero CONTPAQI las manda a
 *   2103090002 (monedero), NO a Caja. No hay discriminador en el CFDI SAT.
 * DECISIÓN: Desactivar CC-CLT-EF para que CC-CLT (prio 63) capture todas
 *   las NCs Club Tuberos → 2103090002 ($222k vs $163k = $58k de diferencia
 *   aceptable, sin discriminador posible en CFDI).
 *
 * Uso:
 *   node src/banks/scripts/fix-clt-efectivo.js           → desactiva CC-CLT-EF
 *   node src/banks/scripts/fix-clt-efectivo.js --reactivar → reactiva CC-CLT-EF
 *   node src/banks/scripts/fix-clt-efectivo.js --dry-run
 */

require('dotenv').config();

const { sequelize }       = require('../../config/database.postgres');
const { CfdiMappingRule } = require('../../shared/models/postgres');

const DRY_RUN    = process.argv.includes('--dry-run');
const REACTIVAR  = process.argv.includes('--reactivar');
const { Op }     = require('sequelize');

const NOMBRES_CLT_EF = [
  'Reg CC-CLT-16-EF — NC Bonificación Club Tuberos 16% Efectivo',
  'Reg CC-CLT-0-EF — NC Bonificación Club Tuberos 0% Efectivo',
  'Reg CC-CLT-M-EF — NC Bonificación Club Tuberos Mixto Efectivo',
];

async function run() {
  await sequelize.authenticate();
  const accion = REACTIVAR ? 'REACTIVAR' : 'DESACTIVAR';
  console.log(`PostgreSQL conectado.${DRY_RUN ? ' (DRY-RUN)' : ''} — ${accion} CC-CLT-EF\n`);

  const reglas = await CfdiMappingRule.findAll({
    where: { nombre: { [Op.in]: NOMBRES_CLT_EF } },
  });

  if (!reglas.length) {
    console.log('No se encontraron reglas CC-CLT-EF en la BD.');
    await sequelize.close();
    return;
  }

  for (const r of reglas) {
    const nuevoEstado = REACTIVAR ? true : false;
    if (r.isActive === nuevoEstado) {
      console.log(`[sin cambio]  ${r.nombre}  isActive=${r.isActive}`);
    } else {
      console.log(`[${accion.toLowerCase()}]  ${r.nombre}  ${r.isActive} → ${nuevoEstado}`);
      if (!DRY_RUN) await r.update({ isActive: nuevoEstado });
    }
  }

  console.log('\n── Resultado ────────────────────────────────────────────────────────');
  if (!REACTIVAR) {
    console.log('  CC-CLT-EF desactivadas — CC-CLT (prio 63) captura todas las NCs');
    console.log('  club tuberos → 2103090002 (monedero). Diferencia con CONTPAQI: ~$58k');
    console.log('  No hay discriminador CFDI para separarlas. Aceptar diferencia.');
  } else {
    console.log('  CC-CLT-EF reactivadas — NCs formaPago=01 van a Caja (1101010003).');
  }

  if (DRY_RUN) console.log('\nDRY-RUN. Ejecuta sin --dry-run para aplicar.');
  else console.log('\n✓ Hecho. Regenera la balanza.');

  await sequelize.close();
  process.exit(0);
}

run().catch(err => { console.error(err.message); process.exit(1); });
