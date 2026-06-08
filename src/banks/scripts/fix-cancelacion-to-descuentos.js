'use strict';

/**
 * fix-cancelacion-to-descuentos.js
 * ─────────────────────────────────────────────────────────────────────────────
 * En CONTPAQI, las Cancelaciones de cliente van a Descuentos (4200020001),
 * NO a Devoluciones (4200010001). Una cancelación es la anulación de una venta
 * sin devolución física del producto — es un ajuste de precio, no una devolución.
 *
 * Actualmente en NUMO:
 *   TO-CAN rules  (tipoOrigen='Cancelación') → cuentaCargo=4200010001 (Devoluciones) ← incorrecto
 *   CLT-CAN rules (club tuberos + Cancelación) → cuentaCargo=4200010001               ← incorrecto
 *
 * Después del fix:
 *   TO-CAN  16% y Mixto → cuentaCargo=4200020001 (Descuentos 16%)
 *   TO-CAN  0%          → cuentaCargo=4200020002 (Descuentos 0%)
 *   CLT-CAN 16% y Mixto → cuentaCargo=4200020001
 *   CLT-CAN 0%          → cuentaCargo=4200020002
 *
 * Efecto esperado:
 *   Devoluciones 4200010001 = solo tipoOrigen='Devolución' (~$2.62M ≈ CONTPAQI)
 *   Descuentos   4200020001 = Bonificaciones + Cancelaciones (~$1.30M vs CONTPAQI $1.06M)
 *
 * Uso:
 *   node src/banks/scripts/fix-cancelacion-to-descuentos.js
 *   node src/banks/scripts/fix-cancelacion-to-descuentos.js --dry-run
 */

require('dotenv').config();

const { sequelize }       = require('../../config/database.postgres');
const { CfdiMappingRule } = require('../../shared/models/postgres');
const { Op }              = require('sequelize');

const DRY_RUN = process.argv.includes('--dry-run');

// Mapa de corrección: cuenta actual → cuenta correcta según tasaIva
function cuentaCorrecta(tasaIva) {
  if (tasaIva === '0') return '4200020002';   // Descuentos 0%
  return '4200020001';                         // Descuentos 16% (también para mixto y null)
}

function cuentaActualEsDevoluciones(cuenta) {
  return cuenta === '4200010001' || cuenta === '4200010002';
}

async function run() {
  await sequelize.authenticate();
  console.log(`PostgreSQL conectado.${DRY_RUN ? ' (DRY-RUN)' : ''}\n`);

  // Buscar todas las reglas TO-CAN y CLT-CAN activas
  const reglas = await CfdiMappingRule.findAll({
    where: {
      nombre:   { [Op.or]: [{ [Op.like]: 'TO-CAN%' }, { [Op.like]: 'Reg CLT-CAN%' }] },
      isActive: true,
    },
    order: [['prioridad', 'ASC'], ['nombre', 'ASC']],
  });

  console.log(`Reglas TO-CAN / CLT-CAN encontradas: ${reglas.length}\n`);

  let actualizadas = 0;
  let sinCambio    = 0;

  for (const r of reglas) {
    const correcta = cuentaCorrecta(r.tasaIva);

    if (!cuentaActualEsDevoluciones(r.cuentaCargo)) {
      console.log(`  [sin cambio]  ${r.nombre}  cuentaCargo: ${r.cuentaCargo} (ya correcto)`);
      sinCambio++;
      continue;
    }

    console.log(`  [actualizar]  ${r.nombre}`);
    console.log(`                cuentaCargo: ${r.cuentaCargo} → ${correcta}  (tasaIva: ${r.tasaIva ?? 'null'})`);

    if (!DRY_RUN) await r.update({ cuentaCargo: correcta });
    actualizadas++;
  }

  console.log(`\n── Resultado ────────────────────────────────────────────────────────`);
  console.log(`  Actualizadas : ${actualizadas}`);
  console.log(`  Sin cambio   : ${sinCambio}`);

  if (!DRY_RUN) {
    console.log('\nEfecto esperado en la balanza:');
    console.log('  4200010001 Devoluciones → solo tipoOrigen=Devolución (~$2.62M ≈ CONTPAQI ✓)');
    console.log('  4200020001 Descuentos   → Bonificaciones + Cancelaciones (~$1.30M)');
    console.log('  Nota: Descuentos seguirá ~$240k sobre CONTPAQI — diferencia de saldo');
    console.log('  anterior (apertura) y posibles diferencias de periodo en NCs.');
    console.log('\nRecuerda regenerar la balanza en NUMO.');
  } else {
    console.log('\nDRY-RUN. Ejecuta sin --dry-run para aplicar.');
  }

  await sequelize.close();
  process.exit(0);
}

run().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
