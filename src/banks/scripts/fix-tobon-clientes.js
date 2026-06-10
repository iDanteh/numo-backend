'use strict';

/**
 * fix-tobon-clientes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Las reglas TO-BON*, TO-DEV* y TO-CAN* (Club Tuberos) tienen
 * cuentaAbono = '2103090002' (Anticipos Otros Club Tuberos).
 *
 * Contabilidad confirmó que el abono correcto para bonificaciones/devoluciones
 * Club Tuberos es '1103010001' (Clientes Nacionales General Tasa 16%), NO el
 * monedero de anticipos. La cuenta 2103090002 se sobreinfla en ~$4M porque
 * recibe todos los NCs Club Tuberos.
 *
 * Este script:
 *   1. Busca reglas activas cuyo nombre comienza con TO-BON*, TO-DEV* o TO-CAN*.
 *   2. Para cada una donde cuentaAbono === '2103090002': actualiza a '1103010001'.
 *   3. Deja sin cambio las reglas con cuentaAbono = '1101010003' (Caja/Efectivo).
 *   4. Deja sin cambio las reglas con cuentaAbono = '2103090001' (Anticipos otros).
 *
 * Seguro de ejecutar múltiples veces (idempotente).
 *
 * Uso:
 *   node src/banks/scripts/fix-tobon-clientes.js
 *   node src/banks/scripts/fix-tobon-clientes.js --dry-run   (solo muestra cambios)
 */

require('dotenv').config();

const { sequelize }      = require('../../config/database.postgres');
const { CfdiMappingRule } = require('../../shared/models/postgres');
const { Op }             = require('sequelize');

const DRY_RUN = process.argv.includes('--dry-run');

// Cuenta incorrecta (monedero anticipos Club Tuberos)
const CUENTA_ABONO_ERRADA   = '2103090002';
// Cuenta correcta (Clientes Nacionales General Tasa 16%)
const CUENTA_ABONO_CORRECTA = '1103010001';

// Cuentas que NO se deben tocar
const CUENTAS_EXCLUIDAS = new Set([
  '1101010003', // Caja — correcto para Efectivo
  '2103090001', // Anticipos otros — diferente caso
]);

async function run() {
  await sequelize.authenticate();
  console.log(`PostgreSQL conectado.${DRY_RUN ? ' (DRY-RUN — no se hacen cambios)' : ''}\n`);

  // ── 1. Cargar reglas TO-BON*, TO-DEV*, TO-CAN* activas ───────────────────
  const reglas = await CfdiMappingRule.findAll({
    where: {
      isActive: true,
      nombre: {
        [Op.or]: [
          { [Op.like]: 'TO-BON%' },
          { [Op.like]: 'TO-DEV%' },
          { [Op.like]: 'TO-CAN%' },
        ],
      },
    },
    order: [['nombre', 'ASC']],
  });

  if (!reglas.length) {
    console.log('No se encontraron reglas TO-BON*/TO-DEV*/TO-CAN* activas. Nada que hacer.');
    await sequelize.close();
    process.exit(0);
  }

  console.log(`Reglas encontradas: ${reglas.length}`);
  for (const r of reglas) {
    console.log(`  [prio ${r.prioridad}] ${r.nombre}  cuentaAbono: ${r.cuentaAbono}`);
  }

  // ── 2. Filtrar y actualizar ───────────────────────────────────────────────
  console.log('\n── Aplicando corrección de cuenta abono ─────────────────────────────');

  let actualizadas  = 0;
  let sinCambio     = 0;
  let excluidas     = 0;

  for (const r of reglas) {
    if (CUENTAS_EXCLUIDAS.has(r.cuentaAbono)) {
      console.log(`  [excluida]    ${r.nombre}  cuentaAbono: ${r.cuentaAbono} (cuenta protegida, sin cambio)`);
      excluidas++;
      continue;
    }

    if (r.cuentaAbono !== CUENTA_ABONO_ERRADA) {
      console.log(`  [sin cambio]  ${r.nombre}  cuentaAbono: ${r.cuentaAbono} (no es la cuenta errónea)`);
      sinCambio++;
      continue;
    }

    console.log(`  [actualizar]  ${r.nombre}  ${CUENTA_ABONO_ERRADA} → ${CUENTA_ABONO_CORRECTA}`);
    if (!DRY_RUN) {
      await r.update({ cuentaAbono: CUENTA_ABONO_CORRECTA });
    }
    actualizadas++;
  }

  // ── 3. Resumen ────────────────────────────────────────────────────────────
  console.log('\n── Resultado ────────────────────────────────────────────────────────');
  if (DRY_RUN) {
    console.log(`DRY-RUN completado.`);
    console.log(`  Reglas que se actualizarían : ${actualizadas}`);
    console.log(`  Reglas sin cambio            : ${sinCambio}`);
    console.log(`  Reglas excluidas (protegidas): ${excluidas}`);
    console.log('\nPara aplicar los cambios, ejecuta sin --dry-run.');
  } else {
    console.log(`Reglas actualizadas (${CUENTA_ABONO_ERRADA} → ${CUENTA_ABONO_CORRECTA}): ${actualizadas}`);
    console.log(`Reglas sin cambio              : ${sinCambio}`);
    console.log(`Reglas excluidas (protegidas)  : ${excluidas}`);
    if (actualizadas > 0) {
      console.log('\nEfecto esperado en la balanza:');
      console.log(`  • La cuenta ${CUENTA_ABONO_ERRADA} (Anticipos Otros Club Tuberos) ya no recibirá NCs.`);
      console.log(`  • La cuenta ${CUENTA_ABONO_CORRECTA} (Clientes Nacionales Gral Tasa 16%) absorberá el saldo (~$4M).`);
    }
  }

  console.log('\nRecuerda regenerar la balanza en NUMO.');

  await sequelize.close();
  process.exit(0);
}

run().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
