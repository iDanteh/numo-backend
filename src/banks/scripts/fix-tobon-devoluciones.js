'use strict';

/**
 * fix-tobon-devoluciones.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Las reglas TO-BON (prio 70) capturan TODAS las notas de crédito del
 * período, incluyendo Devoluciones y Cancelaciones que el ERP clasifica
 * como tipoOrigen='Devolución' / 'Cancelación'.
 *
 * Este script:
 *   1. Lee todas las reglas activas cuyo nombre empieza con 'TO-BON'.
 *   2. Les agrega tipoOrigen='Bonificación' → solo capturan bonificaciones.
 *   3. Crea reglas hermanas (mismas cuentas abono/IVA, prio-5) para
 *      tipoOrigen='Devolución' → cuentaCargo=4200010001 (Devoluciones s/V)
 *      tipoOrigen='Cancelación' → cuentaCargo=4200010001 (Devoluciones s/V)
 *   4. Si las hermanas ya existen por nombre, las actualiza.
 *
 * Seguro de ejecutar múltiples veces.
 *
 * Uso:
 *   node src/banks/scripts/fix-tobon-devoluciones.js
 *   node src/banks/scripts/fix-tobon-devoluciones.js --dry-run  (solo muestra cambios)
 */

require('dotenv').config();

const { sequelize }      = require('../../config/database.postgres');
const { CfdiMappingRule } = require('../../shared/models/postgres');
const { Op }             = require('sequelize');

const DRY_RUN = process.argv.includes('--dry-run');

// Cuenta cargo para Devoluciones y Cancelaciones
const CUENTA_DEVOLUCIONES_16 = '4200010001';
const CUENTA_DEVOLUCIONES_0  = '4200010002';

async function run() {
  await sequelize.authenticate();
  console.log(`PostgreSQL conectado.${DRY_RUN ? ' (DRY-RUN — no se hacen cambios)' : ''}\n`);

  // ── 1. Cargar reglas TO-BON ──────────────────────────────────────────────
  const toBonRules = await CfdiMappingRule.findAll({
    where: {
      nombre:   { [Op.like]: 'TO-BON%' },
      isActive: true,
    },
    order: [['prioridad', 'ASC']],
  });

  if (!toBonRules.length) {
    console.log('No se encontraron reglas TO-BON activas. Nada que hacer.');
    await sequelize.close();
    return;
  }

  console.log(`Reglas TO-BON encontradas: ${toBonRules.length}`);
  for (const r of toBonRules) {
    console.log(`  [prio ${r.prioridad}] ${r.nombre}`);
    console.log(`    tipoOrigen actual: ${r.tipoOrigen ?? '(null — catch-all)'}`);
    console.log(`    cuentaCargo: ${r.cuentaCargo}  cuentaAbono: ${r.cuentaAbono}  cuentaIva: ${r.cuentaIva ?? '—'}`);
  }

  // ── 2. Actualizar TO-BON → agregar tipoOrigen='Bonificación' ─────────────
  console.log('\n── Paso 1: agregar tipoOrigen=Bonificación a reglas TO-BON ─────────');
  for (const r of toBonRules) {
    if (r.tipoOrigen === 'Bonificación') {
      console.log(`  [sin cambio]  ${r.nombre} — ya tiene tipoOrigen=Bonificación`);
      continue;
    }
    console.log(`  [actualizar]  ${r.nombre}  null → Bonificación`);
    if (!DRY_RUN) await r.update({ tipoOrigen: 'Bonificación' });
  }

  // ── 3. Crear reglas hermanas para Devolución y Cancelación ───────────────
  console.log('\n── Paso 2: crear/actualizar reglas hermanas ─────────────────────────');

  for (const r of toBonRules) {
    // Determinar cuentaCargo correcta según tasa
    const esCero = r.tasaIva === '0';
    const cuentaCargoDevol = esCero ? CUENTA_DEVOLUCIONES_0 : CUENTA_DEVOLUCIONES_16;

    for (const origen of ['Devolución', 'Cancelación']) {
      const prefijo = origen === 'Devolución' ? 'DEV' : 'CAN';
      const nombreHermana = r.nombre.replace(/^TO-BON/, `TO-${prefijo}`);

      const datos = {
        nombre:          nombreHermana,
        tipoComprobante: r.tipoComprobante,
        metodoPago:      r.metodoPago,
        formaPago:       r.formaPago,
        tasaIva:         r.tasaIva,
        tieneDescuento:  r.tieneDescuento,
        tipoOrigen:      origen,
        // mismas cuentas abono e IVA que la regla original
        cuentaCargo:     cuentaCargoDevol,
        cuentaAbono:     r.cuentaAbono,
        cuentaAbono2:    r.cuentaAbono2   ?? null,
        cuentaIva:       r.cuentaIva      ?? null,
        cuentaIvaPPD:    r.cuentaIvaPPD   ?? null,
        // prioridad menor que la TO-BON (5 puntos mejor = gana antes)
        prioridad:       r.prioridad - 5,
        isActive:        true,
      };

      const existe = await CfdiMappingRule.findOne({ where: { nombre: nombreHermana } });
      if (existe) {
        const cambios = Object.entries(datos).filter(([k, v]) => String(existe[k] ?? '') !== String(v ?? '')).map(([k]) => k);
        if (!cambios.length) {
          console.log(`  [sin cambio]  ${nombreHermana}`);
        } else {
          console.log(`  [actualizar]  ${nombreHermana}  (campos: ${cambios.join(', ')})`);
          if (!DRY_RUN) await existe.update(datos);
        }
      } else {
        console.log(`  [crear]       ${nombreHermana}  prio:${datos.prioridad}  cargo:${datos.cuentaCargo}  abono:${datos.cuentaAbono}`);
        if (!DRY_RUN) await CfdiMappingRule.create(datos);
      }
    }
  }

  // ── 4. Resumen ────────────────────────────────────────────────────────────
  const totalNuevas = toBonRules.length * 2;
  console.log('\n── Resultado ────────────────────────────────────────────────────────');
  if (DRY_RUN) {
    console.log('DRY-RUN completado. Para aplicar los cambios, ejecuta sin --dry-run.');
  } else {
    console.log(`✓ ${toBonRules.length} reglas TO-BON actualizadas con tipoOrigen=Bonificación.`);
    console.log(`✓ Hasta ${totalNuevas} reglas hermanas creadas/actualizadas.`);
    console.log('\nEfecto esperado en la balanza:');
    console.log('  • tipoOrigen=Bonificación → TO-BON gana (prio 70) → 4200020001 Descuentos (sin cambio)');
    console.log('  • tipoOrigen=Devolución   → TO-DEV gana (prio 65) → 4200010001 Devoluciones ✓');
    console.log('  • tipoOrigen=Cancelación  → TO-CAN gana (prio 65) → 4200010001 Devoluciones ✓');
    console.log('\nRecuerda regenerar la balanza en NUMO para ver los cambios.');
  }

  await sequelize.close();
  process.exit(0);
}

run().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
