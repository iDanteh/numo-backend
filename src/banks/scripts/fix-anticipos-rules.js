'use strict';

/**
 * fix-anticipos-rules.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Corrige dos bugs en las reglas de anticipos:
 *
 * BUG 1 — Reg 24A (prio 92) y Reg 25A (prio 91) nunca disparan porque
 *          Reg 20/20A (prio 88) las bloquea al tener menor número de prioridad.
 *          Fix: subir Reg 24A → prio 85, Reg 25A → prio 84.
 *
 * BUG 2 — Reg 24A/25A necesitan discriminar "sin reembolso" (formaPago=17 o null)
 *          vs NCs con efectivo (formaPago=01/03/28/29, ya cubiertas por Reg 8A-8F).
 *          Fix: agregar variantes específicas por formaPago.
 *
 * Reglas nuevas/actualizadas:
 *   prio 84 — Reg 25A-17  NC saldo a favor 0%  formaPago=17  → 2103090001
 *   prio 84 — Reg 25A     NC saldo a favor 0%  sin formaPago → 2103090001
 *   prio 85 — Reg 24A-17  NC saldo a favor 16% formaPago=17  → 2103090001
 *   prio 85 — Reg 24A     NC saldo a favor 16% sin formaPago → 2103090001
 *
 * Uso:
 *   node src/banks/scripts/fix-anticipos-rules.js
 *   node src/banks/scripts/fix-anticipos-rules.js --dry-run
 */

require('dotenv').config();

const { sequelize }       = require('../../config/database.postgres');
const { CfdiMappingRule } = require('../../shared/models/postgres');

const DRY_RUN = process.argv.includes('--dry-run');

// Reglas actualizadas (las existentes con nueva prioridad + discriminador formaPago)
const ACTUALIZACIONES = [
  // Reg 25A existente: subir de prio 91 → 84, agregar formaPago=null
  {
    nombre:    'Reg 25A — Generación Saldo a Favor Tasa 0% (sin reembolso)',
    nuevaData: { prioridad: 84, formaPago: null },
  },
  // Reg 24A existente: subir de prio 92 → 85, agregar formaPago=null
  {
    nombre:    'Reg 24A — Generación Saldo a Favor 16% (sin reembolso)',
    nuevaData: { prioridad: 85, formaPago: null },
  },
];

// Reglas nuevas para formaPago=17 (Compensación/Netting — también "sin reembolso")
const NUEVAS = [
  {
    nombre:          'Reg 25A-17 — Generación Saldo a Favor Tasa 0% Compensación',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    formaPago:       '17',
    tasaIva:         '0',
    cuentaCargo:     '4200010002',   // Devoluciones s/Ventas 0%
    cuentaAbono:     '2103090001',   // Anticipos Otros (saldo a favor)
    cuentaIva:       null,
    conceptoContiene: null,
    prioridad:       84,
    isActive:        true,
  },
  {
    nombre:          'Reg 24A-17 — Generación Saldo a Favor 16% Compensación',
    tipoComprobante: 'E',
    tipoRelacion:    '01',
    formaPago:       '17',
    tasaIva:         '16',
    cuentaCargo:     '4200010001',   // Devoluciones s/Ventas 16%
    cuentaAbono:     '2103090001',   // Anticipos Otros (saldo a favor)
    cuentaIva:       '2104010001',
    conceptoContiene: null,
    prioridad:       84,
    isActive:        true,
  },
];

async function run() {
  await sequelize.authenticate();
  console.log(`PostgreSQL conectado.${DRY_RUN ? ' (DRY-RUN)' : ''}\n`);

  // ── 1. Actualizar prioridad de Reg 24A/25A existentes ─────────────────────
  console.log('── Paso 1: corregir prioridad Reg 24A/25A ───────────────────────────');
  for (const { nombre, nuevaData } of ACTUALIZACIONES) {
    const regla = await CfdiMappingRule.findOne({ where: { nombre } });
    if (!regla) {
      console.log(`  [no encontrada] ${nombre} — ejecuta --sync primero`);
      continue;
    }
    const cambios = Object.entries(nuevaData)
      .filter(([k, v]) => String(regla[k] ?? '') !== String(v ?? ''))
      .map(([k, v]) => `${k}: ${regla[k] ?? 'null'} → ${v ?? 'null'}`);

    if (!cambios.length) {
      console.log(`  [sin cambio]  ${nombre}`);
    } else {
      console.log(`  [actualizar]  ${nombre}  (${cambios.join(', ')})`);
      if (!DRY_RUN) await regla.update(nuevaData);
    }
  }

  // ── 2. Crear variantes formaPago=17 ───────────────────────────────────────
  console.log('\n── Paso 2: crear variantes compensación (formaPago=17) ──────────────');
  for (const datos of NUEVAS) {
    const existe = await CfdiMappingRule.findOne({ where: { nombre: datos.nombre } });
    if (existe) {
      console.log(`  [ya existe]   ${datos.nombre}`);
    } else {
      console.log(`  [crear]       ${datos.nombre}  prio:${datos.prioridad}  abono:${datos.cuentaAbono}`);
      if (!DRY_RUN) await CfdiMappingRule.create(datos);
    }
  }

  // ── Resumen ────────────────────────────────────────────────────────────────
  console.log('\n── Prioridades resultantes para NCs tipo E tipoRelacion=01 ──────────');
  console.log('  62-63  CLT-DEV/CC-CLT  (club tuberos)                           → 4200010001/4200020001, 2103090002');
  console.log('  65-66  TO-DEV/TO-CAN   (tipoOrigen=Devolución/Cancelación)      → 4200010001, 2103090001/1103010001');
  console.log('  70-71  TO-BON          (tipoOrigen=Bonificación)                → 4200020001, 2103090001/1103010001');
  console.log('  75-78  CC-BON/CC-DTO/CC-DEV/CC-CAN/CC-ANT (por concepto)       → cuentas según tipo');
  console.log('  80-84  Reg 8A-8F       (formaPago efectivo/banco/tarjeta)       → 4200010001, Bancos/Caja');
  console.log('  84     Reg 25A/25A-17  (sin reembolso 0%)                       → 4200010002, 2103090001');
  console.log('  85     Reg 24A/24A-17  (sin reembolso 16%)                      → 4200010001, 2103090001');
  console.log('  88     Reg 20/20A      (catch-all tipoRelacion=01)              → 4200020001, Bancos');
  console.log('  90     Reg 23          (tipoRelacion=07, NC anticipo)            → 2103010001');
  console.log('  91-92  [ya no alcanzan — reemplazadas por 84/85]');

  if (!DRY_RUN) {
    console.log('\n✓ Reglas de anticipos corregidas.');
    console.log('Regenera la balanza para ver el efecto en 2103090001.');
  } else {
    console.log('\nDRY-RUN completado. Ejecuta sin --dry-run para aplicar.');
  }

  await sequelize.close();
  process.exit(0);
}

run().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
