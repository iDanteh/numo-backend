'use strict';

/**
 * fix-club-tuberos-doble-validacion.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Implementa "doble validación" para NCs Club Tuberos:
 *
 *   Capa 1 (prio 62) — concepto 'club tuberos' + tipoOrigen discrimina cargo:
 *     Devolución / Cancelación  → cargo:4200010001 (Devoluciones), abono:2103090002
 *
 *   Capa 2 (prio 63) — concepto 'club tuberos', cualquier tipoOrigen:
 *     → cargo:4200020001 (Descuentos), abono:2103090002
 *
 *   Capa 3 (las CC-CLT existentes, prio 74 → 63):
 *     Sube la prioridad de las CC-CLT del seed de 74 a 63 para que ganen
 *     sobre TO-BON (70) y TO-DEV (65).
 *
 * Cadena de prioridades resultante para NCs Club Tuberos:
 *   62 — CLT-DEV/CLT-CAN (club tuberos + Devolución/Cancelación) → 4200010001, 2103090002
 *   63 — CLT (club tuberos, cualquier origen)                    → 4200020001, 2103090002
 *   65 — TO-DEV "Traslado club t" (tipoOrigen=Devolución)        → 4200010001, 1103010001
 *   66 — TO-DEV otros (tipoOrigen=Devolución)                    → 4200010001, 2103090001
 *   70 — TO-BON "Traslado club t" (tipoOrigen=Bonificación)      → 4200020001, 1103010001
 *   71 — TO-BON otros (tipoOrigen=Bonificación)                  → 4200020001, 2103090001
 *
 * Uso:
 *   node src/banks/scripts/fix-club-tuberos-doble-validacion.js
 *   node src/banks/scripts/fix-club-tuberos-doble-validacion.js --dry-run
 */

require('dotenv').config();

const { sequelize }       = require('../../config/database.postgres');
const { CfdiMappingRule } = require('../../shared/models/postgres');
const { Op }              = require('sequelize');

const DRY_RUN = process.argv.includes('--dry-run');

// ── Reglas nuevas de capa 1 (prio 62): concepto + tipoOrigen ─────────────────
const NUEVAS_CAPA1 = [
  // Devolución Club Tuberos 16%
  {
    nombre:           'Reg CLT-DEV-16 — NC Devolución Club Tuberos 16%',
    tipoComprobante:  'E',
    tasaIva:          '16',
    conceptoContiene: 'club tuberos',
    tipoOrigen:       'Devolución',
    cuentaCargo:      '4200010001',   // Devoluciones s/Ventas 16%
    cuentaAbono:      '2103090002',   // Monedero Club Tuberos
    cuentaIva:        '2104010001',
    prioridad:        62,
  },
  {
    nombre:           'Reg CLT-DEV-0 — NC Devolución Club Tuberos 0%',
    tipoComprobante:  'E',
    tasaIva:          '0',
    conceptoContiene: 'club tuberos',
    tipoOrigen:       'Devolución',
    cuentaCargo:      '4200010002',   // Devoluciones s/Ventas 0%
    cuentaAbono:      '2103090002',
    cuentaIva:        null,
    prioridad:        62,
  },
  {
    nombre:           'Reg CLT-DEV-M — NC Devolución Club Tuberos Mixto',
    tipoComprobante:  'E',
    tasaIva:          'mixto',
    conceptoContiene: 'club tuberos',
    tipoOrigen:       'Devolución',
    cuentaCargo:      '4200010001',
    cuentaAbono:      '2103090002',
    cuentaAbono2:     null,           // porción 0% sin cuenta separada por ahora
    cuentaIva:        '2104010001',
    prioridad:        62,
  },
  // Cancelación Club Tuberos
  {
    nombre:           'Reg CLT-CAN-16 — NC Cancelación Club Tuberos 16%',
    tipoComprobante:  'E',
    tasaIva:          '16',
    conceptoContiene: 'club tuberos',
    tipoOrigen:       'Cancelación',
    cuentaCargo:      '4200010001',
    cuentaAbono:      '2103090002',
    cuentaIva:        '2104010001',
    prioridad:        62,
  },
  {
    nombre:           'Reg CLT-CAN-0 — NC Cancelación Club Tuberos 0%',
    tipoComprobante:  'E',
    tasaIva:          '0',
    conceptoContiene: 'club tuberos',
    tipoOrigen:       'Cancelación',
    cuentaCargo:      '4200010002',
    cuentaAbono:      '2103090002',
    cuentaIva:        null,
    prioridad:        62,
  },
  {
    nombre:           'Reg CLT-CAN-M — NC Cancelación Club Tuberos Mixto',
    tipoComprobante:  'E',
    tasaIva:          'mixto',
    conceptoContiene: 'club tuberos',
    tipoOrigen:       'Cancelación',
    cuentaCargo:      '4200010001',
    cuentaAbono:      '2103090002',
    cuentaIva:        '2104010001',
    prioridad:        62,
  },
];

async function run() {
  await sequelize.authenticate();
  console.log(`PostgreSQL conectado.${DRY_RUN ? ' (DRY-RUN — no se hacen cambios)' : ''}\n`);

  // ── Paso 1: subir prioridad de CC-CLT existentes 74 → 63 ──────────────────
  console.log('── Paso 1: actualizar prioridad CC-CLT 74 → 63 ─────────────────────');

  const cltExistentes = await CfdiMappingRule.findAll({
    where: {
      nombre:   { [Op.like]: 'Reg CC-CLT%' },
      isActive: true,
    },
  });

  if (!cltExistentes.length) {
    console.log('  ⚠ No se encontraron reglas CC-CLT. Crea o sincroniza el seed primero.');
  }

  for (const r of cltExistentes) {
    if (r.prioridad === 63) {
      console.log(`  [sin cambio]  ${r.nombre}  ya tiene prio 63`);
    } else {
      console.log(`  [actualizar]  ${r.nombre}  prio ${r.prioridad} → 63`);
      if (!DRY_RUN) await r.update({ prioridad: 63 });
    }
  }

  // ── Paso 2: crear/actualizar reglas de capa 1 (prio 62) ───────────────────
  console.log('\n── Paso 2: crear/actualizar reglas CLT-DEV y CLT-CAN (prio 62) ─────');

  for (const datos of NUEVAS_CAPA1) {
    const existe = await CfdiMappingRule.findOne({ where: { nombre: datos.nombre } });
    if (existe) {
      const campos = Object.keys(datos).filter(k => k !== 'nombre');
      const difiere = campos.some(k => String(existe[k] ?? '') !== String(datos[k] ?? ''));
      if (!difiere) {
        console.log(`  [sin cambio]  ${datos.nombre}`);
      } else {
        console.log(`  [actualizar]  ${datos.nombre}`);
        if (!DRY_RUN) await existe.update(datos);
      }
    } else {
      console.log(`  [crear]       ${datos.nombre}  prio:${datos.prioridad}  cargo:${datos.cuentaCargo}  abono:${datos.cuentaAbono}  tipoOrigen:${datos.tipoOrigen}`);
      if (!DRY_RUN) await CfdiMappingRule.create(datos);
    }
  }

  // ── Resumen ────────────────────────────────────────────────────────────────
  console.log('\n── Prioridades resultantes para NCs "Club Tuberos" ─────────────────');
  console.log('  62  CLT-DEV/CLT-CAN  (club tuberos + Devolución/Cancelación) → 4200010001, 2103090002');
  console.log('  63  CC-CLT           (club tuberos, cualquier origen)         → 4200020001, 2103090002');
  console.log('  65  TO-DEV club t    (tipoOrigen=Devolución, sin concepto)    → 4200010001, 1103010001');
  console.log('  66  TO-DEV otros     (tipoOrigen=Devolución, genérico)        → 4200010001, 2103090001');
  console.log('  70  TO-BON club t    (tipoOrigen=Bonificación, sin concepto)  → 4200020001, 1103010001');
  console.log('  71  TO-BON otros     (tipoOrigen=Bonificación, genérico)      → 4200020001, 2103090001');

  if (DRY_RUN) {
    console.log('\nDRY-RUN completado. Ejecuta sin --dry-run para aplicar.');
  } else {
    console.log('\n✓ Doble validación Club Tuberos configurada.');
    console.log('Regenera la balanza en NUMO para ver el efecto.');
  }

  await sequelize.close();
  process.exit(0);
}

run().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
