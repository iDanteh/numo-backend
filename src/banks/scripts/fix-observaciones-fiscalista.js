'use strict';

/**
 * fix-observaciones-fiscalista.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Corrige 3 reglas de mapeo según las observaciones del fiscalista:
 *
 *  1. Reg 22 / 22A — Recepción de Anticipo:
 *     cuentaIva '2104010002' (IVA Trasladado Anticipos) → '2104010001' (IVA Trasladado general)
 *     Razón: usar cuenta separada de anticipos genera saldo negativo si el anticipo
 *     y la factura final caen en meses distintos. El fiscalista recomienda usar la
 *     cuenta general para evitar ese riesgo.
 *
 *  2. Reg 22C — Factura Final Anticipo PUE:
 *     cuentaIvaAnticipo '2104010002' → '2104010001'
 *     Razón: si la recepción usó '2104010001', no hay una cuenta diferida que cancelar.
 *     Al usar la misma cuenta el asiento queda DEBE-HABER en '2104010001' con efecto neto
 *     cero, que es el comportamiento correcto (saldo neto del IVA ya está causado).
 *     Esto también corrige el descuadre del Asiento 3 variante PUE.
 *
 *  3. Reg 17 / 17A — NC Devolución Tasa 0%:
 *     cuentaCargo '4200020002' (Descuentos s/Ventas 0%) → '4200010002' (Devoluciones s/Ventas 0%)
 *     Razón: una devolución de mercancía 0% debe ir a la cuenta de Devoluciones,
 *     no a la de Descuentos. El fiscalista señala esta distinción como obligatoria.
 *
 * Uso:
 *   node src/banks/scripts/fix-observaciones-fiscalista.js
 *
 * Es idempotente: si ya está corregido no hace nada.
 */

require('dotenv').config();

const { sequelize }   = require('../../config/database.postgres');
const CfdiMappingRule = require('../../shared/models/postgres/CfdiMappingRule');

const fixes = [
  // ── Fix 1: Rec. Anticipo Efectivo ─────────────────────────────────────────
  {
    nombre:   'Reg 22A — Recepción de Anticipo Efectivo (ClaveProdServ 84111506)',
    cambios:  { cuentaIva: '2104010001' },
    razon:    'IVA Trasladado general en lugar de cuenta separada de anticipos',
  },
  // ── Fix 2: Rec. Anticipo genérico ─────────────────────────────────────────
  {
    nombre:   'Reg 22 — Recepción de Anticipo (ClaveProdServ 84111506)',
    cambios:  { cuentaIva: '2104010001' },
    razon:    'IVA Trasladado general en lugar de cuenta separada de anticipos',
  },
  // ── Fix 3: Factura Final Anticipo PUE ─────────────────────────────────────
  {
    nombre:   'Reg 22C — Factura Final Anticipo PUE (formaPago 30)',
    cambios:  { cuentaIvaAnticipo: '2104010001' },
    razon:    'Elimina el swap IVA Anticipos↔IVA general; con cuenta unificada el asiento cuadra',
  },
  // ── Fix 4: NC Devolución Tasa 0% ──────────────────────────────────────────
  {
    nombre:   'Reg 17 — NC Devolución Tasa 0%',
    cambios:  { cuentaCargo: '4200010002' },
    razon:    'Devoluciones s/Ventas 0% (4200010002) en lugar de Descuentos s/Ventas 0% (4200020002)',
  },
  // ── Fix 5: NC Devolución Tasa 0% Efectivo ─────────────────────────────────
  {
    nombre:   'Reg 17A — NC Devolución Tasa 0% Efectivo',
    cambios:  { cuentaCargo: '4200010002' },
    razon:    'Devoluciones s/Ventas 0% (4200010002) en lugar de Descuentos s/Ventas 0% (4200020002)',
  },
];

async function main() {
  await sequelize.authenticate();

  let aplicados = 0, omitidos = 0, noEncontrados = 0;

  for (const fix of fixes) {
    const regla = await CfdiMappingRule.findOne({ where: { nombre: fix.nombre } });

    if (!regla) {
      console.warn(`  ⚠ No encontrada: "${fix.nombre}"`);
      noEncontrados++;
      continue;
    }

    // Verificar si ya tiene el valor correcto
    const yaCorregida = Object.entries(fix.cambios).every(([k, v]) => regla[k] === v);
    if (yaCorregida) {
      console.log(`  ✓ Ya corregida: "${fix.nombre}"`);
      omitidos++;
      continue;
    }

    await regla.update(fix.cambios);
    console.log(`  ✅ Corregida: "${fix.nombre}"`);
    console.log(`     → ${fix.razon}`);
    aplicados++;
  }

  console.log(`\nResumen:`);
  console.log(`  Aplicadas:     ${aplicados}`);
  console.log(`  Ya correctas:  ${omitidos}`);
  console.log(`  No encontradas: ${noEncontrados}\n`);

  await sequelize.close();
}

main().catch(err => { console.error(err); process.exit(1); });
