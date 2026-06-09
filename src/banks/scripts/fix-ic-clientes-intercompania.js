'use strict';

/**
 * fix-ic-clientes-intercompania.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Corrige directamente en BD las reglas de Intercompañías:
 *
 *  1. Reg IC-I-PPD (facturas PPD intercompañía)
 *     cuentaCargo: 1103010001 → 1103020001 (Clientes Intercompañías)
 *
 *  2. Reg IC-P-16 (cobros PPD intercompañía, tipo P)
 *     Crea las 7 reglas si no existen (una por RFC del grupo).
 *
 * Seguro de ejecutar múltiples veces (idempotente).
 *
 * Uso:
 *   node src/banks/scripts/fix-ic-clientes-intercompania.js
 *   node src/banks/scripts/fix-ic-clientes-intercompania.js --dry-run
 */

require('dotenv').config();

const { sequelize }       = require('../../config/database.postgres');
const { CfdiMappingRule } = require('../../shared/models/postgres');
const { Op }              = require('sequelize');

const DRY_RUN = process.argv.includes('--dry-run');

// Los 7 RFC intercompañía del grupo
const RFC_INTERCOMPANIA = [
  'KTE180215FE1',
  'RSI051018GL6',
  'FEUL5811155D9',
  'GAAA5403026G2',
  'GAFA850630542',
  'AVA1002023N7',
  'GIN121109RX4',
];

async function run() {
  await sequelize.authenticate();
  console.log(`PostgreSQL conectado.${DRY_RUN ? ' (DRY-RUN — sin cambios)' : ''}\n`);

  // ── 1. Corregir Reg IC-I-PPD: cuentaCargo 1103010001 → 1103020001 ──────────
  console.log('── Paso 1: IC-I-PPD  cuentaCargo 1103010001 → 1103020001 (Clientes Intercompañías) ──');

  const [nActualizado] = await (DRY_RUN
    ? Promise.resolve([0])
    : CfdiMappingRule.update(
        { cuentaCargo: '1103020001' },
        {
          where: {
            nombre:      { [Op.like]: 'Reg IC-I-PPD%' },
            cuentaCargo: '1103010001',
            isActive:    true,
          },
        }
      ));

  // Contar cuántas hay (para dry-run)
  const nPPD = await CfdiMappingRule.count({
    where: { nombre: { [Op.like]: 'Reg IC-I-PPD%' }, isActive: true },
  });
  console.log(`  Reglas IC-I-PPD encontradas : ${nPPD}`);
  if (DRY_RUN) {
    const porCorregir = await CfdiMappingRule.count({
      where: { nombre: { [Op.like]: 'Reg IC-I-PPD%' }, cuentaCargo: '1103010001', isActive: true },
    });
    console.log(`  Reglas por corregir         : ${porCorregir}`);
  } else {
    console.log(`  Reglas corregidas           : ${nActualizado}`);
  }

  // ── 2. Crear Reg IC-P-16: cobros PPD tipo P ─────────────────────────────────
  console.log('\n── Paso 2: Reg IC-P-16 — cobros PPD tipo P (Clientes Intercompañías) ─────────');

  let creadas = 0, yaExistian = 0;
  for (const rfc of RFC_INTERCOMPANIA) {
    const nombre = `Reg IC-P-16 — Cobro PPD Intercompañía (${rfc})`;
    const existe = await CfdiMappingRule.findOne({ where: { nombre } });

    if (existe) {
      console.log(`  [ya existe]  ${nombre}`);
      yaExistian++;
    } else {
      console.log(`  [crear]      ${nombre}`);
      if (!DRY_RUN) {
        await CfdiMappingRule.create({
          nombre,
          tipoComprobante: 'P',
          rfcEmisor:       rfc,
          tasaIva:         '16',
          cuentaCargo:     '1102011005',   // Bancos (recibe el dinero)
          cuentaAbono:     '1103020001',   // Clientes Intercompañías (liquida CxC)
          cuentaIva:       '2104010001',
          cuentaIvaPPD:    '2105010001',
          prioridad:       5,
          isActive:        true,
        });
      }
      creadas++;
    }
  }

  // ── Resumen ─────────────────────────────────────────────────────────────────
  console.log('\n── Resultado ────────────────────────────────────────────────────────────────');
  if (DRY_RUN) {
    console.log('DRY-RUN completado. Ejecuta sin --dry-run para aplicar los cambios.');
  } else {
    console.log(`✓ IC-I-PPD corregidas  : ${nActualizado}`);
    console.log(`✓ IC-P-16 creadas      : ${creadas}`);
    console.log(`  IC-P-16 ya existían  : ${yaExistian}`);
    console.log('\nEfecto en la balanza:');
    console.log('  Facturas PPD intercompañía → cargo a 1103020001 (Clientes IC) ✓');
    console.log('  Cobros PPD intercompañía   → abono a 1103020001 (Clientes IC) ✓');
  }

  await sequelize.close();
  process.exit(0);
}

run().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
