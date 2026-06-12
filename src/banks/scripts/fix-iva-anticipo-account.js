'use strict';

/**
 * fix-iva-anticipo-account.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Actualiza cuentaIva de las 4 reglas de RECEPCIÓN de anticipos:
 *   2104010001 (IVA Trasladado definitivo) → 2104010002 (IVA Trasladado – Anticipos diferido)
 *
 * Reglas afectadas:
 *   Reg 22A        (claveProdServ=84111506, formaPago=01)
 *   Reg 22         (claveProdServ=84111506, sin formaPago)
 *   Reg 22C-DESC   (conceptoContiene=anticipo, formaPago=01)
 *   Reg 22C        (conceptoContiene=anticipo, sin formaPago)
 *
 * Uso:
 *   node src/banks/scripts/fix-iva-anticipo-account.js
 *   node src/banks/scripts/fix-iva-anticipo-account.js --dry-run
 */

require('dotenv').config();

const { sequelize }   = require('../../config/database.postgres');
const CfdiMappingRule = require('../../shared/models/postgres/CfdiMappingRule');

const DRY_RUN = process.argv.includes('--dry-run');

const REGLAS_A_CORREGIR = [
  'Reg 22A — Recepción de Anticipo Efectivo (ClaveProdServ 84111506)',
  'Reg 22 — Recepción de Anticipo (ClaveProdServ 84111506)',
  'Reg 22C-DESC — Recepción Anticipo por Descripción Efectivo',
  'Reg 22C — Recepción Anticipo por Descripción',
];

async function run() {
  await sequelize.authenticate();
  console.log(`PostgreSQL conectado.${DRY_RUN ? ' (DRY-RUN)' : ''}\n`);

  let actualizadas = 0;
  let noEncontradas = 0;
  let yaCorrectas = 0;

  for (const nombre of REGLAS_A_CORREGIR) {
    const regla = await CfdiMappingRule.findOne({ where: { nombre } });

    if (!regla) {
      console.log(`  [no encontrada]  ${nombre}`);
      noEncontradas++;
      continue;
    }

    if (regla.cuentaIva === '2104010002') {
      console.log(`  [ya correcta]    ${nombre}  (cuentaIva=${regla.cuentaIva})`);
      yaCorrectas++;
      continue;
    }

    console.log(`  [actualizar]     ${nombre}`);
    console.log(`                   cuentaIva: ${regla.cuentaIva} → 2104010002`);

    if (!DRY_RUN) {
      await regla.update({ cuentaIva: '2104010002' });
    }
    actualizadas++;
  }

  console.log('');
  if (DRY_RUN) {
    console.log(`Dry-run: ${actualizadas} se actualizarían, ${yaCorrectas} ya correctas, ${noEncontradas} no encontradas.`);
  } else {
    console.log(`Completado: ${actualizadas} actualizadas, ${yaCorrectas} ya correctas, ${noEncontradas} no encontradas.`);
  }

  if (actualizadas > 0 && !DRY_RUN) {
    console.log('\nEFECTO EN LA BALANZA (próximas pólizas generadas):');
    console.log('  • Anticipos recibidos → HABER 2104010002 (IVA diferido) en lugar de 2104010001');
    console.log('  • Factura final (Reg 22C) → DEBE 2104010002 + HABER 2104010001 (swap correcto)');
    console.log('  • 2104010002 tendrá saldo ACREEDOR (pasivo) en lugar de deudor');
  }

  await sequelize.close();
  process.exit(0);
}

run().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
