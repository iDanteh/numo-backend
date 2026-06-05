'use strict';

/**
 * fix-obs3-abono-pendiente.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Observación 3 del fiscalista: el abono de NCs (tipo E) sin formaPago
 * debe ir a "Anticipos Otros" (2103090001) en lugar de Bancos (1102011005),
 * porque sin formaPago el reembolso es PENDIENTE — aún no se pagó en efectivo.
 *
 * Solo afecta reglas de tipo E donde:
 *   - formaPago IS NULL  (reembolso desconocido/pendiente)
 *   - cuentaAbono = '1102011005' (Bancos)
 *
 * Las reglas con formaPago='01','02','03','04','28','29' etc. SE CONSERVAN
 * porque esas sí implican reembolso real (efectivo/cheque/transferencia).
 *
 * Uso:
 *   node src/banks/scripts/fix-obs3-abono-pendiente.js
 */

require('dotenv').config();

const { sequelize }   = require('../../config/database.postgres');
const CfdiMappingRule = require('../../shared/models/postgres/CfdiMappingRule');
const { Op }          = require('sequelize');

async function main() {
  await sequelize.authenticate();

  // Buscar reglas afectadas
  const reglas = await CfdiMappingRule.findAll({
    where: {
      tipoComprobante: 'E',
      formaPago:       null,
      cuentaAbono:     '1102011005',  // Bancos → cambiar a Anticipos Otros
    },
  });

  if (reglas.length === 0) {
    console.log('✓ Ninguna regla necesita cambio (ya corregidas o no existen).');
    await sequelize.close();
    return;
  }

  console.log(`\nReglas tipo E sin formaPago con Bancos como abono: ${reglas.length}`);
  console.log('Cambiando cuentaAbono 1102011005 → 2103090001 (Anticipos Otros)...\n');

  for (const r of reglas) {
    await r.update({ cuentaAbono: '2103090001' });
    console.log(`  ✅ ${r.nombre}`);
  }

  console.log(`\nTotal corregidas: ${reglas.length}`);
  console.log('Las reglas con formaPago específica (01, 03, 04, etc.) no fueron modificadas.\n');

  await sequelize.close();
}

main().catch(err => { console.error(err); process.exit(1); });
