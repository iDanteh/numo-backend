'use strict';

/**
 * migrate-cfdi-mapping-ppd-descuento.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Migración: agrega soporte de descuentos a las reglas PPD de ingresos.
 *
 * Cambios:
 *   1. Reg 6  — agrega tieneDescuento=false (ya no captura CFDIs con descuento)
 *   2. Reg 11 — agrega tieneDescuento=false (ya no captura CFDIs con descuento)
 *   3. Reg 13 — agrega tieneDescuento=false (ya no captura CFDIs mixtos con descuento)
 *   4. Reg 6B — inserta nueva regla PPD 16% con descuento (prioridad 59)
 *   5. Reg 6C — inserta nueva regla PPD 0%  con descuento (prioridad 64)
 *
 * Es idempotente: verifica existencia antes de insertar / actualizar.
 *
 * Uso:
 *   node src/banks/scripts/migrate-cfdi-mapping-ppd-descuento.js
 *   docker exec numo-backend node src/banks/scripts/migrate-cfdi-mapping-ppd-descuento.js
 */

require('dotenv').config();

const { sequelize }     = require('../../config/database.postgres');
const CfdiMappingRule   = require('../../shared/models/postgres/CfdiMappingRule');

async function run() {
  await sequelize.authenticate();
  console.log('PostgreSQL conectado.');

  // ── 1. Actualizar Reg 6, 11, 13: agregar tieneDescuento=false ────────────
  const reglasActualizar = [
    'Reg 6 — Venta PPD 16% (Factura a Crédito)',
    'Reg 11 — Venta PPD Tasa 0%',
    'Reg 13 — Venta Mixta PPD (0%+16%)',
  ];

  for (const nombre of reglasActualizar) {
    const regla = await CfdiMappingRule.findOne({ where: { nombre } });
    if (!regla) {
      console.warn(`  ADVERTENCIA: no se encontró la regla "${nombre}" — omitida.`);
      continue;
    }
    if (regla.tieneDescuento === false) {
      console.log(`  OK (ya estaba): "${nombre}"`);
      continue;
    }
    await regla.update({ tieneDescuento: false });
    console.log(`  Actualizada: "${nombre}" → tieneDescuento=false`);
  }

  // ── 2. Insertar Reg 6B — Venta con Descuento PPD 16% ─────────────────────
  const nombre6B = 'Reg 6B — Venta con Descuento PPD 16%';
  const existe6B = await CfdiMappingRule.findOne({ where: { nombre: nombre6B } });
  if (existe6B) {
    console.log(`  OK (ya existía): "${nombre6B}"`);
  } else {
    await CfdiMappingRule.create({
      nombre:          nombre6B,
      tipoComprobante: 'I',
      metodoPago:      'PPD',
      formaPago:       '99',
      tasaIva:         '16',
      tieneDescuento:  true,
      cuentaCargo:     '1103010001',   // Clientes Nac Gral 16%
      cuentaAbono:     '4100020001',   // Ingresos Crédito 16%
      cuentaIvaPPD:    '2105010001',   // IVA Por Trasladar PPD (columna BD: cuenta_iva_p_p_d)
      cuentaDescuento: '4200020001',   // Descuentos s/Ventas 16%
      prioridad:       59,
      isActive:        true,
    });
    console.log(`  Insertada: "${nombre6B}"`);
  }

  // ── 3. Insertar Reg 6C — Venta con Descuento PPD 0% ──────────────────────
  const nombre6C = 'Reg 6C — Venta con Descuento PPD 0%';
  const existe6C = await CfdiMappingRule.findOne({ where: { nombre: nombre6C } });
  if (existe6C) {
    console.log(`  OK (ya existía): "${nombre6C}"`);
  } else {
    await CfdiMappingRule.create({
      nombre:          nombre6C,
      tipoComprobante: 'I',
      metodoPago:      'PPD',
      formaPago:       '99',
      tasaIva:         '0',
      tieneDescuento:  true,
      cuentaCargo:     '1103010002',   // Clientes Nac Gral 0%
      cuentaAbono:     '4100010002',   // Ingresos Contado 0%
      cuentaIvaPPD:    null,
      cuentaDescuento: '4200020002',   // Descuentos s/Ventas 0%
      prioridad:       64,
      isActive:        true,
    });
    console.log(`  Insertada: "${nombre6C}"`);
  }

  console.log('\nMigracion completada.');
  await sequelize.close();
  process.exit(0);
}

run().catch((err) => {
  console.error('Error en migración:', err);
  process.exit(1);
});
