'use strict';

/**
 * migrate-cfdi-mapping-cancelacion-tr01.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Migración: agrega reglas CC-BON-M y CC-DTO-M.
 *
 * Problema que resuelve:
 *   Bonificaciones y DTOs con IVA mixto (0%+16%) carecían de regla CC y
 *   caían a Reg 18 (Devoluciones Mixtas). CC-BON-M y CC-DTO-M los rutan
 *   correctamente a Descuentos s/Ventas 16% (4200020001).
 *
 * Reglas insertadas (4 total):
 *   CC-BON-M-EF / CC-BON-M  (prio 75)
 *   CC-DTO-M-EF / CC-DTO-M  (prio 75)
 *
 * Es idempotente: no hace nada si la regla ya existe (busca por nombre).
 *
 * Uso:
 *   node src/banks/scripts/migrate-cfdi-mapping-cancelacion-tr01.js
 *   docker exec numo-backend node src/banks/scripts/migrate-cfdi-mapping-cancelacion-tr01.js
 */

require('dotenv').config();

const { sequelize }   = require('../../config/database.postgres');
const CfdiMappingRule = require('../../shared/models/postgres/CfdiMappingRule');

const nuevasReglas = [
  // ── CC-BON-M: Bonificación mixta ──────────────────────────────────────────
  {
    nombre:           'Reg CC-BON-M-EF — NC Bonificación Mixta Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          'mixto',
    conceptoContiene: 'bonificaci',
    cuentaCargo:      '4200020001',
    cuentaAbono:      '1101010003',
    cuentaIva:        '2104010001',
    prioridad:        75,
    isActive:         true,
  },
  {
    nombre:           'Reg CC-BON-M — NC Bonificación Mixta',
    tipoComprobante:  'E',
    tasaIva:          'mixto',
    conceptoContiene: 'bonificaci',
    cuentaCargo:      '4200020001',
    cuentaAbono:      '1102011005',
    cuentaIva:        '2104010001',
    prioridad:        75,
    isActive:         true,
  },

  // ── CC-DTO-M: Descuento DTO mixto ─────────────────────────────────────────
  {
    nombre:           'Reg CC-DTO-M-EF — NC Descuento DTO Mixto Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          'mixto',
    conceptoContiene: 'dto',
    cuentaCargo:      '4200020001',
    cuentaAbono:      '1101010003',
    cuentaIva:        '2104010001',
    prioridad:        75,
    isActive:         true,
  },
  {
    nombre:           'Reg CC-DTO-M — NC Descuento DTO Mixto',
    tipoComprobante:  'E',
    tasaIva:          'mixto',
    conceptoContiene: 'dto',
    cuentaCargo:      '4200020001',
    cuentaAbono:      '1102011005',
    cuentaIva:        '2104010001',
    prioridad:        75,
    isActive:         true,
  },

];

async function run() {
  await sequelize.authenticate();
  console.log('PostgreSQL conectado.\n');

  let insertadas = 0;
  let yaExistian = 0;

  for (const regla of nuevasReglas) {
    const existe = await CfdiMappingRule.findOne({ where: { nombre: regla.nombre } });
    if (existe) {
      console.log(`OK (ya existía): "${regla.nombre}"`);
      yaExistian++;
    } else {
      await CfdiMappingRule.create(regla);
      console.log(`Insertada: "${regla.nombre}"`);
      insertadas++;
    }
  }

  console.log(`\nMigración completada: ${insertadas} insertadas, ${yaExistian} ya existían.`);
  await sequelize.close();
  process.exit(0);
}

run().catch((err) => {
  console.error('Error en migración:', err);
  process.exit(1);
});
