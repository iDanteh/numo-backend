'use strict';

/**
 * migrate-cfdi-mapping-conceptos.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Migración: agrega las 15 reglas CC (por conceptoContiene) para sub-clasificar
 * notas de crédito tipo E según la descripción del primer concepto del CFDI.
 *
 * Tipos cubiertos:
 *   CC1 (prio 74) — Bonificación Club Tuberos  → Descuentos + Monedero 2103090002
 *   CC2 (prio 75) — Bonificación genérica      → Descuentos s/Ventas + Bancos/Caja
 *   CC3 (prio 76) — Devolución de Cliente      → Devoluciones s/Ventas + Bancos/Caja
 *   CC4 (prio 77) — Cancelación de Cliente     → Devoluciones s/Ventas + Bancos/Caja
 *   CC5 (prio 78) — Aplicación de Anticipo     → Anticipos + Clientes (motor anticipo)
 *
 * POR QUÉ se necesitan estas reglas:
 *   Sin ellas, una NC E+tipoRelacion=01+tasaIva=16 con descripción "BONIFICACIÓN"
 *   cae en Reg 8A (prio 80 → Devoluciones 4200010001) en lugar de Reg 20
 *   (prio 88 → Descuentos 4200020001). La prioridad numérica gana siempre;
 *   las CC-rules a prio 74-78 fuerzan la ruta correcta.
 *
 * Es idempotente: verifica existencia por nombre antes de insertar.
 *
 * Uso:
 *   node src/banks/scripts/migrate-cfdi-mapping-conceptos.js
 *   docker exec numo-backend node src/banks/scripts/migrate-cfdi-mapping-conceptos.js
 */

require('dotenv').config();

const { sequelize }   = require('../../config/database.postgres');
const CfdiMappingRule = require('../../shared/models/postgres/CfdiMappingRule');

const nuevasReglas = [

  // ── CC1. BONIFICACIÓN CLUB TUBEROS (prio 74) ──────────────────────────────
  // 'club tuberos' es substring de 'bonificacion club tuberos'.toLowerCase()
  // Gana sobre CC-BON (prio 75) por prioridad numérica menor.
  // cuentaAbono = 2103090002: la bonificación acredita el monedero del cliente.
  {
    nombre:           'Reg CC-CLT-16 — NC Bonificación Club Tuberos 16%',
    tipoComprobante:  'E',
    tasaIva:          '16',
    conceptoContiene: 'club tuberos',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '2103090002',  // Anticipos Otros Club Tuberos (acredita monedero)
    cuentaIva:        '2104010001',  // IVA Trasladado
    prioridad:        74,
    isActive:         true,
  },
  {
    nombre:           'Reg CC-CLT-0 — NC Bonificación Club Tuberos 0%',
    tipoComprobante:  'E',
    tasaIva:          '0',
    conceptoContiene: 'club tuberos',
    cuentaCargo:      '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:      '2103090002',  // Anticipos Otros Club Tuberos
    cuentaIva:        null,
    prioridad:        74,
    isActive:         true,
  },

  // ── CC2. BONIFICACIÓN GENÉRICA (prio 75) ─────────────────────────────────
  // 'bonificaci' captura 'BONIFICACIÓN'.toLowerCase() y 'Bonificacion'.toLowerCase()
  // (ambas ortografías, con y sin acento en la o).
  {
    nombre:           'Reg CC-BON-16-EF — NC Bonificación 16% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '16',
    conceptoContiene: 'bonificaci',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja (formaPago=01 → efectivo)
    cuentaIva:        '2104010001',
    prioridad:        75,
    isActive:         true,
  },
  {
    nombre:           'Reg CC-BON-16 — NC Bonificación 16%',
    tipoComprobante:  'E',
    tasaIva:          '16',
    conceptoContiene: 'bonificaci',
    cuentaCargo:      '4200020001',  // Descuentos s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        75,
    isActive:         true,
  },
  {
    nombre:           'Reg CC-BON-0-EF — NC Bonificación 0% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '0',
    conceptoContiene: 'bonificaci',
    cuentaCargo:      '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        null,
    prioridad:        75,
    isActive:         true,
  },
  {
    nombre:           'Reg CC-BON-0 — NC Bonificación 0%',
    tipoComprobante:  'E',
    tasaIva:          '0',
    conceptoContiene: 'bonificaci',
    cuentaCargo:      '4200020002',  // Descuentos s/Ventas 0%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        null,
    prioridad:        75,
    isActive:         true,
  },

  // ── CC3. DEVOLUCIÓN DE CLIENTE (prio 76) ──────────────────────────────────
  // DEBE Devoluciones s/Ventas, HABER Bancos/Caja (reembolso al cliente).
  {
    nombre:           'Reg CC-DEV-16-EF — NC Devolución de Cliente 16% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '16',
    conceptoContiene: 'devolucion',
    cuentaCargo:      '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        '2104010001',
    prioridad:        76,
    isActive:         true,
  },
  {
    nombre:           'Reg CC-DEV-16 — NC Devolución de Cliente 16%',
    tipoComprobante:  'E',
    tasaIva:          '16',
    conceptoContiene: 'devolucion',
    cuentaCargo:      '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        76,
    isActive:         true,
  },
  {
    nombre:           'Reg CC-DEV-0-EF — NC Devolución de Cliente 0% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '0',
    conceptoContiene: 'devolucion',
    cuentaCargo:      '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        null,
    prioridad:        76,
    isActive:         true,
  },
  {
    nombre:           'Reg CC-DEV-0 — NC Devolución de Cliente 0%',
    tipoComprobante:  'E',
    tasaIva:          '0',
    conceptoContiene: 'devolucion',
    cuentaCargo:      '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        null,
    prioridad:        76,
    isActive:         true,
  },

  // ── CC4. CANCELACIÓN DE CLIENTE (prio 77) ─────────────────────────────────
  // Tratamiento idéntico a Devolución: DEBE Devoluciones, HABER Bancos/Caja.
  {
    nombre:           'Reg CC-CAN-16-EF — NC Cancelación de Cliente 16% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '16',
    conceptoContiene: 'cancelacion',
    cuentaCargo:      '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        '2104010001',
    prioridad:        77,
    isActive:         true,
  },
  {
    nombre:           'Reg CC-CAN-16 — NC Cancelación de Cliente 16%',
    tipoComprobante:  'E',
    tasaIva:          '16',
    conceptoContiene: 'cancelacion',
    cuentaCargo:      '4200010001',  // Devoluciones s/Ventas 16%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        '2104010001',
    prioridad:        77,
    isActive:         true,
  },
  {
    nombre:           'Reg CC-CAN-0-EF — NC Cancelación de Cliente 0% Efectivo',
    tipoComprobante:  'E',
    formaPago:        '01',
    tasaIva:          '0',
    conceptoContiene: 'cancelacion',
    cuentaCargo:      '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:      '1101010003',  // Caja
    cuentaIva:        null,
    prioridad:        77,
    isActive:         true,
  },
  {
    nombre:           'Reg CC-CAN-0 — NC Cancelación de Cliente 0%',
    tipoComprobante:  'E',
    tasaIva:          '0',
    conceptoContiene: 'cancelacion',
    cuentaCargo:      '4200010002',  // Devoluciones s/Ventas 0%
    cuentaAbono:      '1102011005',  // Bancos
    cuentaIva:        null,
    prioridad:        77,
    isActive:         true,
  },

  // ── CC5. APLICACIÓN DE ANTICIPO (prio 78) ─────────────────────────────────
  // Motor: cuentaIvaAnticipo activa la lógica de swap IVA-diferido → IVA-definitivo.
  // Cuando tipoRelacion=07 está presente, Reg 23 (prio 90) también matchea;
  // CC-ANT gana por prioridad (78 < 90) con asiento contable idéntico.
  {
    nombre:            'Reg CC-ANT — NC Aplicación de Anticipo (por descripción)',
    tipoComprobante:   'E',
    conceptoContiene:  'anticipo',
    cuentaCargo:       '2103010001',  // Anticipos De Clientes General (cancela pasivo)
    cuentaAbono:       '1103010001',  // Clientes Nac Gral 16% (reduce CxC)
    cuentaIva:         '2104010001',  // IVA Trasladado definitivo (HABER)
    cuentaIvaAnticipo: '2104010002',  // IVA Trasladado Anticipos (DEBE — cancela diferido)
    prioridad:         78,
    isActive:          true,
  },
];

async function run() {
  await sequelize.authenticate();
  console.log('PostgreSQL conectado.');
  console.log('');

  let insertadas = 0;
  let omitidas   = 0;

  for (const datos of nuevasReglas) {
    const existe = await CfdiMappingRule.findOne({ where: { nombre: datos.nombre } });
    if (existe) {
      console.log(`  OK (ya existía): "${datos.nombre}"`);
      omitidas++;
    } else {
      await CfdiMappingRule.create(datos);
      console.log(`  Insertada: "${datos.nombre}"`);
      insertadas++;
    }
  }

  console.log('');
  console.log(`Migración completada: ${insertadas} insertadas, ${omitidas} ya existían.`);
  await sequelize.close();
  process.exit(0);
}

run().catch((err) => {
  console.error('Error en migración:', err);
  process.exit(1);
});
