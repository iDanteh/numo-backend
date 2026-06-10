'use strict';

/**
 * add-anticipo-fallback-rules.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Agrega dos reglas fallback para CFDIs de recepción de anticipo cuyo primer
 * concepto contiene la palabra 'anticipo' en la descripción, pero cuya
 * claveProdServ NO es 84111506 (el código que usan las Reg 22/22A).
 *
 * PROBLEMA QUE RESUELVE:
 *   Las reglas Reg 22A (prio 9, formaPago=01) y Reg 22 (prio 9) exigen
 *   claveProdServ='84111506' exacto. Si los CFDIs de anticipo de la empresa
 *   usan otro código SAT, no hacen match y los CFDIs caen en una regla
 *   genérica que NO abona 2103010001, dejando esa cuenta sin movimientos
 *   de crédito en la balanza.
 *
 * SOLUCIÓN:
 *   Reg 22C-DESC (prio 8, formaPago=01) → anticipo en efectivo
 *   Reg 22C      (prio 8, sin formaPago) → anticipo en banco/genérico
 *
 *   Ambas usan conceptoContiene='anticipo' como criterio de detección.
 *   Prioridad 8 < 9 (Reg 22/22A) garantiza que si un CFDI tiene claveProdServ
 *   84111506 Y concepto 'anticipo', la regla más específica (por spec score)
 *   gana; y si no tiene la clave exacta, estas reglas son las que matchean.
 *
 * CUENTAS:
 *   cuentaAbono  = 2103010001  Anticipos De Clientes General   (HABER — pasivo)
 *   cuentaCargo  = 1101010003  Caja                            (DEBE — efectivo)
 *     ó          = 1102011005  Bancos                          (DEBE — transferencia/cheque)
 *   cuentaIva    = 2104010001  IVA Trasladado general          (HABER)
 *
 * Es idempotente: verifica existencia por nombre antes de insertar (upsert).
 *
 * Uso:
 *   node src/banks/scripts/add-anticipo-fallback-rules.js
 *   node src/banks/scripts/add-anticipo-fallback-rules.js --dry-run
 */

require('dotenv').config();

const { sequelize }   = require('../../config/database.postgres');
const CfdiMappingRule = require('../../shared/models/postgres/CfdiMappingRule');

const DRY_RUN = process.argv.includes('--dry-run');

// ── Definición de las reglas fallback ─────────────────────────────────────────
const NUEVAS_REGLAS = [
  // ── Reg 22C-DESC: anticipo cobrado en efectivo (formaPago=01) ───────────────
  // Prioridad 8 — gana sobre Reg 22A (prio 9) cuando la clave no es 84111506.
  // Cuando el CFDI SÍ tiene claveProdServ=84111506, Reg 22A gana por spec score
  // (tiene un filtro extra: claveProdServ) aunque ambas tengan prioridad 9/8.
  {
    nombre:           'Reg 22C-DESC — Recepción Anticipo por Descripción Efectivo',
    tipoComprobante:  'I',
    formaPago:        '01',
    conceptoContiene: 'anticipo',
    cuentaCargo:      '1101010003',  // Caja (formaPago=01 → cobro en efectivo)
    cuentaAbono:      '2103010001',  // Anticipos De Clientes General (pasivo — HABER)
    cuentaIva:        '2104010001',  // IVA Trasladado general (HABER)
    prioridad:        8,
    isActive:         true,
  },

  // ── Reg 22C: anticipo cobrado por banco / forma de pago genérica ────────────
  // Sin restricción de formaPago → captura transferencias, cheques, tarjetas, etc.
  // Prioridad 8 — gana sobre Reg 22 (prio 9, claveProdServ=84111506).
  {
    nombre:           'Reg 22C — Recepción Anticipo por Descripción',
    tipoComprobante:  'I',
    formaPago:        null,
    conceptoContiene: 'anticipo',
    cuentaCargo:      '1102011005',  // Bancos (dinero recibido por transferencia)
    cuentaAbono:      '2103010001',  // Anticipos De Clientes General (pasivo — HABER)
    cuentaIva:        '2104010001',  // IVA Trasladado general (HABER)
    prioridad:        8,
    isActive:         true,
  },
];

async function run() {
  await sequelize.authenticate();
  console.log('PostgreSQL conectado.');
  console.log('');

  if (DRY_RUN) {
    console.log('*** MODO DRY-RUN — no se realizan cambios en base de datos ***\n');
  }

  // Cargar reglas existentes para validación previa
  const reglasExistentes = await CfdiMappingRule.findAll({
    where:  { isActive: true },
    order:  [['prioridad', 'ASC']],
    raw:    true,
  });

  // Advertir si existen reglas en conflicto de prioridad 9 con conceptoContiene=anticipo
  const conflictos = reglasExistentes.filter(r =>
    r.tipoComprobante === 'I' &&
    r.conceptoContiene?.toLowerCase().includes('anticipo') &&
    r.prioridad <= 8,
  );
  if (conflictos.length > 0) {
    console.log('ADVERTENCIA: Ya existen reglas con conceptoContiene=anticipo y prioridad <= 8:');
    conflictos.forEach(r =>
      console.log(`  [prio ${r.prioridad}] ${r.nombre}`),
    );
    console.log('  Verifique que no habrá conflicto de prioridad antes de continuar.\n');
  }

  let insertadas = 0;
  let omitidas   = 0;

  for (const datos of NUEVAS_REGLAS) {
    const existe = await CfdiMappingRule.findOne({ where: { nombre: datos.nombre } });

    if (DRY_RUN) {
      if (existe) {
        console.log(`  [DRY-RUN] Ya existe — se omitiría: "${datos.nombre}"`);
        omitidas++;
      } else {
        console.log(`  [DRY-RUN] Se insertaría: "${datos.nombre}"`);
        console.log(`            prio=${datos.prioridad}, formaPago=${datos.formaPago ?? '(null)'}`);
        console.log(`            cargo=${datos.cuentaCargo}  abono=${datos.cuentaAbono}  iva=${datos.cuentaIva}`);
        insertadas++;
      }
      continue;
    }

    // Modo real
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
  if (DRY_RUN) {
    console.log(`Dry-run completado: ${insertadas} se insertarían, ${omitidas} ya existen.`);
  } else {
    console.log(`Migración completada: ${insertadas} insertadas, ${omitidas} ya existían.`);
  }

  console.log('');
  console.log('PRÓXIMOS PASOS:');
  console.log('  1. Verificar con diag-anticipos.js que los CFDIs de anticipo ahora');
  console.log('     matchean estas reglas y abona la cuenta 2103010001.');
  console.log('  2. Regenerar la balanza para el periodo y confirmar:');
  console.log('     • 2103010001 Anticipos De Clientes: saldo acreedor correcto');
  console.log('     • 2103090001 Anticipos Otros: saldo acreedor correcto');
  console.log('  3. Si hay anticipos cobrados por Otros (cuenta 2103090001 en lugar');
  console.log('     de 2103010001), agregar reglas análogas con cuentaAbono=2103090001');
  console.log('     y conceptoContiene más específico (ej. \'anticipo otros\').');

  await sequelize.close();
  process.exit(0);
}

run().catch(err => {
  console.error('[add-anticipo-fallback-rules] Error:', err.message);
  process.exit(1);
});
