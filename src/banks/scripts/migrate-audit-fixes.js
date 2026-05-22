'use strict';

/**
 * migrate-audit-fixes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Aplica las correcciones detectadas en auditoría contable (2026-05-22):
 *
 *   H3 — Reg 1G: cuentaIva 2104010002 → 2104010001 (IVA definitivo en ventas con monedero PUE)
 *   H4 — Reg 17: agrega tieneDescuento=false (NCs 0% con descuento → Reg 19B)
 *   H4 — Reg 19B: inserta nueva regla NC Devolución Tasa 0% con Descuento (prioridad 86)
 *
 * Es idempotente: verifica estado actual antes de cada cambio.
 *
 * Uso:
 *   node src/banks/scripts/migrate-audit-fixes.js
 *   docker exec numo-backend node src/banks/scripts/migrate-audit-fixes.js
 */

require('dotenv').config();

const { sequelize }   = require('../../config/database.postgres');
const CfdiMappingRule = require('../../shared/models/postgres/CfdiMappingRule');
const AccountPlan     = require('../../shared/models/postgres/AccountPlan');

async function run() {
  await sequelize.authenticate();
  console.log('PostgreSQL conectado.\n');

  // ── H3: Reg 1G — IVA definitivo en ventas con monedero PUE ──────────────
  const nombre1G = 'Reg 1G — Venta PUE Monedero Electrónico (Club Tuberos)';
  const reg1G = await CfdiMappingRule.findOne({ where: { nombre: nombre1G } });
  if (!reg1G) {
    console.warn(`  ADVERTENCIA: no se encontró "${nombre1G}" — omitida.`);
  } else if (reg1G.cuentaIva === '2104010001') {
    console.log(`  OK (ya estaba): "${nombre1G}" cuentaIva=2104010001`);
  } else {
    const anterior = reg1G.cuentaIva;
    await reg1G.update({ cuentaIva: '2104010001' });
    console.log(`  Actualizada: "${nombre1G}" cuentaIva ${anterior} → 2104010001`);
  }

  // ── H4a: Reg 17 — agregar tieneDescuento=false ───────────────────────────
  const nombre17 = 'Reg 17 — NC Devolución Tasa 0%';
  const reg17 = await CfdiMappingRule.findOne({ where: { nombre: nombre17 } });
  if (!reg17) {
    console.warn(`  ADVERTENCIA: no se encontró "${nombre17}" — omitida.`);
  } else if (reg17.tieneDescuento === false) {
    console.log(`  OK (ya estaba): "${nombre17}" tieneDescuento=false`);
  } else {
    await reg17.update({ tieneDescuento: false });
    console.log(`  Actualizada: "${nombre17}" → tieneDescuento=false`);
  }

  // ── H4b: Reg 19B — insertar si no existe ────────────────────────────────
  const nombre19B = 'Reg 19B — NC Devolución Tasa 0% con Descuento';
  const existe19B = await CfdiMappingRule.findOne({ where: { nombre: nombre19B } });
  if (existe19B) {
    console.log(`  OK (ya existía): "${nombre19B}"`);
  } else {
    await CfdiMappingRule.create({
      nombre:          nombre19B,
      tipoComprobante: 'E',
      tipoRelacion:    '01',
      tasaIva:         '0',
      tieneDescuento:  true,
      cuentaCargo:     '4200010002',   // Devoluciones s/Ventas 0%
      cuentaAbono:     '1102011005',   // Bancos
      cuentaDescuento: '4200020002',   // Descuentos s/Ventas 0% (HABER — cancela descuento original)
      cuentaIva:       null,
      prioridad:       86,
      isActive:        true,
    });
    console.log(`  Insertada: "${nombre19B}" (prioridad 86)`);
  }

  // ── H-I: Reg 22-0 — Recepción de Anticipo Tasa 0% ─────────────────────────
  // Sin IVA diferido. Gana sobre Reg 22 (prio 9) por mayor especificidad (tasaIva='0').
  const nombreReg220 = 'Reg 22-0 — Recepción de Anticipo Tasa 0% (ClaveProdServ 84111506)';
  const existe220 = await CfdiMappingRule.findOne({ where: { nombre: nombreReg220 } });
  if (existe220) {
    console.log(`  OK (ya existía): "${nombreReg220}"`);
  } else {
    await CfdiMappingRule.create({
      nombre:          nombreReg220,
      tipoComprobante: 'I',
      metodoPago:      'PUE',
      claveProdServ:   '84111506',
      tasaIva:         '0',
      cuentaCargo:     '1102011005',   // Bancos (dinero recibido)
      cuentaAbono:     '2103010001',   // Anticipos de Clientes General
      cuentaIva:       null,           // sin IVA (tasa 0%)
      prioridad:       8,              // < 9 de Reg 22 → más prioritaria
      isActive:        true,
    });
    console.log(`  Insertada: "${nombreReg220}" (prioridad 8)`);
  }

  // ── H-H: Cuenta 4100020002 — Ingresos Por Ventas Crédito 0% ────────────────
  // Las facturas PPD tasa 0% deben abonar a Ingresos Crédito (no Contado).
  const codigo4100020002 = '4100020002';
  let cta4100020002 = await AccountPlan.findOne({ where: { codigo: codigo4100020002 } });
  if (cta4100020002) {
    console.log(`  OK (ya existía): cuenta ${codigo4100020002} — ${cta4100020002.nombre}`);
  } else {
    // Usar parentId de la cuenta hermana 4100020001 (Ingresos Crédito 16%)
    const hermana = await AccountPlan.findOne({ where: { codigo: '4100020001' } });
    cta4100020002 = await AccountPlan.create({
      codigo:   codigo4100020002,
      nombre:   'Ingresos Por Ventas Crédito 0%',
      ctaMayor: '4100020001',
      parentId: hermana?.parentId ?? null,
      isActive: true,
    });
    console.log(`  Insertada: cuenta ${codigo4100020002} — Ingresos Por Ventas Crédito 0%`);
  }

  // ── H-H: Actualizar Reg 11, 6C, 13 → usar 4100020002 en vez de 4100010002 ──
  const h_h_reglas = [
    { nombre: 'Reg 11 — Venta PPD Tasa 0%',           campo: 'cuentaAbono',  valor: '4100020002' },
    { nombre: 'Reg 6C — Venta con Descuento PPD 0%',  campo: 'cuentaAbono',  valor: '4100020002' },
    { nombre: 'Reg 13 — Venta Mixta PPD (0%+16%)',    campo: 'cuentaAbono2', valor: '4100020002' },
  ];
  for (const { nombre, campo, valor } of h_h_reglas) {
    const reg = await CfdiMappingRule.findOne({ where: { nombre } });
    if (!reg) {
      console.warn(`  ADVERTENCIA: no se encontró "${nombre}" — omitida.`);
    } else if (reg[campo] === valor) {
      console.log(`  OK (ya estaba): "${nombre}" ${campo}=${valor}`);
    } else {
      const anterior = reg[campo];
      await reg.update({ [campo]: valor });
      console.log(`  Actualizada: "${nombre}" ${campo} ${anterior} → ${valor}`);
    }
  }

  // ── H-D: Cobros PPD Tasa 0% — CxC correcta (1103010002) ───────────────────
  // Para cada formaPago existente en cobros 16%, se agrega una variante 0% con
  // mayor especificidad (tasaIva='0'). El motor elige la más específica en empate.
  const cobros0 = [
    { nombre: 'Reg 7A-0 — Cobro PPD Efectivo Tasa 0%',           formaPago: '01', cuentaCargo: '1101010003', prioridad: 70 },
    { nombre: 'Reg 7B-0 — Cobro PPD Transferencia Tasa 0%',      formaPago: '03', cuentaCargo: '1102011005', prioridad: 71 },
    { nombre: 'Reg 7C-0 — Cobro PPD Cheque Tasa 0%',             formaPago: '04', cuentaCargo: '1102011005', prioridad: 72 },
    { nombre: 'Reg 7F-0 — Cobro PPD Cheque Nominativo Tasa 0%',  formaPago: '02', cuentaCargo: '1102011005', prioridad: 72 },
    { nombre: 'Reg 7D-0 — Cobro PPD Tarjeta Débito Tasa 0%',     formaPago: '28', cuentaCargo: '1102011005', prioridad: 73 },
    { nombre: 'Reg 7E-0 — Cobro PPD Tarjeta Crédito Tasa 0%',    formaPago: '29', cuentaCargo: '1102011005', prioridad: 74 },
    { nombre: 'Reg 7G-0 — Cobro PPD Monedero Electrónico Tasa 0%', formaPago: '05', cuentaCargo: '2103090002', prioridad: 70 },
  ];
  for (const { nombre, formaPago, cuentaCargo, prioridad } of cobros0) {
    const existe = await CfdiMappingRule.findOne({ where: { nombre } });
    if (existe) {
      console.log(`  OK (ya existía): "${nombre}"`);
    } else {
      await CfdiMappingRule.create({
        nombre,
        tipoComprobante: 'P',
        formaPago,
        tasaIva:         '0',
        cuentaCargo,
        cuentaAbono:     '1103010002',  // Clientes Nac Gral 0% (CxC correcta)
        cuentaIva:       null,           // tasa 0%: sin IVA a reconocer
        cuentaIvaPPD:    null,           // tasa 0%: la factura PPD-0% no generó IVA puente
        prioridad,
        isActive:        true,
      });
      console.log(`  Insertada: "${nombre}" (formaPago=${formaPago}, prio=${prioridad})`);
    }
  }

  console.log('\nMigracion de auditoría completada.');
  await sequelize.close();
  process.exit(0);
}

run().catch((err) => {
  console.error('Error en migración:', err);
  process.exit(1);
});
