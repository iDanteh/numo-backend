'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { sequelize } = require('../../config/database.postgres');
const CFDI = require('../../visor/models/CFDI');
const { CfdiMappingRule } = require('../../shared/models/postgres');
const mappingSvc = require('../domains/cfdi-mapping/cfdi-mapping.service');

async function main() {
  await connectMongo();
  await sequelize.authenticate();
  const rules = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']] });
  const rulesPlain = rules.map(r => r.get({ plain: true }));

  // 1. ¿Cuántos cobros tipo P hay en el periodo?
  const cobros = await CFDI.find({
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    ejercicio: 2026, periodo: 2,
    tipoDeComprobante: 'P',
    source: 'SAT', satStatus: 'Vigente', isActive: true,
  }).select('uuid tipoDeComprobante metodoPago formaPago emisor.rfc receptor.rfc subTotal total descuento impuestos conceptos.importe conceptos.Importe conceptos.descuento conceptos.Descuento conceptos.impuestos conceptos.descripcion conceptos.Descripcion complementoPago.totales cfdiRelacionados').lean();

  console.log('=== COBROS TIPO P periodo 2/2026 ===');
  console.log('Total cobros SAT Vigentes:', cobros.length);

  if (!cobros.length) {
    console.log('\n⚠ NO HAY cobros tipo P en MongoDB para este periodo.');
    console.log('  → Por eso no hay HABER en 1103010001 Clientes.');
    console.log('  → ¿Se sincronizaron los complementos de pago del SAT?');
    await disconnectMongo(); sequelize.close(); return;
  }

  // 2. Ver qué reglas aplican
  const byRegla = {};
  let sinRegla = 0;
  for (const c of cobros) {
    const rule = mappingSvc.findRuleInList(c, rulesPlain);
    if (!rule) { sinRegla++; continue; }
    const k = `prio:${rule.prioridad} | ${rule.nombre.substring(0, 45)} | cargo:${rule.cuentaCargo} → abono:${rule.cuentaAbono}`;
    byRegla[k] = (byRegla[k] || 0) + 1;
  }
  console.log('\nDistribución por regla:');
  Object.entries(byRegla).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => console.log(' ', v, '|', k));
  console.log('Sin regla:', sinRegla);

  // 3. ¿Tienen tipoRelacion='04' (serían sustitutos y podrían excluirse)?
  const con04 = cobros.filter(c => c.cfdiRelacionados?.some(r => r.tipoRelacion === '04'));
  const sin04 = cobros.length - con04.length;
  console.log('\nCobros con tipoRelacion=04 (sustitutos):', con04.length);
  console.log('Cobros normales (sin 04):', sin04);

  // 4. ¿Las reglas P tienen cuentaAbono = 1103010001?
  const reglas_P = rulesPlain.filter(r => r.tipoComprobante === 'P');
  console.log('\nReglas para tipo P:', reglas_P.length);
  reglas_P.forEach(r => console.log(
    ' prio:', r.prioridad,
    '| mP:', r.metodoPago || '*',
    '| fP:', r.formaPago || '*',
    '| tasa:', r.tasaIva || '*',
    '| cargo:', r.cuentaCargo,
    '→ abono:', r.cuentaAbono,
    '|', r.nombre?.substring(0, 40)
  ));

  // 5. Total importe de cobros que van a 1103010001 como abono
  const cobrosConRegla = cobros.filter(c => {
    const rule = mappingSvc.findRuleInList(c, rulesPlain);
    return rule?.cuentaAbono === '1103010001';
  });
  const totalAbono = cobrosConRegla.reduce((s, c) => s + Number(c.complementoPago?.totales?.montoTotalPagos || c.total || 0), 0);
  console.log('\nCobros que generarían HABER 1103010001:', cobrosConRegla.length, '| total monto:', totalAbono.toFixed(2));
}

main().then(() => { disconnectMongo(); sequelize.close(); }).catch(console.error);
