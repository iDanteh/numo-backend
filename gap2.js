'use strict';
require('dotenv').config();

const mongoose = require('mongoose');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { CfdiMappingRule } = require('./src/shared/models/postgres');
const mappingSvc = require('./src/banks/domains/cfdi-mapping/cfdi-mapping.service');

const RFC       = 'TIH930921KX5';
const EJERCICIO = 2026;
const PERIODO   = 2;

async function main() {
  await mongoose.connect(process.env.MONGODB_URI);
  await sequelize.authenticate();

  const rules = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']] });
  console.log('Rules loaded:', rules.length);

  const cfdis = await CFDI.find({
    $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio: EJERCICIO, periodo: PERIODO,
    tipoDeComprobante: 'E', source: 'SAT', satStatus: 'Vigente', isActive: true,
  }).select('uuid tipoDeComprobante metodoPago formaPago emisor.rfc receptor.rfc subTotal total descuento impuestos conceptos.importe conceptos.Importe conceptos.descuento conceptos.Descuento conceptos.impuestos conceptos.descripcion conceptos.Descripcion complementoPago.totales cfdiRelacionados.tipoRelacion').lean();
  console.log('CFDIs tipo E:', cfdis.length);

  // Enrich
  const uuidsSin = cfdis.filter(c => c.uuid && (!c.formaPago || !c.metodoPago || !c.conceptos?.length || c.conceptos.every(con => !(con.impuestos?.traslados?.length)))).map(c => c.uuid);
  let erpMap = {};
  if (uuidsSin.length) {
    const erps = await CFDI.find({ uuid: { $in: uuidsSin }, source: 'ERP' }).select('uuid formaPago metodoPago conceptos impuestos').lean();
    erpMap = Object.fromEntries(erps.map(c => [c.uuid, c]));
  }

  const enriched = cfdis.map(cfdi => {
    const erp = erpMap[cfdi.uuid];
    if (!erp) return cfdi;
    const satHasTraslados = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    return {
      ...cfdi,
      formaPago:  cfdi.formaPago  || erp.formaPago,
      metodoPago: cfdi.metodoPago || erp.metodoPago,
      conceptos:  satHasTraslados ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos:  satHasTraslados ? cfdi.impuestos : (erp.impuestos ?? cfdi.impuestos),
    };
  });

  // Classify all
  const ruleBuckets = {};
  let sinRegla = 0;
  for (const cfdi of enriched) {
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (!rule) { sinRegla++; continue; }
    const key = rule.nombre;
    if (!ruleBuckets[key]) ruleBuckets[key] = { sum: 0, count: 0, cargo: rule.cuentaCargo };
    ruleBuckets[key].sum += Number(cfdi.subTotal) || 0;
    ruleBuckets[key].count++;
  }
  console.log('\n=== REGLAS USADAS (tipo E) ===');
  Object.entries(ruleBuckets).sort((a,b) => b[1].sum-a[1].sum).forEach(([r, v]) => {
    console.log(`  ${v.count}x ${v.sum.toFixed(2)} cargo=${v.cargo} | ${r}`);
  });
  console.log('Sin regla:', sinRegla);

  process.exit(0);
}

main().catch(e => { console.error(e.message); process.exit(1); });
