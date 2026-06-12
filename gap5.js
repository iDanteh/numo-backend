'use strict';
require('dotenv').config();
const mongoose = require('mongoose');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { CfdiMappingRule } = require('./src/shared/models/postgres');
const mappingSvc = require('./src/banks/domains/cfdi-mapping/cfdi-mapping.service');

const RFC       = 'CCO011113663';
const EJERCICIO = 2026;
const PERIODO   = 2;

async function main() {
  await mongoose.connect(process.env.MONGODB_URI);
  await sequelize.authenticate();

  const rules = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']] });

  const cfdis = await CFDI.find({
    $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio: EJERCICIO, periodo: PERIODO,
    tipoDeComprobante: 'E', source: 'SAT', satStatus: 'Vigente', isActive: true,
  }).select('uuid tipoDeComprobante metodoPago formaPago emisor.rfc receptor.rfc subTotal total descuento impuestos conceptos.importe conceptos.Importe conceptos.descuento conceptos.Descuento conceptos.impuestos conceptos.descripcion conceptos.Descripcion complementoPago.totales cfdiRelacionados.tipoRelacion').lean();
  console.log('CFDIs tipo E:', cfdis.length);

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

  const devoluciones = [];
  const descuentos = [];
  let sinRegla = 0;

  for (const cfdi of enriched) {
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (!rule) { sinRegla++; continue; }
    const rawDesc = cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '';
    const sub = Number(cfdi.subTotal) || 0;
    const tipoRel = cfdi.cfdiRelacionados?.[0]?.tipoRelacion || null;
    const obj = { uuid: cfdi.uuid, sub, desc: rawDesc.substring(0, 70), descLow: rawDesc.toLowerCase(), tipoRel, fp: cfdi.formaPago, rule: rule.nombre };
    if (rule.cuentaCargo === '4200010001') devoluciones.push(obj);
    if (rule.cuentaCargo === '4200020001') descuentos.push(obj);
  }

  const sumDev = devoluciones.reduce((s,x) => s+x.sub, 0);
  const sumDesc = descuentos.reduce((s,x) => s+x.sub, 0);
  console.log(`\nDevoluciones 16%: ${sumDev.toFixed(2)} (${devoluciones.length} NCs)`);
  console.log(`Descuentos 16%:   ${sumDesc.toFixed(2)} (${descuentos.length} NCs)`);
  console.log(`CONTPAQi Desc target: 1062770.37, gap: ${(1062770.37 - sumDesc).toFixed(2)}`);
  console.log('Sin regla:', sinRegla);

  // Group devoluciones by description prefix
  const devGroups = {};
  for (const d of devoluciones) {
    const key = d.descLow.substring(0, 40);
    if (!devGroups[key]) devGroups[key] = { count: 0, sum: 0 };
    devGroups[key].count++;
    devGroups[key].sum += d.sub;
  }
  console.log('\n=== TOP GRUPOS EN DEVOLUCIONES ===');
  Object.entries(devGroups).sort((a,b) => b[1].sum-a[1].sum).slice(0, 20).forEach(([k,v]) => {
    console.log(`  count=${v.count} sum=${v.sum.toFixed(2)} | "${k}"`);
  });

  // Find NCs in Devoluciones with unusual descriptions
  const unusual = devoluciones.filter(d => !d.descLow.includes('devoluci') && !d.descLow.includes('cancelaci'));
  console.log(`\nNCs SIN 'devolucion'/'cancelacion': ${unusual.length}, sum=${unusual.reduce((s,x)=>s+x.sub,0).toFixed(2)}`);
  unusual.slice(0, 20).forEach(d => console.log(`  ${d.sub.toFixed(2)} tipoRel=${d.tipoRel} fp=${d.fp} | "${d.desc}"`));

  process.exit(0);
}
main().catch(e => { console.error(e.message, e.stack?.split('\n')[1]); process.exit(1); });
