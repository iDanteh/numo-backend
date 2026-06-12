'use strict';
require('dotenv').config();
const mongoose = require('mongoose');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { CfdiMappingRule } = require('./src/shared/models/postgres');
const mappingSvc = require('./src/banks/domains/cfdi-mapping/cfdi-mapping.service');

const RFC = 'CCO011113663';

async function main() {
  await mongoose.connect(process.env.MONGODB_URI);
  await sequelize.authenticate();

  const rules = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']] });

  const cfdis = await CFDI.find({
    $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio: 2026, periodo: 2,
    tipoDeComprobante: 'E', source: 'SAT', satStatus: 'Vigente', isActive: true,
  }).select('uuid tipoDeComprobante metodoPago formaPago emisor.rfc receptor.rfc subTotal total descuento impuestos conceptos.importe conceptos.Importe conceptos.descuento conceptos.Descuento conceptos.impuestos conceptos.descripcion conceptos.Descripcion complementoPago.totales cfdiRelacionados.tipoRelacion').lean();

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

  // Collect Devoluciones with detail
  const devGroups = {};
  for (const cfdi of enriched) {
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (!rule || rule.cuentaCargo !== '4200010001') continue;
    
    const tipoRel = cfdi.cfdiRelacionados?.[0]?.tipoRelacion || 'null';
    const fp = cfdi.formaPago || 'null';
    const sub = Number(cfdi.subTotal) || 0;
    const rawDesc = cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '';
    
    // Group by tipoRelacion + formaPago
    const key = `tipoRel=${tipoRel} | fp=${fp}`;
    if (!devGroups[key]) devGroups[key] = { count: 0, sum: 0, rule: rule.nombre, samples: [] };
    devGroups[key].count++;
    devGroups[key].sum += sub;
    if (devGroups[key].samples.length < 3) devGroups[key].samples.push({ sub: sub.toFixed(2), desc: rawDesc.substring(0, 50) });
  }

  console.log('\n=== DEVOLUCIONES POR tipoRelacion + formaPago ===');
  Object.entries(devGroups).sort((a,b) => b[1].sum-a[1].sum).forEach(([k,v]) => {
    console.log(`  count=${v.count} sum=${v.sum.toFixed(2)} | ${k} | rule="${v.rule}"`);
    v.samples.forEach(s => console.log(`    sample: sub=${s.sub} desc="${s.desc}"`));
  });

  // Specifically: tipoRel=01 in Devoluciones (should be Descuento in CONTPAQi?)
  const tr01 = Object.entries(devGroups).filter(([k]) => k.includes('tipoRel=01'));
  console.log('\ntipoRelacion=01 in Devoluciones:');
  tr01.forEach(([k,v]) => console.log(`  ${k}: count=${v.count} sum=${v.sum.toFixed(2)}`));

  process.exit(0);
}
main().catch(e => { console.error(e.message, e.stack?.split('\n')[1]); process.exit(1); });
