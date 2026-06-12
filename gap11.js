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
      formaPago: cfdi.formaPago || erp.formaPago,
      metodoPago: cfdi.metodoPago || erp.metodoPago,
      conceptos: satHasTraslados ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos: satHasTraslados ? cfdi.impuestos : (erp.impuestos ?? cfdi.impuestos),
    };
  });

  // Check: any Devoluciones NC where ALL conceptos descriptions together mention dto/bonificaci/descuento
  const devDtoKw = [];
  for (const cfdi of enriched) {
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (!rule || rule.cuentaCargo !== '4200010001') continue;
    
    const allDescs = (cfdi.conceptos || []).map(c => (c.descripcion || c.Descripcion || '').toLowerCase()).join(' | ');
    const hasKeyword = allDescs.includes('dto') || allDescs.includes('descto') || allDescs.includes('descuento') || allDescs.includes('bonificaci') || allDescs.includes('ajuste');
    if (hasKeyword) {
      devDtoKw.push({ sub: Number(cfdi.subTotal)||0, fp: cfdi.formaPago, tipoRel: cfdi.cfdiRelacionados?.[0]?.tipoRelacion, rule: rule.nombre, allDescs: allDescs.substring(0, 100) });
    }
  }
  console.log(`NCs en Devoluciones con keywords en TODOS conceptos: ${devDtoKw.length}`);
  devDtoKw.forEach(d => console.log(`  sub=${d.sub.toFixed(2)} tipoRel=${d.tipoRel} fp=${d.fp} | "${d.allDescs}"`));
  console.log(`Sum: ${devDtoKw.reduce((s,x)=>s+x.sub,0).toFixed(2)}`);

  // Look at the 31 NCs going to 2103010001
  console.log('\n=== NCs to 2103010001 (Anticipos?) - first 10 ===');
  let n = 0;
  for (const cfdi of enriched) {
    if (n >= 10) break;
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (rule?.cuentaCargo !== '2103010001') continue;
    n++;
    const rawDesc = (cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '').substring(0, 60);
    const tipoRel = cfdi.cfdiRelacionados?.[0]?.tipoRelacion;
    console.log(`  sub=${cfdi.subTotal} fp=${cfdi.formaPago} tipoRel=${tipoRel} rule="${rule.nombre.substring(0,50)}" desc="${rawDesc}"`);
  }

  // Summary of 2103010001
  let sum2103 = 0, cnt2103 = 0;
  for (const cfdi of enriched) {
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (rule?.cuentaCargo !== '2103010001') continue;
    sum2103 += Number(cfdi.subTotal) || 0;
    cnt2103++;
  }
  console.log(`\n2103010001 total: count=${cnt2103} sum=${sum2103.toFixed(2)}`);

  process.exit(0);
}
main().catch(e => { console.error(e.message); process.exit(1); });
