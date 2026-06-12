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

  // Find NCs in Devoluciones with tipoRel=01 (all should be Descuentos in CONTPAQi?)
  const devTR01 = [];
  for (const cfdi of enriched) {
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (!rule || rule.cuentaCargo !== '4200010001') continue;
    const tipoRel = cfdi.cfdiRelacionados?.[0]?.tipoRelacion;
    if (tipoRel === '01') {
      const rawDesc = cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '';
      devTR01.push({
        uuid: cfdi.uuid, sub: Number(cfdi.subTotal)||0,
        desc: rawDesc.substring(0,60), fp: cfdi.formaPago || 'null',
        rule: rule.nombre,
      });
    }
  }
  devTR01.sort((a,b) => b.sub - a.sub);
  
  const sumTR01 = devTR01.reduce((s,x) => s+x.sub, 0);
  console.log(`\nNCs tipoRel=01 en Devoluciones: ${devTR01.length}, sum=${sumTR01.toFixed(2)}`);
  devTR01.slice(0, 20).forEach(d => console.log(`  ${d.sub.toFixed(2)} fp=${d.fp} rule="${d.rule.substring(0,40)}" desc="${d.desc}"`));

  // See all unique formaPago values in the entire tipo E dataset
  console.log('\n=== ALL formaPago values in tipo E ===');
  const fpAll = {};
  for (const c of enriched) {
    const fp = c.formaPago || 'null';
    fpAll[fp] = (fpAll[fp] || 0) + 1;
  }
  Object.entries(fpAll).sort((a,b) => b[1]-a[1]).forEach(([fp,cnt]) => console.log(`  fp=${fp}: ${cnt}`));

  // Look for fp=15 anywhere
  const fp15 = enriched.filter(c => c.formaPago === '15');
  console.log(`\nNCs con formaPago=15: ${fp15.length}`);
  fp15.slice(0,5).forEach(c => {
    const rule = mappingSvc.findRuleInList(c, rules);
    const rawDesc = c.conceptos?.[0]?.descripcion || c.conceptos?.[0]?.Descripcion || '';
    console.log(`  sub=${c.subTotal} tipoRel=${c.cfdiRelacionados?.[0]?.tipoRelacion} rule="${rule?.nombre}" desc="${rawDesc.substring(0,50)}"`);
  });

  process.exit(0);
}
main().catch(e => { console.error(e.message, e.stack?.split('\n')[1]); process.exit(1); });
