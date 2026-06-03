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

  const rules = await CfdiMappingRule.findAll({
    where: { isActive: true },
    order: [['prioridad', 'ASC']],
  });

  const cfdis = await CFDI.find({
    $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio: EJERCICIO,
    periodo:   PERIODO,
    tipoDeComprobante: 'E',
    source: 'SAT',
    satStatus: 'Vigente',
    isActive: true,
  }).select('uuid tipoDeComprobante metodoPago formaPago emisor.rfc receptor.rfc subTotal total descuento impuestos conceptos.importe conceptos.Importe conceptos.descuento conceptos.Descuento conceptos.impuestos conceptos.descripcion conceptos.Descripcion complementoPago.totales cfdiRelacionados.tipoRelacion').lean();

  // Enrich
  const uuidsSin = cfdis
    .filter(c => c.uuid && (!c.formaPago || !c.metodoPago || !c.conceptos?.length || c.conceptos.every(con => !(con.impuestos?.traslados?.length))))
    .map(c => c.uuid);
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
  for (const cfdi of enriched) {
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (!rule) continue;
    if (rule.cuentaCargo === '4200010001') {
      const rawDesc = cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '';
      const desc = rawDesc.toLowerCase();
      const tipoRel = cfdi.cfdiRelacionados?.[0]?.tipoRelacion || null;
      devoluciones.push({
        uuid: cfdi.uuid,
        subTotal: Number(cfdi.subTotal) || 0,
        desc: rawDesc.substring(0, 70),
        descLow: desc,
        tipoRel,
        formaPago: cfdi.formaPago || null,
        rule: rule.nombre,
        rulePrio: rule.prioridad,
      });
    }
  }

  const rulesUsed = {};
  for (const d of devoluciones) {
    rulesUsed[d.rule] = (rulesUsed[d.rule] || 0) + d.subTotal;
  }
  console.log('\n=== REGLAS EN DEVOLUCIONES ===');
  Object.entries(rulesUsed).sort((a,b) => b[1]-a[1]).forEach(([r, s]) => console.log(`  ${r}: ${s.toFixed(2)}`));

  // Find NCs NOT matching 'devoluci' / 'cancelaci'
  const others = devoluciones.filter(d => !d.descLow.includes('devoluci') && !d.descLow.includes('cancelaci'));
  console.log(`\n=== NCs SIN 'devolucion'/'cancelacion' en Devoluciones (${others.length}) ===`);
  others.forEach(d => {
    console.log(`  sub=${d.subTotal.toFixed(2)} tipoRel=${d.tipoRel} fp=${d.formaPago} rule="${d.rule}" desc="${d.desc}"`);
  });
  console.log(`  Suma: ${others.reduce((s,x) => s+x.subTotal, 0).toFixed(2)}`);

  // Group by description prefix (first 40 chars)
  console.log('\n=== TOP DESCRIPCIONES EN DEVOLUCIONES ===');
  const descGroups = {};
  for (const d of devoluciones) {
    const key = d.desc.substring(0, 40);
    if (!descGroups[key]) descGroups[key] = { count: 0, sum: 0 };
    descGroups[key].count++;
    descGroups[key].sum += d.subTotal;
  }
  Object.entries(descGroups).sort((a,b) => b[1].sum - a[1].sum).slice(0, 25).forEach(([k, v]) => {
    console.log(`  count=${v.count} sum=${v.sum.toFixed(2)} | "${k}"`);
  });

  process.exit(0);
}

main().catch(e => { console.error(e.message, e.stack); process.exit(1); });
