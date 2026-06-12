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

  // All tipo E breakdown by source+satStatus
  const dist = await CFDI.aggregate([
    { $match: { $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }], ejercicio: 2026, periodo: 2, tipoDeComprobante: 'E' }},
    { $group: { _id: { source: '$source', satStatus: '$satStatus', isActive: '$isActive' }, count: { $sum: 1 }, total: { $sum: '$subTotal' }}},
    { $sort: { count: -1 }},
  ]);
  console.log('=== Distribución tipo E por source+satStatus+isActive ===');
  dist.forEach(d => console.log(`  source=${d._id.source} satStatus=${d._id.satStatus} isActive=${d._id.isActive}: count=${d.count} total=${d.total?.toFixed(2)}`));

  // Check all accounts used for tipo E with current filters
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

  // Group ALL cuentaCargo amounts
  const cargoMap = {};
  for (const cfdi of enriched) {
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (!rule) continue;
    const k = rule.cuentaCargo || 'null';
    if (!cargoMap[k]) cargoMap[k] = { count: 0, sum: 0 };
    cargoMap[k].count++;
    cargoMap[k].sum += Number(cfdi.subTotal) || 0;
  }
  console.log('\n=== cuentaCargo distribution (tipo E, SAT Vigente) ===');
  Object.entries(cargoMap).sort((a,b) => b[1].sum-a[1].sum).forEach(([k,v]) => {
    console.log(`  ${k}: count=${v.count} sum=${v.sum.toFixed(2)}`);
  });

  process.exit(0);
}
main().catch(e => { console.error(e.message, e.stack?.split('\n')[1]); process.exit(1); });
