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

  // Check CFDIs excluded by our filters
  const excluded = await CFDI.find({
    $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio: 2026, periodo: 2,
    tipoDeComprobante: 'E',
    $or: [{ source: { $ne: 'SAT' } }, { satStatus: { $ne: 'Vigente' } }, { isActive: { $ne: true } }],
  }).select('uuid source satStatus isActive subTotal conceptos.descripcion conceptos.Descripcion').limit(50).lean();

  console.log(`Excluidos (source!=SAT or satStatus!=Vigente or isActive!=true): ${excluded.length}`);
  const descExcluded = {};
  let totalExcl = 0;
  for (const c of excluded) {
    const desc = (c.conceptos?.[0]?.descripcion || c.conceptos?.[0]?.Descripcion || '').toLowerCase().substring(0, 40);
    const key = `source=${c.source} satStatus=${c.satStatus} isActive=${c.isActive}`;
    if (!descExcluded[key]) descExcluded[key] = { count: 0, sum: 0 };
    descExcluded[key].count++;
    descExcluded[key].sum += Number(c.subTotal) || 0;
    totalExcl += Number(c.subTotal) || 0;
  }
  Object.entries(descExcluded).forEach(([k, v]) => console.log(`  ${k}: count=${v.count} sum=${v.sum.toFixed(2)}`));
  console.log(`Total excluido subTotal: ${totalExcl.toFixed(2)}`);

  // Also check: what if we remove the source='SAT' filter — do we get more E CFDIs?
  const allE = await CFDI.countDocuments({
    $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio: 2026, periodo: 2,
    tipoDeComprobante: 'E',
  });
  const satVigente = await CFDI.countDocuments({
    $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio: 2026, periodo: 2,
    tipoDeComprobante: 'E', source: 'SAT', satStatus: 'Vigente', isActive: true,
  });
  console.log(`\nAll tipo E (no filters): ${allE}`);
  console.log(`With SAT+Vigente+isActive: ${satVigente}`);
  console.log(`Difference: ${allE - satVigente}`);

  process.exit(0);
}
main().catch(e => { console.error(e.message, e.stack?.split('\n')[1]); process.exit(1); });
