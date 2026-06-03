'use strict';
require('dotenv').config();
const mongoose = require('mongoose');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { CfdiMappingRule, AccountPlan } = require('./src/shared/models/postgres');
const mappingSvc = require('./src/banks/domains/cfdi-mapping/cfdi-mapping.service');

const RFC = 'CCO011113663';

async function main() {
  await mongoose.connect(process.env.MONGODB_URI);
  await sequelize.authenticate();

  const rules = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']] });

  // Check 4200030001 account
  const acc = await AccountPlan.findOne({ where: { codigo: '4200030001' }, raw: true });
  console.log('Cuenta 4200030001:', acc?.nombre, '| tipo:', acc?.tipo);

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

  // Show the 3 NCs going to 4200030001
  console.log('\n=== NCs going to 4200030001 ===');
  for (const cfdi of enriched) {
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (rule?.cuentaCargo === '4200030001') {
      const rawDesc = cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '';
      const tipoRel = cfdi.cfdiRelacionados?.[0]?.tipoRelacion;
      console.log(`  sub=${cfdi.subTotal} fp=${cfdi.formaPago} tipoRel=${tipoRel} rule="${rule.nombre}" desc="${rawDesc.substring(0,60)}"`);
    }
  }

  // Check tipo I CFDIs with descuento field
  const ingresosConDesc = await CFDI.find({
    $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio: 2026, periodo: 2,
    tipoDeComprobante: 'I', source: 'SAT', satStatus: 'Vigente', isActive: true,
    $or: [{ descuento: { $gt: 0 } }, { 'conceptos.descuento': { $gt: 0 } }, { 'conceptos.Descuento': { $gt: 0 } }],
  }).select('uuid subTotal descuento conceptos.descripcion conceptos.Descripcion conceptos.descuento conceptos.Descuento').limit(5).lean();
  console.log(`\nTipo I con descuento: ${ingresosConDesc.length} (sample)`);
  ingresosConDesc.forEach(c => {
    const totalDesc = c.conceptos?.reduce((s,x) => s + (Number(x.descuento)||Number(x.Descuento)||0), 0) || 0;
    console.log(`  sub=${c.subTotal} headerDesc=${c.descuento} concDesc=${totalDesc.toFixed(2)}`);
  });

  // Total descuentos in tipo I
  const totalDescI = await CFDI.aggregate([
    { $match: { $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }], ejercicio: 2026, periodo: 2, tipoDeComprobante: 'I', source: 'SAT', satStatus: 'Vigente', isActive: true }},
    { $group: { _id: null, totalDescuento: { $sum: '$descuento' }, count: { $sum: 1 }, countWithDesc: { $sum: { $cond: [{ $gt: ['$descuento', 0] }, 1, 0] }}}},
  ]);
  console.log('\nTipo I descuento header:', JSON.stringify(totalDescI));

  process.exit(0);
}
main().catch(e => { console.error(e.message, e.stack?.split('\n')[1]); process.exit(1); });
