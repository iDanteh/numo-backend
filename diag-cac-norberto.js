'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const { AccountPlan, CfdiMappingRule } = require('./src/shared/models/postgres');
const CFDI = require('./src/visor/models/CFDI');
const { cfdiToMovimientos, findRuleInList } = require('./src/banks/domains/cfdi-mapping/cfdi-mapping.service');

const UUID = process.env.DIAG_UUID || 'C218AFD7-A84F-46E9-9D62-67434E02928E';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const cfdi = await CFDI.findOne({ uuid: UUID }).lean();
  console.log('\n=== CFDI ===\n');
  console.log(JSON.stringify({
    uuid: cfdi.uuid, tipoDeComprobante: cfdi.tipoDeComprobante, tipoOrigen: cfdi.tipoOrigen,
    metodoPago: cfdi.metodoPago, formaPago: cfdi.formaPago, serie: cfdi.serie, folio: cfdi.folio,
    subtotal: cfdi.subtotal, total: cfdi.total, descuento: cfdi.descuento,
    conceptos: (cfdi.conceptos ?? []).map(c => c.descripcion),
    documentosRelacionados: cfdi.documentosRelacionados,
  }, null, 2));

  const rules = await CfdiMappingRule.findAll({ where: { isActive: true }, raw: true, order: [['prioridad', 'ASC']] });
  const rule = findRuleInList(cfdi, rules);
  console.log('\n=== Regla matcheada ===\n');
  console.log(JSON.stringify(rule, null, 2));

  const allAccounts = await AccountPlan.findAll({ attributes: ['id', 'codigo'], raw: true });
  const cuentaMap = Object.fromEntries(allAccounts.map(a => [a.codigo, a.id]));
  const codigoPorId = Object.fromEntries(allAccounts.map(a => [a.id, a.codigo]));

  const movs = await cfdiToMovimientos(cfdi, rule, cuentaMap, {});
  console.log('\n=== Movimientos generados ===\n');
  console.log('Total:', movs.length);
  for (const m of movs) {
    console.log({
      cuentaCodigo: codigoPorId[m.cuentaId] ?? m.cuentaId,
      concepto: m.concepto, debe: m.debe, haber: m.haber,
      tipoOrigen: m.tipoOrigen, reglaNombre: m.reglaNombre, formaPago: m.formaPago,
      _esCargoPrincipal: m._esCargoPrincipal,
    });
  }

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
