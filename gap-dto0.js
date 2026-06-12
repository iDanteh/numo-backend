'use strict';
require('dotenv').config();

const mongoose = require('mongoose');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { CfdiMappingRule } = require('./src/shared/models/postgres');
const mappingSvc = require('./src/banks/domains/cfdi-mapping/cfdi-mapping.service');

const RFC      = 'CCO011113663';
const EJERCICIO = 2026;
const PERIODO   = 2;
const TARGET_CTA = '4200020002';

async function run() {
  await mongoose.connect(process.env.MONGODB_URI);
  await sequelize.authenticate();

  const rules = await CfdiMappingRule.findAll({
    where: { isActive: true },
    order: [['prioridad', 'ASC']],
  });

  // CFDIs tipo E
  const cfdis = await CFDI.find({
    $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio: EJERCICIO,
    periodo:   PERIODO,
    tipoDeComprobante: 'E',
    source:    'SAT',
    satStatus: 'Vigente',
    isActive:  true,
  })
  .select('uuid tipoDeComprobante metodoPago formaPago emisor.rfc receptor.rfc subTotal total descuento impuestos conceptos.importe conceptos.Importe conceptos.descuento conceptos.Descuento conceptos.impuestos conceptos.descripcion conceptos.Descripcion complementoPago.totales cfdiRelacionados.tipoRelacion')
  .lean();

  // Enriquecer con ERP
  const uuidsSinConceptos = cfdis
    .filter(c => c.uuid && (!c.conceptos?.length || c.conceptos.every(con => !(con.impuestos?.traslados?.length))))
    .map(c => c.uuid);

  let erpMetaMap = {};
  if (uuidsSinConceptos.length) {
    const erpCfdis = await CFDI.find({ uuid: { $in: uuidsSinConceptos }, source: 'ERP' })
      .select('uuid formaPago metodoPago conceptos impuestos').lean();
    erpMetaMap = Object.fromEntries(erpCfdis.map(c => [c.uuid, c]));
  }

  const cfdisEnriquecidos = cfdis.map(cfdi => {
    const erp = erpMetaMap[cfdi.uuid];
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

  // Encontrar los que van a 4200020002
  const enCuenta = [];
  let totalMonto = 0;

  for (const cfdi of cfdisEnriquecidos) {
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (!rule) continue;

    // Verificar si cuentaCargo o cuentaAbono es 4200020002
    const mapeaACuenta = [
      rule.cuentaCargo, rule.cuentaAbono, rule.cuentaAbono2,
      rule.cuentaCargo2, rule.cuentaDescuento, rule.cuentaDescuento0,
    ].includes(TARGET_CTA);

    if (mapeaACuenta) {
      // Calcular el monto que aporta a esa cuenta
      const movs = await mappingSvc.cfdiToMovimientos(cfdi, rule, {});
      const aportacion = movs
        .filter(m => {
          // Necesitamos resolver cuentaId → codigo, pero aquí usamos la regla directamente
          return true;
        })
        .reduce((s, m) => s + (Number(m.debe) || 0) + (Number(m.haber) || 0), 0);

      const conceptoDesc = cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '';
      enCuenta.push({
        uuid:     cfdi.uuid,
        fp:       cfdi.formaPago,
        mp:       cfdi.metodoPago,
        total:    cfdi.total,
        regla:    rule.nombre,
        concepto: conceptoDesc.substring(0, 60),
      });
      totalMonto += Number(cfdi.total) || 0;
    }
  }

  console.log(`\nCFDIs mapeados a ${TARGET_CTA}: ${enCuenta.length}`);
  console.log(`Total (suma de cfdi.total): ${totalMonto.toFixed(2)}`);
  console.log('\nDetalle:');
  for (const c of enCuenta) {
    console.log(`  ${c.uuid}  fp=${c.fp}  total=${c.total}  regla=${c.regla}`);
    console.log(`    concepto: ${c.concepto}`);
  }

  // También: qué reglas tienen 4200020002
  console.log('\n── Reglas activas que usan 4200020002 ──');
  for (const r of rules) {
    const usa = [r.cuentaCargo, r.cuentaAbono, r.cuentaAbono2, r.cuentaCargo2, r.cuentaDescuento, r.cuentaDescuento0].includes(TARGET_CTA);
    if (usa) console.log(`  [prio ${r.prioridad}] ${r.nombre}`);
  }

  await mongoose.disconnect();
  await sequelize.close();
  process.exit(0);
}

run().catch(e => { console.error(e); process.exit(1); });
