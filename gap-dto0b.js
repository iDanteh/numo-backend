'use strict';
require('dotenv').config();

const mongoose = require('mongoose');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { CfdiMappingRule, AccountPlan } = require('./src/shared/models/postgres');
const mappingSvc = require('./src/banks/domains/cfdi-mapping/cfdi-mapping.service');
const { Op } = require('sequelize');

const RFC       = 'CCO011113663';
const EJERCICIO = 2026;
const PERIODO   = 2;

async function run() {
  await mongoose.connect(process.env.MONGODB_URI);
  await sequelize.authenticate();

  const rules = await CfdiMappingRule.findAll({
    where: { isActive: true },
    order: [['prioridad', 'ASC']],
  });

  // Precargar cuentas
  const codigosTodos = [...new Set(
    rules.flatMap(r => [
      r.cuentaCargo, r.cuentaAbono, r.cuentaAbono2,
      r.cuentaIva, r.cuentaIvaPPD, r.cuentaIvaRetenido,
      r.cuentaIsrRetenido, r.cuentaIvaAnticipo, r.cuentaDeltaAnticipo,
      r.cuentaCargo2, r.cuentaDescuento, r.cuentaDescuento0,
    ].filter(Boolean)),
  )];
  const cuentasRows = await AccountPlan.findAll({
    where: { codigo: { [Op.in]: codigosTodos } },
    attributes: ['id', 'codigo', 'nombre'],
    raw: true,
  });
  const cuentaMapByCod = Object.fromEntries(cuentasRows.map(c => [c.codigo, c.id]));
  const cuentaMapById  = Object.fromEntries(cuentasRows.map(c => [c.id, c]));

  // Todos los tipo E
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

  // Clasificar cada CFDI
  const resumen = {}; // cuentaCargo → { total, count, cfdis[] }

  for (const cfdi of cfdisEnriquecidos) {
    const tasaIva = mappingSvc._detectTasaIva ? mappingSvc._detectTasaIva(cfdi) : '?';
    if (tasaIva !== '0' && tasaIva !== '?') continue; // solo tasa 0%

    const rule = mappingSvc.findRuleInList(cfdi, rules);
    const cuentaKey = rule ? rule.cuentaCargo : 'SIN_REGLA';
    const cuentaNom = rule ? (cuentasRows.find(c => c.codigo === rule.cuentaCargo)?.nombre || rule.cuentaCargo) : 'Sin regla';

    if (!resumen[cuentaKey]) resumen[cuentaKey] = { nombre: cuentaNom, total: 0, count: 0, items: [] };
    resumen[cuentaKey].total += Number(cfdi.total) || 0;
    resumen[cuentaKey].count++;
    resumen[cuentaKey].items.push({
      uuid:     cfdi.uuid,
      fp:       cfdi.formaPago,
      total:    cfdi.total,
      regla:    rule?.nombre || '—',
      concepto: (cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '').substring(0, 60),
      tasaIva,
    });
  }

  console.log('\n── CFDIs tipo E tasa 0% — distribución por cuentaCargo ──\n');
  for (const [cod, g] of Object.entries(resumen).sort((a,b) => b[1].total - a[1].total)) {
    console.log(`${cod}  ${g.nombre}`);
    console.log(`  count=${g.count}  total=${g.total.toFixed(2)}`);
    for (const i of g.items) {
      console.log(`    ${i.uuid}  fp=${i.fp}  $${i.total}  tasa=${i.tasaIva}`);
      console.log(`      concepto: ${i.concepto}`);
      console.log(`      regla:    ${i.regla}`);
    }
  }

  await mongoose.disconnect();
  await sequelize.close();
  process.exit(0);
}

run().catch(e => { console.error(e); process.exit(1); });
