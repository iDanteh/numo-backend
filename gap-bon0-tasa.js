'use strict';
require('dotenv').config();

const mongoose = require('mongoose');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { CfdiMappingRule } = require('./src/shared/models/postgres');
const mappingSvc = require('./src/banks/domains/cfdi-mapping/cfdi-mapping.service');

const RFC       = 'CCO011113663';
const EJERCICIO = 2026;
const PERIODO   = 2;

// _detectTasaIva está exportado como _detectTasaIvaPublic
const detectTasaIva = mappingSvc._detectTasaIvaPublic;

async function run() {
  await mongoose.connect(process.env.MONGODB_URI);
  await sequelize.authenticate();

  const rules = await CfdiMappingRule.findAll({
    where: { isActive: true },
    order: [['prioridad', 'ASC']],
  });

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
    .filter(c => c.uuid && (
      !c.formaPago ||
      !c.metodoPago ||
      !c.conceptos?.length ||
      c.conceptos.every(con => !(con.impuestos?.traslados?.length))
    ))
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

  // Clasificar cada CFDI con la función real
  const grupos = { '0': [], '16': [], mixto: [], null: [] };

  for (const cfdi of cfdisEnriquecidos) {
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (!rule) continue;
    if (rule.cuentaCargo !== '4200020001') continue;

    const tasa = detectTasaIva(cfdi);
    const key  = tasa ?? 'null';
    const desc = (cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '').toLowerCase();

    // Solo bonificaciones/DTOs
    if (!desc.includes('bonificac') && !desc.includes('dto') && !desc.includes('descuento')) continue;

    const totalImptos = cfdi.impuestos?.totalImpuestosTrasladados;

    grupos[key].push({
      uuid:        cfdi.uuid,
      fp:          cfdi.formaPago,
      total:       cfdi.total,
      subTotal:    cfdi.subTotal,
      totalImptos,
      regla:       rule.nombre,
      concepto:    desc.substring(0, 60),
    });
  }

  // Resumen por tasa
  console.log('\n── Bonificaciones/DTOs en 4200020001 — tasa detectada por _detectTasaIva ──\n');
  for (const [tasa, items] of Object.entries(grupos)) {
    const total = items.reduce((s, i) => s + (Number(i.total) || 0), 0);
    console.log(`TASA "${tasa}": count=${items.length}  total=${total.toFixed(2)}`);
    if (items.length <= 20) {
      for (const i of items) {
        console.log(`  ${i.uuid}  fp=${i.fp}  $${i.total}  totalImptos=${i.totalImptos ?? 'null'}`);
        console.log(`    concepto: ${i.concepto}`);
        console.log(`    regla:    ${i.regla}`);
      }
    } else {
      console.log(`  (primeros 5)`);
      for (const i of items.slice(0, 5)) {
        console.log(`  ${i.uuid}  fp=${i.fp}  $${i.total}  totalImptos=${i.totalImptos ?? 'null'}`);
        console.log(`    concepto: ${i.concepto}`);
      }
    }
    console.log('');
  }

  // Resumen: los de tasa "null" son candidatos → no matchean CC-BON-0
  const candidatos_null = grupos['null'];
  const candidatos_0    = grupos['0'];

  console.log(`\nCandidatos con tasa=null (totalImpuestosTrasladados ausente): ${candidatos_null.length}  total=${candidatos_null.reduce((s,i)=>s+(Number(i.total)||0),0).toFixed(2)}`);
  console.log(`Candidatos con tasa=0   (deberían ir a CC-BON-0):            ${candidatos_0.length}  total=${candidatos_0.reduce((s,i)=>s+(Number(i.total)||0),0).toFixed(2)}`);
  console.log(`\nGap objetivo: 534.14`);

  await mongoose.disconnect();
  await sequelize.close();
  process.exit(0);
}

run().catch(e => { console.error(e); process.exit(1); });
