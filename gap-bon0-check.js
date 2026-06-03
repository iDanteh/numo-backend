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

// Detecta la tasa IVA real directamente de los datos MongoDB
// sin usar _detectTasaIva (que no está exportada)
function detectTasaReal(cfdi) {
  const traslados = [];

  // Recopilar traslados del header
  if (cfdi.impuestos?.traslados?.length) {
    for (const t of cfdi.impuestos.traslados) {
      const tasa = String(t.tasaOCuota ?? t.TasaOCuota ?? '');
      traslados.push(parseFloat(tasa));
    }
  }

  // Recopilar traslados por concepto
  if (cfdi.conceptos?.length) {
    for (const con of cfdi.conceptos) {
      if (con.impuestos?.traslados?.length) {
        for (const t of con.impuestos.traslados) {
          const tasa = String(t.tasaOCuota ?? t.TasaOCuota ?? '');
          traslados.push(parseFloat(tasa));
        }
      }
    }
  }

  if (!traslados.length) return 'sin_traslados';

  const has16 = traslados.some(t => t >= 0.15);
  const has0  = traslados.some(t => t < 0.01);

  if (has16 && has0)  return 'mixto';
  if (has16)          return '16';
  if (has0)           return '0';
  return 'otro';
}

async function run() {
  await mongoose.connect(process.env.MONGODB_URI);
  await sequelize.authenticate();

  const rules = await CfdiMappingRule.findAll({
    where: { isActive: true },
    order: [['prioridad', 'ASC']],
  });

  // Reglas que rutean a 4200020001
  const reglasA01 = rules.filter(r => r.cuentaCargo === '4200020001');
  console.log('Reglas que rutean a 4200020001:');
  for (const r of reglasA01) {
    console.log(`  [prio ${r.prioridad}] ${r.nombre}`);
  }
  console.log('');

  // Traer solo CFDIs tipo E con concepto que contiene "bonificac" o "dto"
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

  // Filtrar: los que van a 4200020001 con concepto bonificación/dto
  const mal_ruteados = [];

  for (const cfdi of cfdisEnriquecidos) {
    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (!rule) continue;
    if (rule.cuentaCargo !== '4200020001') continue;

    // Solo los de bonificacion/dto (candidatos a ser 0%)
    const desc = (cfdi.conceptos?.[0]?.descripcion || cfdi.conceptos?.[0]?.Descripcion || '').toLowerCase();
    if (!desc.includes('bonificac') && !desc.includes('dto') && !desc.includes('descuento')) continue;

    const tasaReal = detectTasaReal(cfdi);
    mal_ruteados.push({
      uuid:      cfdi.uuid,
      fp:        cfdi.formaPago,
      mp:        cfdi.metodoPago,
      total:     cfdi.total,
      regla:     rule.nombre,
      tasaReal,
      concepto:  desc.substring(0, 70),
    });
  }

  // Agrupar por tasaReal
  const por_tasa = {};
  for (const item of mal_ruteados) {
    if (!por_tasa[item.tasaReal]) por_tasa[item.tasaReal] = { count: 0, total: 0, items: [] };
    por_tasa[item.tasaReal].count++;
    por_tasa[item.tasaReal].total += Number(item.total) || 0;
    por_tasa[item.tasaReal].items.push(item);
  }

  console.log('── Bonificaciones/DTOs ruteados a 4200020001, por tasa real ──\n');
  for (const [tasa, g] of Object.entries(por_tasa)) {
    console.log(`TASA ${tasa}: count=${g.count}  total=${g.total.toFixed(2)}`);
    for (const i of g.items) {
      console.log(`  ${i.uuid}  fp=${i.fp}  $${i.total}`);
      console.log(`    concepto: ${i.concepto}`);
      console.log(`    regla:    ${i.regla}`);
    }
    console.log('');
  }

  // Resumen de candidatos mal ruteados (los con tasa real = 0 o sin_traslados)
  const candidatos = mal_ruteados.filter(i => i.tasaReal === '0' || i.tasaReal === 'sin_traslados');
  const totalCandidatos = candidatos.reduce((s, i) => s + (Number(i.total) || 0), 0);
  console.log(`\nCandidatos que DEBERÍAN ir a 4200020002 (tasa 0% o sin traslados): ${candidatos.length}`);
  console.log(`Total acumulado: ${totalCandidatos.toFixed(2)}`);
  console.log(`Gap esperado: 534.14`);
  console.log(`Diferencia vs gap: ${(totalCandidatos - 534.14).toFixed(2)}`);

  await mongoose.disconnect();
  await sequelize.close();
  process.exit(0);
}

run().catch(e => { console.error(e); process.exit(1); });
