'use strict';

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { sequelize } = require('../../config/database.postgres');
const CFDI        = require('../../visor/models/CFDI');
const { CfdiMappingRule } = require('../../shared/models/postgres');
const mappingSvc  = require('../domains/cfdi-mapping/cfdi-mapping.service');

// ── Ajusta estos valores ──────────────────────────────────────────────────────
const RFC       = 'CCO011113663';
const EJERCICIO = 2026;
const PERIODO   = 2;
// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const rules = await CfdiMappingRule.findAll({
    where: { isActive: true },
    order: [['prioridad', 'ASC']],
  });

  const cfdis = await CFDI.find({
    $or: [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio:         EJERCICIO,
    periodo:           PERIODO,
    tipoDeComprobante: 'I',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
  })
    .select('uuid tipoDeComprobante metodoPago formaPago emisor.rfc subTotal total descuento impuestos conceptos.importe conceptos.Importe conceptos.descuento conceptos.Descuento conceptos.impuestos')
    .lean();

  console.log(`Total CFDIs tipo I encontrados: ${cfdis.length}`);

  // Enriquecer con datos ERP (mismo patrón que generator y balanza)
  const uuidsSinConceptos = cfdis
    .filter(c => c.uuid && (
      !c.formaPago ||
      !c.metodoPago ||
      !c.conceptos?.length ||
      c.conceptos.every(con => !(con.impuestos?.traslados?.length))
    ))
    .map(c => c.uuid);
  console.log(`CFDIs sin traslados en conceptos (candidatos a enriquecer): ${uuidsSinConceptos.length}`);
  console.log('Sample UUID SAT:', uuidsSinConceptos[0]);

  let erpMetaMap = {};
  if (uuidsSinConceptos.length) {
    const erpCfdis = await CFDI.find({
      uuid: { $in: uuidsSinConceptos }, source: 'ERP',
    }).select('uuid formaPago metodoPago conceptos impuestos').lean();
    console.log(`ERP CFDIs encontrados con esos UUIDs: ${erpCfdis.length}`);
    if (erpCfdis.length === 0) {
      // Verificar si el UUID existe en ERP con cualquier fuente
      const cualquierFuente = await CFDI.findOne({ uuid: uuidsSinConceptos[0] }).select('uuid source').lean();
      console.log('Ese UUID en MongoDB:', cualquierFuente);
    }
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
      conceptos:  satHasTraslados ? cfdi.conceptos : (erp.conceptos ?? cfdi.conceptos ?? []),
      impuestos:  satHasTraslados ? cfdi.impuestos : (erp.impuestos  ?? cfdi.impuestos),
    };
  });

  const tally    = {};
  const tasaTally = {};
  let   sinRegla = 0;

  for (const cfdi of cfdisEnriquecidos) {
    // Contar distribución de tasas detectadas
    const tasa = mappingSvc._detectTasaIvaPublic ? mappingSvc._detectTasaIvaPublic(cfdi) : null;
    const tasaKey = String(tasa);
    if (!tasaTally[tasaKey]) tasaTally[tasaKey] = { count: 0, totalMxn: 0 };
    tasaTally[tasaKey].count++;
    tasaTally[tasaKey].totalMxn += Number(cfdi.total || 0);

    const rule = mappingSvc.findRuleInList(cfdi, rules);
    if (!rule) {
      sinRegla++;
      continue;
    }
    const key = `${rule.cuentaAbono} | prio:${rule.prioridad} | ${rule.nombre}`;
    if (!tally[key]) tally[key] = { count: 0, totalMxn: 0 };
    tally[key].count    += 1;
    tally[key].totalMxn += Number(cfdi.total || 0);
  }

  // Mostrar 3 CFDIs detectados como '16' para ver su estructura
  const muestras16 = cfdisEnriquecidos.filter(c => mappingSvc._detectTasaIvaPublic(c) === '16').slice(0, 3);
  console.log('\n--- MUESTRA de 3 CFDIs detectados como tasaIva=16 ---');
  for (const c of muestras16) {
    console.log({
      metodoPago:   c.metodoPago,
      formaPago:    c.formaPago,
      subTotal:     c.subTotal,
      total:        c.total,
      descuento:    c.descuento,
      totalImpTras: c.impuestos?.totalImpuestosTrasladados,
      totalImpRet:  c.impuestos?.totalImpuestosRetenidos,
      numConceptos: (c.conceptos || []).length,
      numTraslados: (c.conceptos?.[0]?.impuestos?.traslados || []).length,
      headerTraslados: (c.impuestos?.traslados || []).length,
    });
  }

  // Mostrar 3 CFDIs detectados como '0'
  const muestras0 = cfdisEnriquecidos.filter(c => mappingSvc._detectTasaIvaPublic(c) === '0').slice(0, 3);
  console.log('\n--- MUESTRA de 3 CFDIs detectados como tasaIva=0 ---');
  for (const c of muestras0) {
    console.log({
      metodoPago:   c.metodoPago,
      formaPago:    c.formaPago,
      subTotal:     c.subTotal,
      total:        c.total,
      totalImpTras: c.impuestos?.totalImpuestosTrasladados,
      numConceptos: (c.conceptos || []).length,
    });
  }

  console.log('\nDistribución de tasaIva detectada:');
  Object.entries(tasaTally)
    .sort((a, b) => b[1].count - a[1].count)
    .forEach(([t, v]) =>
      console.log(`  tasaIva=${t}  →  CFDIs: ${v.count}  |  MXN: ${v.totalMxn.toFixed(2)}`),
    );

  console.log(`\nSin regla: ${sinRegla}\n`);
  console.log('Desglose por regla ganadora (ordenado por monto):');
  Object.entries(tally)
    .sort((a, b) => b[1].totalMxn - a[1].totalMxn)
    .forEach(([k, v]) =>
      console.log(`  ${k}  →  CFDIs: ${v.count}  |  MXN: ${v.totalMxn.toFixed(2)}`),
    );

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
