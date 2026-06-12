'use strict';
/**
 * diag-club-tuberos.js
 * Diagnóstico de la cuenta 2103090002 (Anticipos Otros - Club Tuberos).
 *
 * Muestra qué CFDIs generan DEBE y HABER en esa cuenta según las reglas activas,
 * para identificar por qué NUMO difiere del CONTPAQ de referencia.
 *
 * Uso:
 *   node src/banks/scripts/diag-club-tuberos.js --ejercicio 2026 --periodo 2
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { sequelize }     = require('../../config/database.postgres');
const CFDI              = require('../../visor/models/CFDI');
const mappingSvc        = require('../domains/cfdi-mapping/cfdi-mapping.service');
const { CfdiMappingRule } = require('../../shared/models/postgres');

const RFC       = 'CCO011113663';
const CUENTA    = '2103090002';

const args      = process.argv.slice(2);
const get       = (f) => { const i = args.indexOf(f); return i >= 0 ? args[i + 1] : null; };
const EJERCICIO = Number(get('--ejercicio') || 2026);
const PERIODO   = Number(get('--periodo')   || 2);

async function run() {
  await connectMongo();
  await sequelize.authenticate();

  const rulesRaw = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']], raw: true });
  const rules    = rulesRaw.map(r => r.dataValues ?? r);

  // Cuentas → ids
  const { AccountPlan } = require('../../shared/models/postgres');
  const { Op }          = require('sequelize');
  const codigos = [...new Set(rules.flatMap(r => [
    r.cuentaCargo, r.cuentaAbono, r.cuentaAbono2, r.cuentaIva, r.cuentaIvaPPD,
    r.cuentaIvaRetenido, r.cuentaIsrRetenido, r.cuentaIvaAnticipo, r.cuentaDeltaAnticipo,
    r.cuentaCargo2, r.cuentaDescuento, r.cuentaDescuento0, r.cuentaCargoMixto0, r.cuentaIvaAbono,
  ].filter(Boolean)))];
  const cuentasRows = await AccountPlan.findAll({ where: { codigo: { [Op.in]: codigos } }, attributes: ['id', 'codigo'], raw: true });
  const cuentaMapByCod = Object.fromEntries(cuentasRows.map(c => [c.codigo, c.id]));
  const targetId = cuentaMapByCod[CUENTA];

  console.log(`RFC=${RFC}  Ejercicio=${EJERCICIO}  Período=${PERIODO}  Cuenta=${CUENTA}  id=${targetId}\n`);

  let totalDebe = 0, totalHaber = 0;
  let sinRegla = 0;

  const resumen = { debe: [], haber: [] };

  for (const tipo of ['I', 'E', 'P']) {
    const cfdis = await CFDI.find({
      $or:               [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
      ejercicio:         EJERCICIO,
      periodo:           PERIODO,
      tipoDeComprobante: tipo,
      source:            'SAT',
      satStatus:         'Vigente',
      isActive:          true,
    }).select('uuid folio serie tipoDeComprobante metodoPago formaPago subTotal total impuestos conceptos cfdiRelacionados tasaIvaInferida tipoOrigen').lean();

    for (const cfdi of cfdis) {
      const rule = mappingSvc.findRuleInList(cfdi, rules);
      if (!rule) { sinRegla++; continue; }

      const movs = await mappingSvc.cfdiToMovimientos(cfdi, rule, cuentaMapByCod);
      const movsEnCuenta = movs.filter(m => m.cuentaId === targetId);
      if (!movsEnCuenta.length) continue;

      const folio = `${cfdi.serie || ''}${cfdi.folio || ''}`;
      const desc  = `${tipo} fp=${cfdi.formaPago || '-'} mp=${cfdi.metodoPago || '-'} regla="${rule.nombre}"`;
      for (const mov of movsEnCuenta) {
        const debe  = Number(mov.debe  || 0);
        const haber = Number(mov.haber || 0);
        if (debe > 0) {
          totalDebe += debe;
          resumen.debe.push({ folio, uuid: cfdi.uuid, monto: debe, desc });
        }
        if (haber > 0) {
          totalHaber += haber;
          resumen.haber.push({ folio, uuid: cfdi.uuid, monto: haber, desc });
        }
      }
    }
  }

  console.log('══ DEBE (cargos a 2103090002) ══════════════════════════════════════════');
  resumen.debe.sort((a, b) => b.monto - a.monto);
  for (const r of resumen.debe) {
    console.log(`  ${r.folio.padEnd(14)} ${String(r.monto.toFixed(2)).padStart(12)}  ${r.desc}`);
  }
  console.log(`  ${'TOTAL'.padEnd(14)} ${String(totalDebe.toFixed(2)).padStart(12)}\n`);

  console.log('══ HABER (abonos a 2103090002) ════════════════════════════════════════');
  resumen.haber.sort((a, b) => b.monto - a.monto);
  for (const r of resumen.haber) {
    console.log(`  ${r.folio.padEnd(14)} ${String(r.monto.toFixed(2)).padStart(12)}  ${r.desc}`);
  }
  console.log(`  ${'TOTAL'.padEnd(14)} ${String(totalHaber.toFixed(2)).padStart(12)}\n`);

  console.log('══ RESUMEN ════════════════════════════════════════════════════════════');
  console.log(`  DEBE:          ${totalDebe.toFixed(2)}`);
  console.log(`  HABER:         ${totalHaber.toFixed(2)}`);
  console.log(`  Sin regla:     ${sinRegla}`);
  console.log(`\n  Referencia CONTPAQ DEBE:  200,090.16`);
  console.log(`  Referencia CONTPAQ HABER: 163,616.95`);
  console.log(`  Diferencia DEBE:  ${(totalDebe - 200090.16).toFixed(2)}`);
  console.log(`  Diferencia HABER: ${(totalHaber - 163616.95).toFixed(2)}`);

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

run().catch(err => { console.error(err.message, err.stack); process.exit(1); });
