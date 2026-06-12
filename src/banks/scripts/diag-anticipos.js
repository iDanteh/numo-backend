'use strict';

/**
 * diag-anticipos.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Diagnóstico de CFDIs tipo I que probablemente sean anticipos de clientes,
 * para identificar por qué no están abonando las cuentas 2103010001 y
 * 2103090001 en la balanza.
 *
 * Muestra:
 *   1. Distribución de claveProdServ del primer concepto (top 20) — tipo I
 *   2. CFDIs cuyo primer concepto contiene 'anticipo' (case insensitive):
 *      cantidad, claveProdServ que usan y regla que gana actualmente
 *   3. Muestra detallada de los primeros 15 CFDIs con 'anticipo' en concepto
 *
 * Uso:
 *   node src/banks/scripts/diag-anticipos.js
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { sequelize }     = require('../../config/database.postgres');
const CFDI              = require('../../visor/models/CFDI');
const { CfdiMappingRule } = require('../../shared/models/postgres');
const mappingSvc        = require('../domains/cfdi-mapping/cfdi-mapping.service');

// ── Ajusta estos valores ──────────────────────────────────────────────────────
const RFC       = 'CCO011113663';
const EJERCICIO = 2026;
const PERIODO   = 2;
// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const rules = await CfdiMappingRule.findAll({
    where:  { isActive: true },
    order:  [['prioridad', 'ASC']],
    raw:    true,
  });

  // ── 1. Cargar CFDIs tipo I SAT ─────────────────────────────────────────────
  const cfdis = await CFDI.find({
    $or:               [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio:         EJERCICIO,
    periodo:           PERIODO,
    tipoDeComprobante: 'I',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
  })
    .select('uuid tipoDeComprobante metodoPago formaPago emisor.rfc receptor.rfc subTotal total impuestos conceptos cfdiRelacionados tipoOrigen')
    .lean();

  console.log(`\nTotal CFDIs tipo I SAT vigentes encontrados: ${cfdis.length}`);

  // ── 2. Enriquecer con ERP (mismo patrón que diag-notas-credito) ────────────
  const uuidsAll = cfdis.map(c => c.uuid).filter(Boolean);
  const erpCfdis = await CFDI.find({ uuid: { $in: uuidsAll }, source: 'ERP' })
    .select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados')
    .lean();
  const erpMap = Object.fromEntries(erpCfdis.map(c => [c.uuid, c]));
  console.log(`ERP CFDIs encontrados: ${erpCfdis.length} de ${cfdis.length}`);

  const enriquecidos = cfdis.map(cfdi => {
    const erp = erpMap[cfdi.uuid];
    if (!erp) return cfdi;
    const satHasTraslados = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    return {
      ...cfdi,
      formaPago:  cfdi.formaPago  || erp.formaPago,
      metodoPago: cfdi.metodoPago || erp.metodoPago,
      conceptos:  satHasTraslados ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos:  satHasTraslados ? cfdi.impuestos : (erp.impuestos ?? cfdi.impuestos),
      tipoOrigen: cfdi.tipoOrigen ?? erp.tipoOrigen ?? null,
    };
  });

  // ── 3. Distribución de claveProdServ (top 20) ─────────────────────────────
  const claveTally = {};
  for (const c of enriquecidos) {
    const clave = c.conceptos?.[0]?.claveProdServ ?? '(sin clave)';
    claveTally[clave] = (claveTally[clave] ?? 0) + 1;
  }

  console.log('\n── Distribución claveProdServ primer concepto (top 20) ──────────────');
  Object.entries(claveTally)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .forEach(([k, v]) => console.log(`  ${String(v).padStart(5)}  ${k}`));

  // ── 4. CFDIs con 'anticipo' en descripción del primer concepto ────────────
  const conAnticipo = enriquecidos.filter(c => {
    const desc = (c.conceptos?.[0]?.descripcion ?? c.conceptos?.[0]?.Descripcion ?? '').toLowerCase();
    return desc.includes('anticipo');
  });

  console.log(`\n── CFDIs tipo I con 'anticipo' en concepto: ${conAnticipo.length} ───────────────────`);

  if (conAnticipo.length === 0) {
    console.log('  (ninguno encontrado — la descripción puede usar otra palabra)');
    console.log('\n  Sugerencia: revisa la distribución de claveProdServ arriba y busca');
    console.log('  el código que usan los anticipos en este RFC para crear la regla correcta.');
  } else {
    // Distribución de claveProdServ de los que tienen 'anticipo'
    const claveAnticipo = {};
    const reglaAnticipo = {};
    let   sinRegla      = 0;

    for (const c of conAnticipo) {
      const clave = c.conceptos?.[0]?.claveProdServ ?? '(sin clave)';
      claveAnticipo[clave] = (claveAnticipo[clave] ?? 0) + 1;

      const rule = mappingSvc.findRuleInList(c, rules);
      if (!rule) {
        sinRegla++;
      } else {
        const rKey = `[prio ${rule.prioridad}] ${rule.nombre} → abono:${rule.cuentaAbono}`;
        reglaAnticipo[rKey] = (reglaAnticipo[rKey] ?? 0) + 1;
      }
    }

    console.log('\n  claveProdServ de CFDIs "anticipo":');
    Object.entries(claveAnticipo).sort((a, b) => b[1] - a[1])
      .forEach(([k, v]) => console.log(`    ${String(v).padStart(5)}  ${k}`));

    console.log('\n  Regla ganadora actual para CFDIs "anticipo":');
    Object.entries(reglaAnticipo).sort((a, b) => b[1] - a[1])
      .forEach(([k, v]) => console.log(`    ${String(v).padStart(5)}  ${k}`));
    if (sinRegla > 0) {
      console.log(`    ${String(sinRegla).padStart(5)}  SIN REGLA`);
    }

    // ── 5. Muestra detallada de primeros 15 CFDIs "anticipo" ────────────────
    console.log('\n── MUESTRA: primeros 15 CFDIs con anticipo en concepto ──────────────');
    const muestra = conAnticipo.slice(0, 15);
    for (const c of muestra) {
      const desc      = (c.conceptos?.[0]?.descripcion ?? c.conceptos?.[0]?.Descripcion ?? '(sin descripción)').substring(0, 60);
      const clave     = c.conceptos?.[0]?.claveProdServ ?? '(sin clave)';
      const rule      = mappingSvc.findRuleInList(c, rules);
      const abona84   = rules.some(r => r.claveProdServ === '84111506' && r.tipoComprobante === 'I');
      console.log({
        uuid:          c.uuid?.substring(0, 8),
        concepto:      desc,
        claveProdServ: clave,
        metodoPago:    c.metodoPago,
        formaPago:     c.formaPago,
        total:         c.total,
        regla:         rule ? `[${rule.prioridad}] ${rule.nombre}` : 'SIN REGLA',
        cuentaAbono:   rule?.cuentaAbono ?? '—',
        problema:      clave !== '84111506' && abona84
          ? 'CLAVE DIFERENTE A 84111506 → no matchea Reg 22/22A'
          : null,
      });
    }
  }

  // ── 6. Resumen: cuántos tipo I hay por metodoPago ─────────────────────────
  const metodoPagoTally = {};
  for (const c of enriquecidos) {
    const mp = c.metodoPago ?? '(null)';
    metodoPagoTally[mp] = (metodoPagoTally[mp] ?? 0) + 1;
  }
  console.log('\n── Distribución metodoPago (tipo I) ─────────────────────────────────');
  Object.entries(metodoPagoTally).sort((a, b) => b[1] - a[1])
    .forEach(([k, v]) => console.log(`  ${String(v).padStart(5)}  ${k}`));

  // ── 7. Resumen de reglas que DEBERÍAN abonar 2103010001 / 2103090001 ───────
  console.log('\n── Reglas activas que abonan cuentas de anticipo ────────────────────');
  const reglasAnticipo = rules.filter(r =>
    r.cuentaAbono === '2103010001' || r.cuentaAbono === '2103090001',
  );
  if (reglasAnticipo.length === 0) {
    console.log('  (ninguna regla activa abona 2103010001 ni 2103090001)');
  } else {
    reglasAnticipo.forEach(r =>
      console.log(`  [prio ${r.prioridad}] ${r.nombre}`),
    );
  }

  // ── 8. Regla ganadora GLOBAL para tipo I (top 10) ────────────────────────
  const reglaGlobal  = {};
  let   sinReglaGlob = 0;
  for (const c of enriquecidos) {
    const rule = mappingSvc.findRuleInList(c, rules);
    if (!rule) { sinReglaGlob++; continue; }
    const rKey = `[prio ${rule.prioridad}] ${rule.nombre} → abono:${rule.cuentaAbono}`;
    reglaGlobal[rKey] = (reglaGlobal[rKey] ?? 0) + 1;
  }
  console.log('\n── Regla ganadora global (todos los tipo I, top 10) ─────────────────');
  Object.entries(reglaGlobal).sort((a, b) => b[1] - a[1]).slice(0, 10)
    .forEach(([k, v]) => console.log(`  ${String(v).padStart(5)}  ${k}`));
  if (sinReglaGlob > 0) {
    console.log(`  ${String(sinReglaGlob).padStart(5)}  SIN REGLA`);
  }

  console.log('\n──────────────────────────────────────────────────────────────────────');
  console.log('INTERPRETACIÓN:');
  console.log('  Si claveProdServ de los CFDIs "anticipo" ≠ 84111506 → las reglas Reg 22/22A');
  console.log('    NO matchean y los CFDIs caen en otra regla que no abona 2103010001.');
  console.log('  Solución: correr add-anticipo-fallback-rules.js para agregar Reg 22C-DESC');
  console.log('    y Reg 22C que usan conceptoContiene=\'anticipo\' como criterio de matching.');
  console.log('──────────────────────────────────────────────────────────────────────');

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
