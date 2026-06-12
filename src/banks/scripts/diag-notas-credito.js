'use strict';

/**
 * diag-notas-credito.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Diagnóstico de notas de crédito (tipo E) para identificar por qué van a
 * Descuentos en lugar de Devoluciones.
 *
 * Muestra para cada NC:
 *   - descripción del primer concepto
 *   - tipoRelacion del cfdiRelacionados
 *   - tipoOrigen (SAT derivado + ERP si existe)
 *   - regla que ganó y la cuenta cargo resultante
 *
 * Uso:
 *   node src/banks/scripts/diag-notas-credito.js
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

  // ── 1. Cargar NCs SAT ──────────────────────────────────────────────────────
  const cfdis = await CFDI.find({
    $or:               [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio:         EJERCICIO,
    periodo:           PERIODO,
    tipoDeComprobante: 'E',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
  })
    .select('uuid tipoDeComprobante metodoPago formaPago emisor.rfc receptor.rfc subTotal total descuento impuestos conceptos cfdiRelacionados tipoOrigen')
    .lean();

  console.log(`\nTotal NCs tipo E SAT encontradas: ${cfdis.length}`);

  // ── 2. Enriquecer con ERP ──────────────────────────────────────────────────
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
    const relSAT  = cfdi.cfdiRelacionados ?? [];
    const tipoEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP  = (erp.cfdiRelacionados ?? []).filter(r => !tipoEnSAT.has(r.tipoRelacion));
    return {
      ...cfdi,
      formaPago:        cfdi.formaPago  || erp.formaPago,
      metodoPago:       cfdi.metodoPago || erp.metodoPago,
      conceptos:        satHasTraslados ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos:        satHasTraslados ? cfdi.impuestos : (erp.impuestos ?? cfdi.impuestos),
      tipoOrigen:       cfdi.tipoOrigen ?? erp.tipoOrigen ?? null,
      cfdiRelacionados: relERP.length ? [...relSAT, ...relERP] : relSAT,
      _tipoOrigenERP:   erp.tipoOrigen ?? null,
    };
  });

  // ── 3. Analizar ────────────────────────────────────────────────────────────
  const cuentaTally = {};
  const reglaTally  = {};
  let   sinRegla    = 0;

  // Contadores para tipoRelacion y tipoOrigen
  const relacionTally  = {};
  const origenTally    = {};
  const conceptoTally  = {};

  console.log('\n── MUESTRA: primeras 10 NCs ──────────────────────────────────────────');
  const muestra = enriquecidos.slice(0, 10);
  for (const c of muestra) {
    const desc       = (c.conceptos?.[0]?.descripcion ?? c.conceptos?.[0]?.Descripcion ?? '(sin descripción)').substring(0, 60);
    const relaciones = (c.cfdiRelacionados ?? []).map(r => r.tipoRelacion).join(', ') || '—';
    const origenERP  = c._tipoOrigenERP ?? '(sin ERP)';
    const origenDeriv = mappingSvc._derivarTipoOrigenPublic ? mappingSvc._derivarTipoOrigenPublic(c) : '?';
    const rule       = mappingSvc.findRuleInList(c, rules);
    console.log({
      uuid:       c.uuid?.substring(0, 8),
      concepto:   desc,
      relaciones,
      tipoOrigenERP:    origenERP,
      tipoOrigenDerivado: origenDeriv,
      regla:      rule ? `[${rule.prioridad}] ${rule.nombre}` : 'SIN REGLA',
      cuentaCargo: rule?.cuentaCargo ?? '—',
    });
  }

  // ── 4. Estadísticas globales ───────────────────────────────────────────────
  for (const c of enriquecidos) {
    // tipoRelacion
    const relaciones = (c.cfdiRelacionados ?? []).map(r => r.tipoRelacion).join(',') || '(ninguna)';
    relacionTally[relaciones] = (relacionTally[relaciones] ?? 0) + 1;

    // tipoOrigen ERP
    const origenERP = c._tipoOrigenERP ?? '(sin ERP)';
    origenTally[origenERP] = (origenTally[origenERP] ?? 0) + 1;

    // primera palabra del concepto
    const desc = (c.conceptos?.[0]?.descripcion ?? c.conceptos?.[0]?.Descripcion ?? '').toLowerCase().trim();
    const palabraKey = desc.split(/\s+/)[0] || '(vacío)';
    conceptoTally[palabraKey] = (conceptoTally[palabraKey] ?? 0) + 1;

    // regla ganadora
    const rule = mappingSvc.findRuleInList(c, rules);
    if (!rule) { sinRegla++; continue; }
    const rKey = `[prio ${rule.prioridad}] ${rule.nombre} → cargo:${rule.cuentaCargo}`;
    reglaTally[rKey] = (reglaTally[rKey] ?? 0) + 1;
    cuentaTally[rule.cuentaCargo] = (cuentaTally[rule.cuentaCargo] ?? 0) + 1;
  }

  // ── 4B. Análisis específico Club Tuberos ──────────────────────────────────
  const cltNCs = enriquecidos.filter(c => {
    const desc = (c.conceptos?.[0]?.descripcion ?? c.conceptos?.[0]?.Descripcion ?? '').toLowerCase();
    return desc.includes('club tuberos');
  });

  console.log(`\n── Club Tuberos: ${cltNCs.length} NCs ──────────────────────────────────`);

  // RFC Receptor — discriminador potencial
  const rfcTally = {};
  const rfcMonto = {};
  for (const c of cltNCs) {
    const rfc = c.receptor?.rfc ?? '(sin RFC)';
    rfcTally[rfc] = (rfcTally[rfc] ?? 0) + 1;
    rfcMonto[rfc] = (rfcMonto[rfc] ?? 0) + Number(c.subTotal || 0);
  }
  const rfcEntries = Object.entries(rfcTally).sort((a, b) => b[1] - a[1]);
  console.log(`\n  RFC Receptor: ${rfcEntries.length} clientes distintos`);
  rfcEntries.slice(0, 20).forEach(([rfc, n]) =>
    console.log(`    ${String(n).padStart(4)} NCs  $${(rfcMonto[rfc] ?? 0).toLocaleString('es-MX', { minimumFractionDigits: 2 }).padStart(12)}  ${rfc}`)
  );
  if (rfcEntries.length > 20) console.log(`    ... y ${rfcEntries.length - 20} más`);

  // Descripción completa del concepto — ¿hay variaciones?
  const conceptoFullTally = {};
  for (const c of cltNCs) {
    const d = (c.conceptos?.[0]?.descripcion ?? c.conceptos?.[0]?.Descripcion ?? '(sin)').trim();
    conceptoFullTally[d] = (conceptoFullTally[d] ?? 0) + 1;
  }
  console.log(`\n  Conceptos distintos: ${Object.keys(conceptoFullTally).length}`);
  Object.entries(conceptoFullTally).sort((a, b) => b[1] - a[1]).slice(0, 10)
    .forEach(([d, n]) => console.log(`    ${String(n).padStart(4)}  "${d}"`));

  // formaPago por RFC — ¿algún RFC usa formaPago distinto?
  const cltFormaPagoTally = {};
  const cltMontoByFormaPago = {};
  for (const c of cltNCs) {
    const fp   = c.formaPago ?? '(sin formaPago)';
    const rule = mappingSvc.findRuleInList(c, rules);
    const key  = `formaPago=${fp}  abono:${rule?.cuentaAbono ?? '—'}`;
    cltFormaPagoTally[key]    = (cltFormaPagoTally[key]    ?? 0) + 1;
    cltMontoByFormaPago[key]  = (cltMontoByFormaPago[key]  ?? 0) + Number(c.subTotal || 0);
  }
  console.log('\n  formaPago | abono | NCs | subTotal');
  Object.entries(cltFormaPagoTally).sort((a, b) => b[1] - a[1])
    .forEach(([k, v]) =>
      console.log(`    ${String(v).padStart(4)}  $${(cltMontoByFormaPago[k] ?? 0).toLocaleString('es-MX', {minimumFractionDigits: 2})}  ${k}`)
    );

  // ── 4C. Tipo P (pagos PPD) con formaPago=05 — consumo de monedero ──────────
  const pagosP = await CFDI.find({
    $or:               [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio:         EJERCICIO,
    periodo:           PERIODO,
    tipoDeComprobante: 'P',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
    formaPago:         '05',
  })
    .select('uuid formaPago complementoPago.totales')
    .lean();

  // CFDI 4.0: complementoPago.totales.montoTotalPagos
  // CFDI 3.3: complementoPago.pagos[].monto (suma de parcialidades)
  const montoP05 = pagosP.reduce((s, c) => {
    const v40 = Number(c.complementoPago?.totales?.montoTotalPagos || 0);
    const v33 = (c.complementoPago?.pagos ?? []).reduce((ps, p) => ps + Number(p.monto || 0), 0);
    return s + (v40 || v33);
  }, 0);
  console.log(`\n── Tipo P formaPago=05 (monedero Club Tuberos): ${pagosP.length} pagos | Monto: $${montoP05.toLocaleString('es-MX', { minimumFractionDigits: 2 })}`);

  // Tipo I formaPago=05 — ventas pagadas con monedero (PUE y PPD)
  const ventasMonederoPUE = await CFDI.find({
    $or:               [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio:         EJERCICIO,
    periodo:           PERIODO,
    tipoDeComprobante: 'I',
    metodoPago:        'PUE',
    formaPago:         '05',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
  }).select('uuid subTotal total metodoPago').lean();

  const ventasMonederoPPD = await CFDI.find({
    $or:               [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio:         EJERCICIO,
    periodo:           PERIODO,
    tipoDeComprobante: 'I',
    metodoPago:        'PPD',
    formaPago:         '05',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
  }).select('uuid subTotal total metodoPago').lean();

  const montoI05PUE = ventasMonederoPUE.reduce((s, c) => s + Number(c.total || 0), 0);
  const montoI05PPD = ventasMonederoPPD.reduce((s, c) => s + Number(c.total || 0), 0);
  const montoI05    = montoI05PUE + montoI05PPD;

  console.log(`── Tipo I PUE formaPago=05: ${ventasMonederoPUE.length} CFDIs | Total: $${montoI05PUE.toLocaleString('es-MX', { minimumFractionDigits: 2 })}`);
  console.log(`── Tipo I PPD formaPago=05: ${ventasMonederoPPD.length} CFDIs | Total: $${montoI05PPD.toLocaleString('es-MX', { minimumFractionDigits: 2 })}`);
  console.log(`── Total tipo I formaPago=05: ${ventasMonederoPUE.length + ventasMonederoPPD.length} CFDIs | $${montoI05.toLocaleString('es-MX', { minimumFractionDigits: 2 })}`);

  // También buscar tipo I de TODOS los periodos del ejercicio para detectar
  // si hay monedero de periodos anteriores que se procesa aquí
  const ventasMonederoEjercicio = await CFDI.find({
    $or:               [{ 'emisor.rfc': RFC }, { 'receptor.rfc': RFC }],
    ejercicio:         EJERCICIO,
    tipoDeComprobante: 'I',
    formaPago:         '05',
    source:            'SAT',
    satStatus:         'Vigente',
    isActive:          true,
  }).select('uuid subTotal total metodoPago periodo').lean();

  console.log(`── Tipo I formaPago=05 TODO el ejercicio ${EJERCICIO}: ${ventasMonederoEjercicio.length} CFDIs`);
  const porPeriodo = {};
  for (const c of ventasMonederoEjercicio) {
    porPeriodo[c.periodo] = (porPeriodo[c.periodo] ?? 0) + 1;
  }
  Object.entries(porPeriodo).sort().forEach(([p, n]) => console.log(`   periodo ${p}: ${n} CFDIs`));

  console.log(`\n   → NUMO cargo 2103090002 capturado = $${(montoP05 + montoI05).toLocaleString('es-MX', { minimumFractionDigits: 2 })}`);
  console.log(`   → CONTPAQI cargo 2103090002        = $200,090.16`);
  console.log(`   → Brecha                           = $${(200090.16 - montoP05 - montoI05).toLocaleString('es-MX', { minimumFractionDigits: 2 })}`);

  // ── 4D. ¿El fix CC-CLT-EF fue correcto? ────────────────────────────────────
  console.log('\n── Verificación fix CC-CLT-EF ─────────────────────────────────────────');
  console.log(`  256 NCs "Bonificacion club tuberos" formaPago=01`);
  console.log(`  SubTotal: $191,814  →  Con IVA: ~$222,504`);
  console.log(`  CONTPAQI abono 2103090002: $163,616`);
  console.log(`  CONTPAQI abono implícito Caja/Clientes: ~$${(222504 - 163616).toLocaleString('es-MX')}`);
  console.log(`  `);
  console.log(`  Conclusión: $163k (~73%) de esas NCs debería ir a 2103090002 (monedero)`);
  console.log(`              $58k  (~27%) debería ir a 1101010003 (Caja)`);
  console.log(`  Pero NO hay discriminador en el CFDI SAT para distinguirlos.`);
  console.log(`  → Recomendación: revertir CC-CLT-EF y aceptar el $58k de diferencia`);

  console.log('\n── tipoRelacion en cfdiRelacionados ──────────────────────────────────');
  Object.entries(relacionTally).sort((a, b) => b[1] - a[1])
    .forEach(([k, v]) => console.log(`  ${String(v).padStart(5)}  ${k}`));

  console.log('\n── tipoOrigen del ERP ────────────────────────────────────────────────');
  Object.entries(origenTally).sort((a, b) => b[1] - a[1])
    .forEach(([k, v]) => console.log(`  ${String(v).padStart(5)}  ${k}`));

  console.log('\n── Primera palabra del concepto (top 15) ─────────────────────────────');
  Object.entries(conceptoTally).sort((a, b) => b[1] - a[1]).slice(0, 15)
    .forEach(([k, v]) => console.log(`  ${String(v).padStart(5)}  ${k}`));

  console.log('\n── Regla ganadora por CFDI ───────────────────────────────────────────');
  Object.entries(reglaTally).sort((a, b) => b[1] - a[1])
    .forEach(([k, v]) => console.log(`  ${String(v).padStart(5)}  ${k}`));

  console.log('\n── Cuenta cargo resultante ───────────────────────────────────────────');
  Object.entries(cuentaTally).sort((a, b) => b[1] - a[1])
    .forEach(([k, v]) => console.log(`  ${String(v).padStart(5)}  ${k}`));

  console.log(`\n  Sin regla: ${sinRegla}`);
  console.log('\n──────────────────────────────────────────────────────────────────────');
  console.log('INTERPRETACIÓN:');
  console.log('  Si "Primera palabra del concepto" = "bonificacion" → las NCs van a');
  console.log('    CC-BON (Descuentos) porque la prioridad 75 gana sobre Reg 8A (80).');
  console.log('  Si "tipoOrigen del ERP" = "Devolución" → se puede crear una regla');
  console.log('    con tipoOrigen=Devolución a prio 72 para rescatar esas NCs.');
  console.log('  Si tipoRelacion = "01" pero concepto = "bonificacion" → mismo caso.');

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
