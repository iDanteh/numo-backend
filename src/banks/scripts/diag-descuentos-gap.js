'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { sequelize } = require('../../config/database.postgres');
const CFDI = require('../../visor/models/CFDI');
const { CfdiMappingRule } = require('../../shared/models/postgres');
const mappingSvc = require('../domains/cfdi-mapping/cfdi-mapping.service');

async function main() {
  await connectMongo();
  await sequelize.authenticate();
  const rules = await CfdiMappingRule.findAll({ where:{ isActive:true }, order:[['prioridad','ASC']] });
  const rulesPlain = rules.map(r => r.get({ plain:true }));

  const cfdis = await CFDI.find({
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    ejercicio: 2026, periodo: 2,
    tipoDeComprobante: 'E',
    source: 'SAT', satStatus: 'Vigente', isActive: true,
  }).select('uuid folio serie tipoDeComprobante subTotal total metodoPago formaPago emisor.rfc receptor.rfc descuento conceptos impuestos tipoOrigen cfdiRelacionados').lean();

  // Enriquecer con ERP (igual que balanza-preliminar)
  const uuids = cfdis.map(c => c.uuid).filter(Boolean);
  const erpDocs = await CFDI.find({ uuid: { $in: uuids }, source: 'ERP' })
    .select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados').lean();
  const erpMap = Object.fromEntries(erpDocs.map(e => [e.uuid, e]));

  const enriq = cfdis.map(c => {
    const erp = erpMap[c.uuid];
    if (!erp) return c;
    const satHasT = c.conceptos?.some(con => con.impuestos?.traslados?.length);
    const relSAT = c.cfdiRelacionados ?? [];
    const tiposEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP = (erp.cfdiRelacionados ?? []).filter(r => !tiposEnSAT.has(r.tipoRelacion));
    return { ...c,
      formaPago: c.formaPago || erp.formaPago,
      metodoPago: c.metodoPago || erp.metodoPago,
      conceptos: satHasT ? c.conceptos : (erp.conceptos?.length ? erp.conceptos : c.conceptos ?? []),
      impuestos: satHasT ? c.impuestos : (erp.impuestos ?? c.impuestos),
      tipoOrigen: c.tipoOrigen ?? erp.tipoOrigen ?? null,
      cfdiRelacionados: relERP.length ? [...relSAT, ...relERP] : relSAT,
      _tipoOrigenERP: erp.tipoOrigen ?? null,
    };
  });

  // Debug: ver campos del primer CFDI enriquecido y los primeros campos de la primera regla
  const sample = enriq[0];
  const todev = rulesPlain.find(r => r.nombre?.includes('TO-DEV-16 —') && r.tipoOrigen);
  console.log('Sample CFDI:', sample?.uuid?.substring(0,8), '| mP:', sample?.metodoPago, '| fP:', sample?.formaPago, '| tipoOrigen:', sample?.tipoOrigen);
  console.log('Rules count:', rulesPlain.length);
  if (todev) {
    const cfdiTO = sample?.tipoOrigen || '';
    const ruleTO = todev.tipoOrigen || '';
    console.log('TO-DEV tipoOrigen hex:', Buffer.from(ruleTO).toString('hex'), '=', ruleTO);
    console.log('CFDI  tipoOrigen hex:', Buffer.from(cfdiTO).toString('hex'), '=', cfdiTO);
    console.log('Match?', cfdiTO === ruleTO, '| tipoComp match?', todev.tipoComprobante === sample?.tipoDeComprobante);
  }
  // Forzar match manual del sample
  const manualRule = mappingSvc.findRuleInList(sample, rulesPlain);
  console.log('findRuleInList sample:', manualRule?.nombre ?? 'NINGUNA', '→', manualRule?.cuentaCargo);

  let t1 = 0, t2 = 0, sinRegla = 0;
  const byRule = {};
  const toCanNCs = []; // NCs que van a TO-CAN para analizar
  for (const c of enriq) {
    const rule = mappingSvc.findRuleInList(c, rulesPlain);
    if (!rule) { sinRegla++; continue; }
    const k = rule.cuentaCargo + ' | prio:' + rule.prioridad + ' | ' + rule.nombre.substring(0,45);
    byRule[k] = (byRule[k]||0) + Number(c.subTotal||0);
    if (rule.cuentaCargo === '4200010001') t1 += Number(c.subTotal||0);
    if (rule.cuentaCargo === '4200020001') t2 += Number(c.subTotal||0);
    // Guardar NCs que van a TO-CAN para ver sus conceptos
    if (rule.nombre?.startsWith('TO-CAN')) {
      const desc = (c.conceptos?.[0]?.descripcion ?? c.conceptos?.[0]?.Descripcion ?? '').substring(0,30);
      toCanNCs.push({ uuid: c.uuid?.substring(0,8), sub: Number(c.subTotal||0), desc, tipoOrigen: c._tipoOrigenERP });
    }
  }

  console.log('NCs tipo E SAT vigentes (enriquecidos):', enriq.length);
  console.log('→ 4200010001 Devoluciones subTotal:', t1.toFixed(2));
  console.log('→ 4200020001 Descuentos  subTotal:', t2.toFixed(2));
  console.log('Sin regla:', sinRegla);
  console.log('\nTop 15 por cuenta→regla:');
  Object.entries(byRule).sort((a,b)=>b[1]-a[1]).slice(0,15)
    .forEach(([k,v]) => console.log(' ', k, '→', v.toFixed(2)));

  // Analizar TO-CAN: conceptos COMPLETOS y sus montos
  console.log('\nTO-CAN NCs — conceptos completos (todos los distintos):');
  const byDescFull = {};
  toCanNCs.forEach(n => {
    const key = n.desc; // primeros 30 chars
    if (!byDescFull[key]) byDescFull[key] = { total: 0, n: 0 };
    byDescFull[key].total += n.sub;
    byDescFull[key].n += 1;
  });
  Object.entries(byDescFull).sort((a,b)=>b[1].total-a[1].total)
    .forEach(([k,v]) => console.log(' ', `"${k}"`, `→ $${v.total.toFixed(2)} (${v.n} NCs)`));

  // UUIDs conocidos que CONTPAQI pone en DESCUENTOS
  const UUID_DESCUENTOS = ['64D5A4F1','F4340578','46AE3078']; // C0-260200709, C0-260200300, C0-260200320
  // A0-260210804 lo buscaremos también

  const cancelacionPuras = toCanNCs.filter(n => n.desc && !n.desc.toLowerCase().includes('de cliente'));
  console.log('\nComparando campos ERP: NCs que van a Descuentos vs Devoluciones en CONTPAQI');

  // Enriquecer con MÁS campos del ERP
  const uuidsPuras = cancelacionPuras.map(n => n.uuid).filter(Boolean);
  const erpExtra = await CFDI.find({
    uuid: { $in: uuidsPuras.map(u => {
      // Buscar UUID completo que empieza con esos 8 chars
      return null; // placeholder
    }).filter(Boolean) },
    source: 'ERP'
  }).select('uuid tipoOrigen formaPago metodoPago cfdiRelacionados').limit(0).lean();

  // Buscar directamente los conocidos
  const conocidos = await CFDI.find({
    $or: [
      { serie:'C0', folio:'260200709', source:'ERP' },
      { serie:'C0', folio:'260200300', source:'ERP' },
      { serie:'C0', folio:'260200320', source:'ERP' },
      { serie:'A0', folio:'260210804', source:'ERP' },
    ]
  }).select('uuid serie folio tipoOrigen formaPago metodoPago cfdiRelacionados').lean();

  console.log('\n=== NCs CONOCIDAS → DESCUENTOS en CONTPAQI ===');
  conocidos.forEach(c => console.log(
    ' UUID:', c.uuid?.substring(0,8),
    '| serie:', c.serie, c.folio,
    '| tipoOrigen:', c.tipoOrigen,
    '| fP:', c.formaPago,
    '| rels:', (c.cfdiRelacionados||[]).map(r=>r.tipoRelacion).join(',')
  ));

  // Mostrar también una muestra de las que van a Devoluciones (el resto)
  const cancelPurasConERP = await CFDI.find({
    $or: [
      { serie:'C0', source:'ERP', tipoDeComprobante:'E', ejercicio:2026, periodo:2 },
      { serie:'B0', source:'ERP', tipoDeComprobante:'E', ejercicio:2026, periodo:2 },
    ],
    $and: [{ tipoOrigen:'Cancelación' }]
  }).select('uuid serie folio tipoOrigen formaPago cfdiRelacionados').limit(5).lean();

  console.log('\n=== MUESTRA de otras "CANCELACION *" → DEVOLUCIONES en CONTPAQI ===');
  cancelPurasConERP.forEach(c => {
    const desc = ''; // skip concept check
    console.log(
      ' UUID:', c.uuid?.substring(0,8),
      '| serie:', c.serie, c.folio,
      '| tipoOrigen:', c.tipoOrigen,
      '| fP:', c.formaPago,
      '| rels:', (c.cfdiRelacionados||[]).map(r=>r.tipoRelacion).join(',')
    );
  });
}
main().then(() => { disconnectMongo(); sequelize.close(); }).catch(console.error);
