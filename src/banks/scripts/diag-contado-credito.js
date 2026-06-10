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
  const rules = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']] });
  const rulesPlain = rules.map(r => r.get({ plain: true }));

  // Replicar filtro exacto de balanza-preliminar (excluye tipoRelacion=07)
  const sat = await CFDI.find({
    'emisor.rfc': 'CCO011113663',
    ejercicio: 2026, periodo: 2,
    tipoDeComprobante: 'I',
    source: 'SAT', satStatus: 'Vigente', isActive: true,
    'cfdiRelacionados.tipoRelacion': { $ne: '07' },
  }).select('uuid folio serie subTotal metodoPago formaPago impuestos tipoOrigen cfdiRelacionados conceptos descuento').lean();

  // Enriquecer igual que balanza-preliminar
  const uuidsParaEnriquecer = new Set(sat.filter(c =>
    !c.formaPago || !c.metodoPago || !c.conceptos?.length ||
    c.conceptos.every(con => !(con.impuestos?.traslados?.length)) ||
    (c.tipoDeComprobante === 'I' && c.metodoPago === 'PPD')
  ).map(c => c.uuid).filter(Boolean));

  const erpDocs = await CFDI.find({ uuid: { $in: [...uuidsParaEnriquecer] }, source: 'ERP' })
    .select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados').lean();
  const erpMap = Object.fromEntries(erpDocs.map(e => [e.uuid, e]));

  const enriq = sat.map(cfdi => {
    const erp = erpMap[cfdi.uuid];
    if (!erp) return { ...cfdi, _mpOrig: cfdi.metodoPago, _erpMp: null };
    const satHasT = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    const relSAT = cfdi.cfdiRelacionados ?? [];
    const tiposEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP = (erp.cfdiRelacionados ?? []).filter(r => !tiposEnSAT.has(r.tipoRelacion));
    const mpFinal = (cfdi.metodoPago === 'PPD' && erp.metodoPago === 'PUE') ? 'PUE' : (cfdi.metodoPago || erp.metodoPago);
    return {
      ...cfdi,
      formaPago: cfdi.formaPago || erp.formaPago,
      metodoPago: mpFinal,
      conceptos: satHasT ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos: satHasT ? cfdi.impuestos : (erp.impuestos ?? cfdi.impuestos),
      tipoOrigen: cfdi.tipoOrigen ?? erp.tipoOrigen ?? null,
      cfdiRelacionados: relERP.length ? [...relSAT, ...relERP] : relSAT,
      _mpOrig: cfdi.metodoPago, _erpMp: erp.metodoPago,
    };
  });

  // Aplicar reglas
  const byCuenta = {};
  const pueReg9 = [];
  const ppd99 = [], ppdOtro = [];

  for (const c of enriq) {
    const rule = mappingSvc.findRuleInList(c, rulesPlain);
    if (!rule) continue;
    const cuenta = rule.cuentaAbono;
    const sub = Number(c.subTotal || 0);

    if (cuenta?.startsWith('4100')) {
      if (!byCuenta[cuenta]) byCuenta[cuenta] = { total: 0, n: 0 };
      byCuenta[cuenta].total += sub;
      byCuenta[cuenta].n++;
    }

    if (c.metodoPago === 'PUE' && rule.nombre?.includes('Reg 9')) {
      pueReg9.push({ uuid: c.uuid?.substring(0, 8), serie: c.serie, folio: c.folio, fP: c.formaPago, sub });
    }
    if (c.metodoPago === 'PPD') {
      (c.formaPago === '99' ? ppd99 : ppdOtro).push({ uuid: c.uuid?.substring(0, 8), fP: c.formaPago, sub });
    }
  }

  // Resultados vs CONTPAQI
  const CONTPAQI = { '4100010001': 51621608.20, '4100010002': 72703.81, '4100020001': 21674064.38, '4100020002': 39035.84 };
  console.log('=== CONTADO vs CREDITO ===');
  console.log('Facturas I (sin tipoRel=07):', enriq.length);
  for (const [cuenta, v] of Object.entries(byCuenta).sort((a, b) => b[1].total - a[1].total)) {
    const cp = CONTPAQI[cuenta] ?? 0;
    console.log(cuenta, '|', v.total.toFixed(2), '| CP:', cp.toFixed(2), '| diff:', (v.total - cp).toFixed(2), '(' + v.n + ' facturas)');
  }

  const conv = enriq.filter(c => c._mpOrig === 'PPD' && c.metodoPago === 'PUE');
  console.log('\nPPD→PUE convertidos:', conv.length, '| total:', conv.reduce((s, c) => s + Number(c.subTotal || 0), 0).toFixed(2));

  console.log('\nFacturas PUE sin regla específica (→ Reg 9):', pueReg9.length, '| total:', pueReg9.reduce((s, d) => s + d.sub, 0).toFixed(2));
  pueReg9.forEach(d => console.log(' ', d.uuid, d.serie, d.folio, '| fP:', d.fP, '| sub:', d.sub.toFixed(2)));

  console.log('\nFacturas PPD con formaPago=99:', ppd99.length, '| total:', ppd99.reduce((s, d) => s + d.sub, 0).toFixed(2));
  console.log('Facturas PPD con formaPago≠99:', ppdOtro.length, '| total:', ppdOtro.reduce((s, d) => s + d.sub, 0).toFixed(2));
  const byFP = {};
  ppdOtro.forEach(c => { byFP[c.fP || 'null'] = (byFP[c.fP || 'null'] || 0) + c.sub; });
  Object.entries(byFP).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => console.log('  formaPago:', k, '| total:', v.toFixed(2)));
}

main()
  .then(() => { disconnectMongo(); sequelize.close(); })
  .catch(console.error);
