'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const { sequelize } = require('../../config/database.postgres');
const CFDI = require('../../visor/models/CFDI');
const { CfdiMappingRule, AccountPlan } = require('../../shared/models/postgres');
const mappingSvc = require('../domains/cfdi-mapping/cfdi-mapping.service');

const UUIDS = [
  '6B025962-60EB-426C-ABC0-DBD805037037','5FAA1C13-1555-41F9-A856-0F81E456710A',
  '8347A6AB-AE5F-4A7A-8EBA-9B574042B5B1','30986792-3C3C-4DE0-9458-4340B876654B',
  '402558FF-57FA-44DE-8E18-950E767C3AD6','673BB243-1596-4AEE-BB19-7AB504D2C78E',
  'C2757025-58E3-4476-B97C-0BAAB93BDB9E','7DDDE068-C431-44F4-90C2-F0B16CA1CB87',
  '45929E4D-255F-44AE-9842-A8D42F86D43A','62FC7256-1C05-4E3D-893B-5E0B6D7B4C64',
  '564D5863-665C-419D-8B40-016E330C3093','7F471A86-C320-451D-B6B7-75053A30978C',
  'E94BC324-D8E5-4E50-8B1E-2E29EBB3D9C7','90C2DBAE-809D-4AC1-AA26-246CE91643CA',
  'CF029D2C-53DC-4916-AE0C-A36653DFB324','A7333DAA-721F-4789-9FBE-7826BBFACEB7',
  'DD0DB65B-D181-43D2-90AD-F06D141C7765','BD00DBDE-D5A4-4858-8CB9-2D6718ADDC87',
  '608DEBC5-5489-40AC-81D2-0CCF0813A35A','384709AE-9D35-4D04-B008-6946C459CD9E',
];

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const rules = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']] });
  const rulesPlain = rules.map(r => r.get({ plain: true }));
  const cuentas = await AccountPlan.findAll();
  const cuentaMapByCod = Object.fromEntries(cuentas.map(c => [c.codigo, c.id]));
  const cuentaId4100 = cuentaMapByCod['4100020001'];

  const sat = await CFDI.find({ uuid: { $in: UUIDS }, source: 'SAT' })
    .select('uuid serie folio tipoDeComprobante subTotal total metodoPago formaPago emisor.rfc receptor.rfc descuento impuestos conceptos cfdiRelacionados tipoOrigen').lean();
  const erpDocs = await CFDI.find({ uuid: { $in: UUIDS }, source: 'ERP' })
    .select('uuid formaPago metodoPago conceptos impuestos tipoOrigen cfdiRelacionados').lean();
  const erpMap = Object.fromEntries(erpDocs.map(e => [e.uuid, e]));

  const enriq = sat.map(cfdi => {
    const erp = erpMap[cfdi.uuid];
    if (!erp) return cfdi;
    const satHasT = cfdi.conceptos?.some(con => con.impuestos?.traslados?.length);
    const relSAT = cfdi.cfdiRelacionados ?? [];
    const tiposEnSAT = new Set(relSAT.map(r => r.tipoRelacion));
    const relERP = (erp.cfdiRelacionados ?? []).filter(r => !tiposEnSAT.has(r.tipoRelacion));
    const mpFinal = (cfdi.metodoPago === 'PPD' && erp.metodoPago === 'PUE') ? 'PUE' : (cfdi.metodoPago || erp.metodoPago);
    return {
      ...cfdi, formaPago: cfdi.formaPago || erp.formaPago, metodoPago: mpFinal,
      conceptos: satHasT ? cfdi.conceptos : (erp.conceptos?.length ? erp.conceptos : cfdi.conceptos ?? []),
      impuestos: satHasT ? cfdi.impuestos : (erp.impuestos ?? cfdi.impuestos),
      tipoOrigen: cfdi.tipoOrigen ?? erp.tipoOrigen ?? null,
      cfdiRelacionados: relERP.length ? [...relSAT, ...relERP] : relSAT,
    };
  }).sort((a, b) => Number(b.subTotal || 0) - Number(a.subTotal || 0));

  console.log('Factura        | SubTotal      | HABER ingreso | Diff');
  let ts = 0, tm = 0;
  for (const c of enriq) {
    const rule = mappingSvc.findRuleInList(c, rulesPlain);
    if (!rule) { console.log(c.serie + c.folio, 'SIN REGLA'); continue; }
    const movs = await mappingSvc.cfdiToMovimientos(c, rule, cuentaMapByCod);
    const haber = movs.filter(m => m.cuentaId === cuentaId4100 && m.haber > 0).reduce((s, m) => s + m.haber, 0);
    const sub = Number(c.subTotal || 0);
    ts += sub; tm += haber;
    console.log((c.serie + c.folio).padEnd(14), '|', sub.toFixed(2).padStart(13), '|', haber.toFixed(2).padStart(13), '|', (haber - sub).toFixed(2).padStart(10));
  }
  console.log('-'.repeat(58));
  console.log('TOTAL'.padEnd(14), '|', ts.toFixed(2).padStart(13), '|', tm.toFixed(2).padStart(13), '|', (tm - ts).toFixed(2).padStart(10));
}

main().then(() => { disconnectMongo(); sequelize.close(); }).catch(console.error);
