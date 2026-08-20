'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const mappingSvc = require('./src/banks/domains/cfdi-mapping/cfdi-mapping.service.js');
const CfdiMappingRule = require('./src/shared/models/postgres/CfdiMappingRule');

const FOLIO = process.env.DIAG_FOLIO || '260801150';
const SERIE = process.env.DIAG_SERIE || 'B0';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const cfdi = await CFDI.findOne({ serie: SERIE, folio: FOLIO, source: 'SAT', satStatus: 'Vigente', isActive: true }).lean();
  if (!cfdi) { console.log('CFDI NO ENCONTRADO'); process.exit(1); }
  console.log('CFDI encontrado:', JSON.stringify({
    uuid: cfdi.uuid, serie: cfdi.serie, folio: cfdi.folio, total: cfdi.total,
    formaPago: cfdi.formaPago, metodoPago: cfdi.metodoPago,
    emisorRfc: cfdi.emisor?.rfc, receptorRfc: cfdi.receptor?.rfc, receptorNombre: cfdi.receptor?.nombre,
    tipoDeComprobante: cfdi.tipoDeComprobante, descuento: cfdi.descuento,
    conceptos: (cfdi.conceptos ?? []).map(c => ({ claveProdServ: c.claveProdServ, descripcion: c.descripcion })),
  }, null, 2));

  const rules = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']] });
  console.log(`\nTotal reglas activas: ${rules.length}`);

  const elegida = mappingSvc.findRuleInList(cfdi, rules);
  console.log('\nRegla elegida por findRuleInList:', elegida ? JSON.stringify({ nombre: elegida.nombre, prioridad: elegida.prioridad, cuentaCargo: elegida.cuentaCargo, rfcReceptor: elegida.rfcReceptor, formaPago: elegida.formaPago, tasaIva: elegida.tasaIva }) : 'NINGUNA');

  console.log('\nReglas cuyo rfcReceptor coincide con el receptor de esta factura (o es null):');
  for (const r of rules) {
    if (!r.rfcReceptor || r.rfcReceptor === cfdi.receptor?.rfc) {
      console.log(`  prioridad=${r.prioridad} nombre="${r.nombre}" cuentaCargo=${r.cuentaCargo} rfcReceptor=${r.rfcReceptor} formaPago=${r.formaPago} tasaIva=${r.tasaIva} tipoComprobante=${r.tipoComprobante} metodoPago=${r.metodoPago}`);
    }
  }

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
