'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');

async function main() {
  await connectMongo();

  const cfdi = await CFDI.findOne({ serie: 'B0', folio: '260802639', source: 'SAT' }).select('uuid folio serie total receptor.nombre fecha').lean();
  console.log('CFDI:', JSON.stringify(cfdi));

  // Buscar por folioFiscal (uuid) y por folioExterno (folio numerico), y tambien
  // cualquier BankMovement cuyo concepto/erpLinks mencione al cliente.
  const condiciones = [];
  if (cfdi?.uuid) condiciones.push({ 'erpLinks.folioFiscal': cfdi.uuid });
  if (cfdi?.folio) condiciones.push({ 'erpLinks.folioExterno': cfdi.folio });
  condiciones.push({ concepto: /ELECTRICA MEXICANA DE ANTEQUERA/i });

  const bms = await BankMovement.find({ $or: condiciones }).select('fecha concepto numeroAutorizacion erpLinks').lean();
  console.log(`BankMovements encontrados: ${bms.length}`);
  for (const bm of bms) {
    console.log('---');
    console.log('numeroAutorizacion:', bm.numeroAutorizacion, 'concepto:', bm.concepto, 'fecha:', bm.fecha);
    for (const l of bm.erpLinks ?? []) {
      console.log('  erpLink:', JSON.stringify({ serie: l.serie, folioExterno: l.folioExterno, total: l.total, folioFiscal: l.folioFiscal }));
      for (const mk of l.movimientosKore ?? []) {
        for (const fp of mk.formasPago ?? []) {
          console.log('    formaPago:', fp.formaPagoDescripcion, 'monto:', fp.monto, 'adicionales:', JSON.stringify(fp.adicionales));
        }
      }
    }
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
