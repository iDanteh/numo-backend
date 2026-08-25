'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');

async function main() {
  await connectMongo();

  const cfdi = await CFDI.findOne({ serie: 'B0', folio: '260801134', source: 'SAT' }).select('uuid folio serie total receptor.nombre').lean();
  console.log('CFDI:', JSON.stringify(cfdi));

  if (cfdi?.uuid) {
    const bms = await BankMovement.find({ 'erpLinks.folioFiscal': cfdi.uuid }).select('fecha concepto numeroAutorizacion erpLinks').lean();
    console.log(`BankMovement con folioFiscal=${cfdi.uuid}: ${bms.length}`);
    for (const bm of bms) {
      console.log('numeroAutorizacion (documento):', bm.numeroAutorizacion, 'concepto:', bm.concepto, 'fecha:', bm.fecha);
      for (const l of bm.erpLinks) {
        if (l.folioFiscal === cfdi.uuid) {
          console.log('erpLink match:', JSON.stringify({ serie: l.serie, folioExterno: l.folioExterno, total: l.total }));
          for (const mk of l.movimientosKore ?? []) {
            for (const fp of mk.formasPago ?? []) {
              console.log('formaPago:', fp.formaPagoDescripcion, 'monto:', fp.monto, 'adicionales:', JSON.stringify(fp.adicionales));
            }
          }
        }
      }
    }
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
