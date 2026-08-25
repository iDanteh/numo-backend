'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const BankMovement = require('./src/banks/domains/banks/BankMovement.model');

async function main() {
  await connectMongo();

  const bms = await BankMovement.find({ 'erpLinks.folioExterno': '260801134' }).select('fecha concepto numeroAutorizacion erpLinks').lean();
  console.log(`Por folioExterno=260801134: ${bms.length}`);
  for (const bm of bms) {
    console.log('numeroAutorizacion:', bm.numeroAutorizacion, 'concepto:', bm.concepto, 'fecha:', bm.fecha);
    for (const l of bm.erpLinks) {
      if (l.folioExterno === '260801134') {
        console.log('erpLink:', JSON.stringify({ serie: l.serie, folioExterno: l.folioExterno, total: l.total, folioFiscal: l.folioFiscal }));
        for (const mk of l.movimientosKore ?? []) {
          for (const fp of mk.formasPago ?? []) {
            console.log('  formaPago:', fp.formaPagoDescripcion, 'monto:', fp.monto, 'adicionales:', JSON.stringify(fp.adicionales));
          }
        }
      }
    }
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
