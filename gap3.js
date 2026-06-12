'use strict';
require('dotenv').config();
const mongoose = require('mongoose');
const CFDI = require('./src/visor/models/CFDI');

async function main() {
  await mongoose.connect(process.env.MONGODB_URI);

  // Check what ejercicio/periodo data exists for tipo E
  const samples = await CFDI.find({ tipoDeComprobante: 'E', source: 'SAT', satStatus: 'Vigente', isActive: true })
    .select('uuid emisor.rfc receptor.rfc ejercicio periodo').limit(5).lean();
  console.log('Sample tipo E SAT Vigente:', JSON.stringify(samples, null, 2));

  // Check RFC
  const rfcCheck = await CFDI.distinct('emisor.rfc', { tipoDeComprobante: 'E', ejercicio: 2026, periodo: 2, source: 'SAT' });
  console.log('\nRFCs emisor en E/2026/2:', rfcCheck.slice(0, 10));

  process.exit(0);
}
main().catch(e => { console.error(e.message); process.exit(1); });
