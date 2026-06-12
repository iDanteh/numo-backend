'use strict';
require('dotenv').config();
const mongoose = require('mongoose');
const CFDI = require('./src/visor/models/CFDI');

async function main() {
  await mongoose.connect(process.env.MONGODB_URI);

  // Find all RFCs with tipo E in period 2026/2
  const rfcsEmisor = await CFDI.distinct('emisor.rfc', { tipoDeComprobante: 'E', ejercicio: 2026, periodo: 2, source: 'SAT', satStatus: 'Vigente', isActive: true });
  const rfcsReceptor = await CFDI.distinct('receptor.rfc', { tipoDeComprobante: 'E', ejercicio: 2026, periodo: 2, source: 'SAT', satStatus: 'Vigente', isActive: true });
  console.log('Emisores:', rfcsEmisor);
  console.log('Receptores count:', rfcsReceptor.length, rfcsReceptor.slice(0, 5));

  // Count per emisor
  const counts = await CFDI.aggregate([
    { $match: { tipoDeComprobante: 'E', ejercicio: 2026, periodo: 2, source: 'SAT', satStatus: 'Vigente', isActive: true }},
    { $group: { _id: '$emisor.rfc', count: { $sum: 1 }, total: { $sum: '$subTotal' }}},
    { $sort: { count: -1 }},
    { $limit: 10 },
  ]);
  console.log('\nPor emisor RFC (top 10):');
  counts.forEach(c => console.log(`  ${c._id}: count=${c.count} total=${c.total?.toFixed(2)}`));

  process.exit(0);
}
main().catch(e => { console.error(e.message); process.exit(1); });
