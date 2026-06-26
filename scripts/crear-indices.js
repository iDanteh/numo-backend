'use strict';
const mongoose = require('mongoose');
const URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/cfdi_comparator';

(async () => {
  await mongoose.connect(URI);
  const db = mongoose.connection.db;

  const cfdisIdx = [
    { uuid: 1 },
    { tipoDeComprobante: 1, isActive: 1, ejercicio: 1, periodo: 1 },
    { tipoDeComprobante: 1, isActive: 1 },
    { 'complementoPago.pagos.doctosRelacionados.idDocumento': 1 },
  ];
  for (const idx of cfdisIdx) {
    await db.collection('cfdis').createIndex(idx);
    console.log('cfdis index:', JSON.stringify(idx));
  }

  const bankIdx = [
    { 'erpLinks.folioFiscal': 1 },
    { isActive: 1 },
  ];
  for (const idx of bankIdx) {
    await db.collection('bank_movements').createIndex(idx);
    console.log('bank_movements index:', JSON.stringify(idx));
  }

  console.log('Todos los indices creados correctamente.');
  await mongoose.disconnect();
})();
