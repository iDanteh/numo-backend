'use strict';

/**
 * Migración: redondear a 2 decimales todos los campos monetarios
 * de los documentos ERP en la colección cfdis.
 *
 * Campos afectados:
 *   total, subTotal, descuento,
 *   impuestos.totalImpuestosTrasladados,
 *   impuestos.totalImpuestosRetenidos,
 *   complementoPago.totales.montoTotalPagos
 *
 * Uso:
 *   node scripts/round-erp-montos.js
 */

require('dotenv').config();
const mongoose = require('mongoose');

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/cfdi_comparator';

const r2 = (field) => ({ $round: [`$${field}`, 2] });

async function run() {
  await mongoose.connect(MONGODB_URI);
  console.log('MongoDB conectado');

  const db = mongoose.connection.db;
  const col = db.collection('cfdis');

  // Contar cuántos documentos ERP hay
  const total = await col.countDocuments({ source: 'ERP' });
  console.log(`Documentos ERP a procesar: ${total}`);

  const result = await col.updateMany(
    { source: 'ERP' },
    [
      {
        $set: {
          total:                                  r2('total'),
          subTotal:                               r2('subTotal'),
          descuento:                              r2('descuento'),
          'impuestos.totalImpuestosTrasladados':  r2('impuestos.totalImpuestosTrasladados'),
          'impuestos.totalImpuestosRetenidos':    r2('impuestos.totalImpuestosRetenidos'),
          'complementoPago.totales.montoTotalPagos': {
            $cond: {
              if:   { $ifNull: ['$complementoPago.totales.montoTotalPagos', false] },
              then: r2('complementoPago.totales.montoTotalPagos'),
              else: '$complementoPago.totales.montoTotalPagos',
            },
          },
        },
      },
    ]
  );

  console.log(`Documentos modificados: ${result.modifiedCount} de ${total}`);
  await mongoose.disconnect();
  console.log('Listo.');
}

run().catch((err) => {
  console.error('Error en migración:', err);
  process.exit(1);
});
