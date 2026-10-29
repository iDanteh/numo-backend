'use strict';
const mongoose = require('mongoose');

/**
 * Datos de una remisión de Kore que NO cambian (2026-10-29, reporte "Por
 * facturar"): cliente, tipo de pago y estatus ("POR FACTURAR", "TICKET
 * SENCILLO"… — Kore no lo cambia al facturar). Se guardan la primera vez que
 * se consultan en /cuentas-pendientes para no volver a preguntarle a Kore por
 * cada remisión en cada reporte (con cientos, Kore responde 429). El saldo y
 * la factura sí cambian: NO se guardan aquí.
 */
const koreRemisionSchema = new mongoose.Schema({
  rfc:                  { type: String, required: true },
  serie:                { type: String, required: true },
  folio:                { type: String, required: true },
  nombre:               { type: String, default: null },
  tipoPago:             { type: String, default: null },
  nombreTipoMovimiento: { type: String, default: null },
  consultadoEn:         { type: Date,   default: null },
}, { timestamps: true, collection: 'kore_remisiones' });

koreRemisionSchema.index({ rfc: 1, serie: 1, folio: 1 }, { unique: true });

module.exports = mongoose.model('KoreRemision', koreRemisionSchema);
