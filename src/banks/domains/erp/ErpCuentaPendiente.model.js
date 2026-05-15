'use strict';
const mongoose = require('mongoose');

const formaPagoSchema = new mongoose.Schema({
  formasPago:  { type: String, default: null },
  monto:       { type: Number, default: null },
  autorizacion:{ type: String, default: null },
}, { _id: false });

const movimientoSchema = new mongoose.Schema({
  serie:          { type: String, default: null },
  folio:          { type: String, default: null },
  serieOrigen:    { type: String, default: null },
  folioOrigen:    { type: String, default: null },
  saldoAnterior:  { type: Number, default: null },
  saldoActual:    { type: Number, default: null },
  subtotal:       { type: Number, default: null },
  impuesto:       { type: Number, default: null },
  total:          { type: Number, default: null },
  formasPago:     { type: [formaPagoSchema], default: [] },
}, { _id: false });

const erpCuentaPendienteSchema = new mongoose.Schema({

  // Clave natural del ERP — se usa como filtro en el upsert (idempotente)
  erpId: { type: String, required: true, unique: true, index: true },

  serie:          { type: String, default: null },
  folio:          { type: String, default: null },
  serieExterna:   { type: String, default: null },
  folioExterno:   { type: String, default: null },
  folioFiscal:    { type: String, default: null },
  tipoPago:       { type: String, default: null },
  subtotal:       { type: Number, default: null },
  impuesto:       { type: Number, default: null },
  total:          { type: Number, default: null },
  saldoActual:    { type: Number, default: null },

  fechaCreacion:      { type: Date, default: null },
  fechaRealPago:      { type: Date, default: null },
  fechaAfectacion:    { type: Date, default: null },
  fechaVencimiento:   { type: Date, default: null },
  fechaProgramada:    { type: Date, default: null },

  concepto:           { type: String, default: null },
  conceptoCobroID:    { type: String, default: null },
  almacen:            { type: String, default: null },
  personaId:          { type: String, default: null },
  claveImpuesto:      { type: String, default: null },
  factorImpuesto:     { type: Number, default: null },
  anotacion:          { type: String, default: null },
  plazo:              { type: Number, default: null },
  tipoMovimiento:     { type: String, default: null },

  movimientos: { type: [movimientoSchema], default: [] },

  // Última vez que el ERP devolvió este registro
  lastSeenAt: { type: Date, default: null },

  // Autorizaciones normalizadas pre-computadas al momento del sync.
  // Permite la query inversa del motor Match ERP (Option B):
  //   authNormSet (de movimientos bancarios) → { _autsNorm: { $in: [...] } }
  // Se llena con extraerAutsNorm(movimientos) en erp-sync.service.js.
  _autsNorm: { type: [String], default: [] },

}, { timestamps: true, collection: 'erp_cuentas_pendientes' });

// Índices para el motor Match ERP
erpCuentaPendienteSchema.index({ lastSeenAt: -1 });        // auto-sync: findOne + sort para verificar frescura del caché
erpCuentaPendienteSchema.index({ fechaRealPago:    1 });   // filtro fechaDesde en la carga de CxC
erpCuentaPendienteSchema.index({ fechaAfectacion:  1 });   // filtro fechaDesde en la carga de CxC
erpCuentaPendienteSchema.index({ _autsNorm: 1 });          // Option B: query inversa movements → CxC por auth

module.exports = mongoose.model('ErpCuentaPendiente', erpCuentaPendienteSchema);
