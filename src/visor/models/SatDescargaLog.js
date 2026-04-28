'use strict';

const mongoose = require('mongoose');

/**
 * Log persistente de descargas SAT (automáticas y manuales).
 * Se crea una entrada por cada ejecución de procesarDescarga.
 */
const satDescargaLogSchema = new mongoose.Schema({
  rfc:             { type: String, required: false, uppercase: true, index: true, default: 'SISTEMA' },
  tipo:            { type: String, enum: ['automatica', 'manual', 'erp_automatica'], required: true },
  tipoComprobante: { type: String, default: 'Emitidos' },
  fechaInicio:     { type: String },   // YYYY-MM-DD del rango solicitado al SAT
  fechaFin:        { type: String },   // YYYY-MM-DD del rango solicitado al SAT
  ejercicio:       { type: Number },
  periodo:         { type: Number },
  estado:          { type: String, enum: ['en_proceso', 'completado', 'error'], default: 'en_proceso' },
  error:           { type: String, default: null },
  // Resultados de la descarga
  totalSAT:          { type: Number, default: 0 },
  totalERP:          { type: Number, default: 0 },
  coinciden:         { type: Number, default: 0 },
  soloSAT:           { type: Number, default: 0 },
  soloERP:           { type: Number, default: 0 },
  diferencias:       { type: Number, default: 0 },
  paquetes:          { type: Number, default: 0 },
  // Detección de descarga incompleta
  totalReportadoSAT: { type: Number, default: 0 }, // NumeroCFDIs reportado por SAT en verificación
  incompleta:        { type: Boolean, default: false }, // true si se descargó <95% de lo reportado
  inicio: { type: Date, default: Date.now },
  fin:    { type: Date },
}, { collection: 'sat_descarga_logs', timestamps: false });

satDescargaLogSchema.index({ rfc: 1, inicio: -1 });
satDescargaLogSchema.index({ inicio: -1 });

module.exports = mongoose.model('SatDescargaLog', satDescargaLogSchema);
