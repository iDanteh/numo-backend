'use strict';

const mongoose = require('mongoose');

/**
 * Checkpoint de descarga masiva SAT.
 * Permite reanudar desde el último paquete exitoso si el proceso se interrumpe.
 */
const satJobCheckpointSchema = new mongoose.Schema({
  rfc:             { type: String, required: true, uppercase: true },
  fecha:           { type: String, required: true },   // YYYY-MM-DD
  tipoComprobante: { type: String, required: true },
  ejercicio:       { type: Number, required: true },
  periodo:         { type: Number, required: true },

  fechaFin:            { type: String, default: null },   // YYYY-MM-DD (extremo del rango)

  idSolicitud:         { type: String, default: null },
  idsPaquetes:         [String],                         // lista completa del SAT
  paquetesProcesados:  [String],                         // los ya descargados y guardados
  paquetesFallidos:    [String],                         // los que agotaron sus 2 intentos SAT
  totalReportadoSAT:   { type: Number, default: 0 },     // NumeroCFDIs de verificación
  cfdisDescargados:    { type: Number, default: 0 },     // CFDIs realmente descargados
  reintentos:          { type: Number, default: 0 },     // veces que se reintentó esta descarga

  status: {
    type: String,
    enum: ['solicitando', 'verificando', 'descargando', 'completado', 'incompleto', 'error'],
    default: 'solicitando',
  },
  error:    { type: String, default: null },
  startedAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
}, { collection: 'sat_job_checkpoints' });

satJobCheckpointSchema.index(
  { rfc: 1, fecha: 1, tipoComprobante: 1 },
  { unique: true },
);

// Índice para queries por status (reintentarIncompletos, getCheckpointsSalud)
satJobCheckpointSchema.index({ status: 1, updatedAt: -1 });

module.exports = mongoose.model('SatJobCheckpoint', satJobCheckpointSchema);
