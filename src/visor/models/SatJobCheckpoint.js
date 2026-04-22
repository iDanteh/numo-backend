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

  idSolicitud:         { type: String, default: null },
  idsPaquetes:         [String],                         // lista completa del SAT
  paquetesProcesados:  [String],                         // los ya descargados y guardados
  totalReportadoSAT:   { type: Number, default: 0 },     // NumeroCFDIs de verificación

  status: {
    type: String,
    enum: ['solicitando', 'verificando', 'descargando', 'completado', 'error'],
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

module.exports = mongoose.model('SatJobCheckpoint', satJobCheckpointSchema);
