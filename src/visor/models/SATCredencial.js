const mongoose = require('mongoose');

/**
 * Almacena credenciales e.firma cifradas con AES-256-GCM.
 * TTL de 5 días — MongoDB las elimina automáticamente.
 */
const satCredencialSchema = new mongoose.Schema({
  rfc: {
    type: String,
    required: true,
    unique: true,
    uppercase: true,
    trim: true,
    index: true,
  },

  // Formato: iv:authTag:datosCifrados (hex separado por ':')
  cerCifrado: { type: String, required: true },
  keyCifrado: { type: String, required: true },
  passwordCifrado: { type: String, required: true },

  // TTL: MongoDB elimina el documento 5 días después de createdAt
  createdAt: {
    type: Date,
    default: Date.now,
    expires: 5 * 24 * 60 * 60, // 432000 segundos
  },

  // Qué avisos de vencimiento ya se mandaron para esta "generación" de
  // credenciales (se reinicia cada vez que se suben credenciales nuevas,
  // ver `guardar()` en sat/credenciales.js).
  alertasEnviadas: {
    d2:    { type: Boolean, default: false }, // 2 días antes
    d1:    { type: Boolean, default: false }, // 1 día antes
    horas: { type: Boolean, default: false }, // últimas N horas (SAT_CREDENCIALES_ALERTA_HORAS_ANTES)
  },
});

module.exports = mongoose.model('SATCredencial', satCredencialSchema);
