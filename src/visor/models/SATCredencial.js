const mongoose = require('mongoose');

/**
 * Almacena credenciales e.firma cifradas con AES-256-GCM.
 * TTL de 8 horas — MongoDB las elimina automáticamente.
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

  // TTL: MongoDB elimina el documento 8 horas después de createdAt
  createdAt: {
    type: Date,
    default: Date.now,
    expires: 8 * 60 * 60, // 28800 segundos
  },
});

module.exports = mongoose.model('SATCredencial', satCredencialSchema);
