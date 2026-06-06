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
});

module.exports = mongoose.model('SATCredencial', satCredencialSchema);
