'use strict';

const mongoose = require('mongoose');

/**
 * Persistencia de límites de descarga masiva SAT por RFC.
 * Se usa como respaldo del Map en memoria: si el proceso se reinicia,
 * cargamos cuántas solicitudes ya se hicieron hoy para no sobrepasarlas.
 * Las "activas" NO se persisten — al reiniciar el proceso todas terminaron.
 */
const satRateLimitSchema = new mongoose.Schema({
  rfc:         { type: String, required: true, uppercase: true, unique: true },
  fecha:       { type: String, required: true },   // YYYY-MM-DD en CDMX
  solicitudes: { type: Number, default: 0 },
  updatedAt:   { type: Date,   default: Date.now },
}, { collection: 'sat_rate_limits' });

module.exports = mongoose.model('SatRateLimit', satRateLimitSchema);
