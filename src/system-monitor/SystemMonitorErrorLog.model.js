'use strict';

const mongoose = require('mongoose');

/**
 * SystemMonitorErrorLog.model.js — persistencia de los errores 5xx que
 * traffic-tracker.middleware.js ya captura en memoria (`erroresRecientes`, tope 20).
 * Ese array es un singleton EN MEMORIA de un solo proceso: no sobrevive un reinicio
 * del contenedor (ocurre cada noche vía auto-update.sh). Este modelo guarda el mismo
 * documento (ts/metodo/path/status) para que el panel pueda mostrar un histórico real
 * además de la vista "en vivo" rápida que ya sirve el array en memoria — mismo
 * criterio de retención (TTL) que SystemMonitorSnapshot.model.js.
 */
const systemMonitorErrorLogSchema = new mongoose.Schema({
  ts:     { type: Date, required: true },
  metodo: { type: String, required: true },
  path:   { type: String, required: true },
  status: { type: Number, required: true },
}, { versionKey: false });

// Mismo criterio que SystemMonitorSnapshot.model.js: un único índice sobre `ts`
// sirve tanto para las consultas por rango (getErroresHistorial) como de TTL —
// declararlo dos veces haría que Mongo lo rechace por "options conflict" al tener
// el mismo key pattern. 30 días, misma retención que el snapshot.
const TTL_SEGUNDOS = 30 * 24 * 60 * 60;
systemMonitorErrorLogSchema.index({ ts: 1 }, { expireAfterSeconds: TTL_SEGUNDOS });

module.exports = mongoose.model('SystemMonitorErrorLog', systemMonitorErrorLogSchema);
