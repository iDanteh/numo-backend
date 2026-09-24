'use strict';

const mongoose = require('mongoose');

/**
 * SystemMonitorSnapshot.model.js — RESUMEN persistido cada 5 minutos (ver
 * system-monitor.cron.js), no el detalle de los ring buffers en memoria de
 * traffic-tracker.middleware.js. Esos viven 60 minutos y se resetean al
 * reiniciar el proceso ("tráfico actual"); esto es historial real ("qué pasó
 * ayer/la semana pasada"), un documento chico por tick, no el detalle completo.
 */
const systemMonitorSnapshotSchema = new mongoose.Schema({
  fecha: { type: Date, required: true },

  requestsPorMinuto:         { type: Number, default: 0 },
  requestsEnCurso:           { type: Number, default: 0 },
  erroresUltimoMinuto:       { type: Number, default: 0 },
  tasaErrorPct:              { type: Number, default: 0 },
  tiempoRespuestaPromedioMs: { type: Number, default: null },

  estadoGeneral: { type: String, enum: ['normal', 'degradado', 'caido'], required: true },

  eventLoopLagMs: { type: Number, default: 0 },
  uptimeSegundos: { type: Number, default: 0 },

  memoria: {
    rssMb:       { type: Number, default: 0 },
    heapUsedMb:  { type: Number, default: 0 },
    heapTotalMb: { type: Number, default: 0 },
  },
  // Distinta de `memoria` (esa es solo del proceso Node) — ver el mismo
  // comentario en system-monitor.service.js#construirSnapshot.
  memoriaHost: {
    totalMb:  { type: Number, default: 0 },
    freeMb:   { type: Number, default: 0 },
    usadoPct: { type: Number, default: 0 },
  },
  cpu: {
    load1:  { type: Number, default: 0 },
    load5:  { type: Number, default: 0 },
    load15: { type: Number, default: 0 },
    cores:  { type: Number, default: 0 },
  },
}, { versionKey: false });

// Único índice sobre `fecha`: sirve tanto para las consultas por rango
// (getHistorial) como de TTL — declararlo dos veces (uno normal + uno TTL)
// haría que Mongo lo rechace por "options conflict" al tener el mismo key
// pattern. 30 días es retención razonable para "qué pasó ayer/la semana
// pasada" sin dejar crecer la colección indefinidamente (a 1 doc/5min son
// ~8,640 documentos/mes, liviano, pero no hay razón para guardarlo para
// siempre) — ajustar acá si más adelante se pide más retención.
const TTL_SEGUNDOS = 30 * 24 * 60 * 60;
systemMonitorSnapshotSchema.index({ fecha: 1 }, { expireAfterSeconds: TTL_SEGUNDOS });

module.exports = mongoose.model('SystemMonitorSnapshot', systemMonitorSnapshotSchema);
