'use strict';
const mongoose = require('mongoose');

/**
 * Última "Estadística por facturar" del MES EN CURSO por empresa y sucursal
 * (2026-10-30, pedido del usuario: "no debe calcularse así, ya debe verse en
 * cuanto se entra al módulo"). Se recalcula sola cada hora (cron) y al
 * abrirla si ya es vieja; la pantalla muestra al instante la última guardada.
 * `centroCostoId` null = Todas las sucursales.
 */
const estadisticaFotoSchema = new mongoose.Schema({
  rfc:           { type: String, required: true },
  centroCostoId: { type: Number, default: null },
  fechaInicio:   { type: String, default: null },
  fechaFin:      { type: String, default: null },
  resultado:     { type: mongoose.Schema.Types.Mixed, default: null },
  calculadoEn:   { type: Date, default: null },
  error:         { type: String, default: null },
  // Última vez que alguien la abrió: el cron solo mantiene al día las que se usan.
  consultadaEn:  { type: Date, default: null },
}, { timestamps: true, collection: 'estadisticas_por_facturar' });

estadisticaFotoSchema.index({ rfc: 1, centroCostoId: 1 }, { unique: true });

module.exports = mongoose.model('EstadisticaPorFacturarFoto', estadisticaFotoSchema);
