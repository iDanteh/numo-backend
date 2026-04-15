'use strict';
const mongoose = require('mongoose');

/**
 * CollectionRequest — Solicitud de cobro vinculada a un comprobante de transferencia.
 *
 * Ciclo de vida:
 *   pendiente  → usuario crea la solicitud (sin imagen analizada todavía)
 *   por_confirmar → imagen analizada, movimiento candidato encontrado, espera confirmación
 *   confirmado → usuario confirmó el movimiento bancario correcto
 *   rechazado  → transferencia no encontrada, cancelada o errónea
 */
const collectionRequestSchema = new mongoose.Schema({

  // ── Datos del cobro ─────────────────────────────────────────────────────────
  clienteNombre: { type: String, trim: true, default: null },
  clienteRFC:    { type: String, trim: true, uppercase: true, default: null },
  monto:         { type: Number, default: null },
  concepto:      { type: String, trim: true, default: null },

  // ── Datos extraídos del comprobante de transferencia ───────────────────────
  // Se guardan para auditoría y para re-buscar si el usuario rechaza el candidato.
  comprobante: {
    montoExtraido:            { type: Number, default: null },
    fechaExtraida:            { type: Date,   default: null },
    horaExtraida:             { type: String, default: null },
    claveRastreo:             { type: String, trim: true, default: null },
    referencia:               { type: String, trim: true, default: null },
    bancoOrigen:              { type: String, trim: true, default: null },
    bancoDestino:             { type: String, trim: true, default: null },
    cuentaOrigenUltimos4:     { type: String, trim: true, default: null },
    cuentaDestinoUltimos4:    { type: String, trim: true, default: null },
    titularOrigen:            { type: String, trim: true, default: null },
    titularDestino:           { type: String, trim: true, default: null },
    conceptoExtraido:         { type: String, trim: true, default: null },
    confianzaExtraccion:      { type: Number, default: 0 },  // 0-100
  },

  // ── Movimiento bancario confirmado ─────────────────────────────────────────
  bankMovementId: {
    type:    mongoose.Schema.Types.ObjectId,
    ref:     'BankMovement',
    default: null,
    index:   true,
  },

  // ── CFDIs relacionados (opcional: el contador los vincula manualmente) ──────
  cfdiIds: [{ type: mongoose.Schema.Types.ObjectId, ref: 'CFDI' }],

  // ── Estado ──────────────────────────────────────────────────────────────────
  status: {
    type:    String,
    enum:    ['pendiente', 'por_confirmar', 'confirmado', 'rechazado'],
    default: 'pendiente',
    index:   true,
  },

  notas: { type: String, trim: true, default: null },

  // ── Auditoría ────────────────────────────────────────────────────────────────
  creadoPor:     { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null },
  confirmadoPor: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null },
  confirmadoAt:  { type: Date, default: null },

}, { timestamps: true, collection: 'collection_requests' });

collectionRequestSchema.index({ status: 1, createdAt: -1 });
collectionRequestSchema.index({ 'comprobante.claveRastreo': 1 }, { sparse: true });

module.exports = mongoose.model('CollectionRequest', collectionRequestSchema);
