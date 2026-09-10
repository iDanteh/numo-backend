'use strict';
const mongoose = require('mongoose');

// AnticipoGenerado — trazabilidad de anticipos que Kore genera AUTOMÁTICAMENTE
// cuando un depósito bancario sobrepaga una CxC cobrada vía Solicitudes de
// Cobro (proceso asíncrono, del lado de Kore). NO es un erpLink más sobre el
// BankMovement: ese depósito ya está 100% contabilizado contra la CxC original
// (aplicarSolicitudOperacion ya lo dejó 'identificado') — agregarle otro link
// duplicaría su saldoErp. Este modelo es puramente de AUDITORÍA/trazabilidad,
// para que contabilidad sepa "este anticipo salió de este depósito".
//
// Correlación: Kore no conoce el `solicitudIdErp` de Numo (es un id nuestro,
// nunca enhebrado por su pipeline asíncrono de anticipos) — en cambio, SIEMPRE
// tiene a mano el `id` de la CxC que originó el excedente (confirmado con un
// caso real de Kore: el campo `anotacion` del anticipo ya menciona la venta de
// origen en texto libre). Por eso el webhook nuevo exige `origenCuentaId` (el
// `id` de esa CxC en Kore) — matchea 1:1 contra `CollectionRequest.cxcs[].erpId`
// (ya indexado). `anotacion` se guarda tal cual, SOLO para mostrar/auditoría —
// nunca se parsea para correlacionar (es una oración pensada para humanos, un
// cambio de wording la rompería en silencio).
const anticipoGeneradoSchema = new mongoose.Schema({
  // Cuenta.id del anticipo en Kore — idempotencia (Kore puede reintentar el
  // mismo aviso por timeout de red, mismo criterio que solicitudIdErp en
  // CollectionRequest).
  anticipoIdErp: { type: String, required: true, trim: true, unique: true, index: true },

  // Folio interno vs. externo del anticipo — mismo patrón serie/folio vs.
  // serieExterna/folioExterno que ya usa CxCSolicitud (cxcs[] en
  // CollectionRequest.model.js). El externo (ej. "OPA-00362") es el que
  // contabilidad reconoce y el que después aparece en
  // GET /erp/cobros/saldos-favor/:personaId?tipo=anticipo cuando se vaya a usar
  // como forma de pago en un cobro futuro.
  anticipoSerie:        { type: String, trim: true, default: null },
  anticipoFolio:         { type: String, trim: true, default: null },
  anticipoSerieExterna: { type: String, trim: true, default: null },
  anticipoFolioExterno: { type: String, trim: true, default: null },

  monto:             { type: Number, required: true },
  fechaCreacionKore: { type: Date,   default: null },

  personaId:     { type: String, trim: true, default: null },
  nombrePersona: { type: String, trim: true, default: null },

  // Texto libre de Kore ("Anticipo generado por el excedente cobrado en la
  // venta A0-260900171") — solo para mostrar en el historial, nunca para
  // correlacionar (ver nota de arriba).
  anotacion: { type: String, trim: true, default: null },

  // El dato real de correlación — `id` de la CxC (cuentas[].id en Kore) que
  // generó el excedente. Requerido por contrato del webhook.
  origenCuentaIdErp: { type: String, required: true, trim: true, index: true },

  // Resultado de la correlación automática (ver anticipo-generado.service.js).
  // null cuando no se pudo resolver sin ambigüedad — no se adivina, un humano
  // lo revisa desde el historial.
  solicitudCobroId: { type: mongoose.Schema.Types.ObjectId, ref: 'CollectionRequest', default: null },
  bankMovementIds:  { type: [{ type: mongoose.Schema.Types.ObjectId, ref: 'BankMovement' }], default: [] },

  correlacionAutomatica: { type: Boolean, default: false },
  // Ej. "No se encontró ninguna CollectionRequest identificada con cxcs.erpId=X"
  // o "Ambiguo: 2 solicitudes candidatas resueltas antes de la fecha del anticipo".
  motivoSinCorrelacion: { type: String, trim: true, default: null },

  recibidoAt: { type: Date, default: Date.now },
}, { timestamps: true, collection: 'anticipos_generados' });

anticipoGeneradoSchema.index({ createdAt: -1 });
anticipoGeneradoSchema.index({ correlacionAutomatica: 1, createdAt: -1 });

module.exports = mongoose.model('AnticipoGenerado', anticipoGeneradoSchema);
