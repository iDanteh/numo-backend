'use strict';
const mongoose = require('mongoose');

/**
 * CollectionRequest — Solicitud de cobro generada por el ERP (Kore), para una o
 * varias CxC, con el comprobante de transferencia opcional para facilitar la
 * búsqueda manual del depósito bancario correspondiente.
 *
 * Modela los dos únicos modos que hoy soporta aplicar un cobro en Kore/cobro-panel:
 *   Modo 1 — 1 sola CxC  (cxcs.length === 1) + N formas de pago
 *   Modo 2 — N CxC       (cxcs.length  >  1) + 1 sola forma de pago global
 * No existe combinación N CxC × M formas de pago — tampoco en el Angular actual.
 *
 * Ciclo de vida:
 *   pendiente   → creada por el ERP, en espera de revisión (bandeja de Solicitudes de Cobro)
 *   identificada → un usuario de cobranza/contabilidad la vinculó a un BankMovement
 *   rechazada   → no se encontró el depósito o la solicitud es inválida
 *
 * NOTA: esta versión NO aplica el cobro en Kore automáticamente — solo registra
 * la solicitud y su vinculación/rechazo. La aplicación real (aplicarCobroOperacion/
 * aplicarCobroOperacionMultiple) queda para una siguiente iteración, ver memoria
 * "Solicitudes de Cobro ERP-Kore".
 */
const collectionRequestSchema = new mongoose.Schema({

  // ── CxC a cobrar (1 = Modo 1, >1 = Modo 2 — ver validación más abajo) ─────────
  cxcs: {
    type: [{
      erpId:                { type: String, required: true, trim: true }, // Cuenta ID en Kore
      serie:                { type: String, trim: true, default: null },
      folioExterno:         { type: String, trim: true, default: null },
      folioFiscal:          { type: String, trim: true, default: null },
      total:                { type: Number, default: null },              // importe original de la CxC
      tipoPago:             { type: String, trim: true, default: null },  // PUE/PPD
      nombrePersona:        { type: String, trim: true, default: null },
      // Tipo de movimiento en Kore (ej. "VENTA ESPECIAL", "FACTURADO") — necesario
      // para resolver automáticamente el concepto/forma de pago vía el mapa de
      // reglas de negocio cuando el ERP no manda conceptoId directamente.
      nombreTipoMovimiento: { type: String, trim: true, default: null },
      // Porción de esta CxC cubierta por el cobro (= cuentas[].Monto en Kore).
      // Solo es obligatorio cuando hay más de una CxC (Modo 2) — en Modo 1 el
      // monto de la única CxC ya se deduce de la suma de formasPago.
      montoAsignado:        { type: Number, default: null },
    }],
    default: [],
    validate: {
      validator: v => Array.isArray(v) && v.length > 0,
      message:   'Se requiere al menos una CxC',
    },
  },

  // ── Formas de pago del cobro (mismo shape que AsignacionPago/DetalleFormaPago
  // en cobro-panel — necesario para poder aplicar el cobro más adelante).
  // Modo 1 admite N entradas; Modo 2 (cxcs.length > 1) exige exactamente 1. ────
  formasPago: {
    type: [{
      formaPagoId:          { type: String, required: true },
      formaPagoDescripcion: { type: String, required: true, trim: true },
      importe:              { type: Number, required: true },
      // Aut. bancaria — SIEMPRE la asigna Numo con el folio del BankMovement
      // identificado al aplicar el cobro. Nunca se acepta la que mande el ERP
      // en la creación (ver _parseFormasPago en el service, que la ignora).
      referencia:           { type: String, trim: true, default: null },
      bancoKoreId:          { type: String, trim: true, default: null },
      bancoDescripcion:     { type: String, trim: true, default: null },
    }],
    default: [],
    validate: {
      validator: v => Array.isArray(v) && v.length > 0,
      message:   'Se requiere al menos una forma de pago',
    },
  },

  monto:       { type: Number, required: true }, // Σ formasPago[].importe, calculado al crear
  descripcion: { type: String, trim: true, default: null }, // anotación libre del cobro
  conceptoId:  { type: String, trim: true, default: null }, // concepto Kore, si el ERP ya lo resuelve

  // ID propio de la solicitud en Kore — opcional, habilita idempotencia (si el
  // ERP reintenta el mismo POST por timeout, se regresa la solicitud existente
  // en vez de duplicarla, ver collection-request.service.js). A propósito SIN
  // `default: null`: si todas las solicitudes sin este dato guardaran `null`
  // explícito, el índice único de abajo (sparse) fallaría en la segunda — un
  // índice sparse solo excluye documentos donde el campo está AUSENTE, no en null.
  solicitudIdErp: { type: String, trim: true },

  // ── Comprobante de transferencia (opcional, LEGACY) ──────────────────────────
  // Se guardaba como binario en Mongo (no en disco, para no depender de un
  // volumen persistente en el contenedor) — límite bajo para no acercarse al
  // máximo de 16MB por documento de MongoDB. Ya NO se escribe para solicitudes
  // nuevas (ver `comprobantes[]` abajo, subidas a Drive) — se deja el campo tal
  // cual para no perder los comprobantes de documentos viejos ni migrar nada.
  comprobante: {
    data:         { type: Buffer, default: null },
    mimetype:     { type: String, default: null },
    originalName: { type: String, default: null },
  },

  // ── Comprobantes de transferencia (uno o varios, guardados en Drive) ────────
  // Cada uno puede corresponder a un depósito bancario DISTINTO (ej. cliente
  // paga mitad por transferencia y mitad en efectivo, cada uno con su propio
  // comprobante) — por eso es un arreglo y no un objeto único como el legacy.
  // El binario vive en la carpeta compartida de Drive (GOOGLE_DRIVE_COMPROBANTES_FOLDER_ID,
  // ver drive-comprobantes.service.js); aquí solo se guarda la referencia.
  comprobantes: {
    type: [{
      driveFileId:      { type: String, required: true, trim: true },
      driveWebViewLink: { type: String, default: null },
      mimetype:         { type: String, default: null },
      originalName:     { type: String, default: null },
      uploadedAt:       { type: Date, default: Date.now },
    }],
    default: [],
  },

  // ── Solicitante (usuario Numo con rol tienda, identificado por el ERP) ────────
  // Se guarda denormalizado (mismo patrón que identificadoPor en BankMovement) —
  // no hay ref/populate porque los usuarios viven en Postgres, no en Mongo.
  solicitanteUserId: { type: String, required: true, trim: true }, // Auth0 sub del usuario tienda
  solicitanteNombre: { type: String, trim: true, default: null },

  // ── Movimiento bancario vinculado (al identificar la solicitud) ──────────────
  bankMovementId: {
    type:    mongoose.Schema.Types.ObjectId,
    ref:     'BankMovement',
    default: null,
    index:   true,
  },

  // ── Estado ────────────────────────────────────────────────────────────────────
  status: {
    type:    String,
    enum:    ['pendiente', 'identificada', 'rechazada'],
    default: 'pendiente',
    index:   true,
  },
  motivoRechazo: { type: String, trim: true, default: null },

  // ── Resolución (identificar/rechazar) ────────────────────────────────────────
  resueltoPorUserId: { type: String, default: null },
  resueltoPorNombre: { type: String, default: null },
  resueltoAt:        { type: Date,   default: null },

  // ── Aplicación del cobro en Kore (al identificar) ────────────────────────────
  // "identificar" concilia Y aplica el cobro en un solo paso (todo o nada): si
  // Kore rechaza la operación, no se guarda nada de este bloque ni se marca
  // identificada — ver collection-request.service.js. koreOperacionResult
  // guarda la respuesta cruda de Kore para auditoría/diagnóstico.
  cobroAplicado:       { type: Boolean, default: false },
  cobroAplicadoAt:     { type: Date,    default: null },
  koreOperacionResult: { type: mongoose.Schema.Types.Mixed, default: null },

}, { timestamps: true, collection: 'collection_requests' });

// Modo 1 vs Modo 2 — expuesto como campo derivado, no se persiste.
collectionRequestSchema.virtual('modo').get(function () {
  return (this.cxcs?.length ?? 0) > 1 ? 'multi' : 'single';
});
collectionRequestSchema.set('toJSON',   { virtuals: true });
collectionRequestSchema.set('toObject', { virtuals: true });

// Defensa en profundidad — la misma regla ya se valida en el service (create),
// pero se repite aquí por si algún día se inserta/actualiza el documento desde
// otro punto de entrada (script, consola, otro dominio).
collectionRequestSchema.pre('validate', function (next) {
  if ((this.cxcs?.length ?? 0) > 1) {
    if (this.formasPago?.length !== 1) {
      return next(new Error('Una solicitud con varias CxC (Modo 2) solo admite exactamente una forma de pago'));
    }
    if (this.cxcs.some(c => !(c.montoAsignado > 0))) {
      return next(new Error('En Modo 2, cada CxC requiere montoAsignado > 0'));
    }
  }
  next();
});

collectionRequestSchema.index({ status: 1, createdAt: -1 });
collectionRequestSchema.index({ solicitanteUserId: 1, createdAt: -1 });
collectionRequestSchema.index({ 'cxcs.erpId': 1 });
collectionRequestSchema.index({ solicitudIdErp: 1 }, { unique: true, sparse: true });

module.exports = mongoose.model('CollectionRequest', collectionRequestSchema);
