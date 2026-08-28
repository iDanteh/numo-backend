'use strict';
const mongoose = require('mongoose');
const { tipoSaldoEspecial } = require('./collection-request-erp-links');

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
 *   cancelada   → el ERP (Kore) canceló la CxC del lado de él antes de que Numo
 *                 revisara la solicitud (ver #cancelarPorErp) — distinto de
 *                 "rechazada": no es una decisión de un revisor de Numo, es un
 *                 evento externo que Kore reporta.
 *
 * Al identificar/rechazar, collection-request.service.js avisa el estatus a Kore
 * (kore-caja.service.js#actualizarEstatusSolicitud, revision-contable) y, solo al
 * aprobar, aplica el cobro en un segundo paso (#aplicarSolicitudOperacion) — ver
 * memoria del proyecto "Solicitudes de Cobro ERP-Kore".
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
      // Saldo(s) a favor / anticipo(s) REALES que cubren esta forma de pago —
      // obligatorio cuando formaPagoDescripcion es "saldo a favor" o "anticipo"
      // (ver tipoSaldoEspecial en collection-request-erp-links.js). Sin esto,
      // Kore no sabe de qué registro específico descontar — puede haber más de
      // uno si el importe se cubre repartido entre varios (mismo patrón que
      // cobroSaldosAFavorConfirmados/cobroAnticiposConfirmados en cobro-panel).
      saldosAplicados: {
        type: [{
          id:    { type: String, required: true, trim: true }, // ErpSaldoFavor.id (Kore)
          monto: { type: Number, required: true },
        }],
        default: [],
      },
      // Movimiento bancario asignado a ESTA forma de pago específica (multi-
      // bank-movement, D2). Clave de asignación = el _id del subdocumento
      // (formaPagoDocId), no formaPagoId — este último NO es único dentro de
      // formasPago[] (dos entradas "transferencia" son legales en Modo 1) y
      // resolverAsignaciones() dependería de una clave ambigua. Antes del
      // backfill (ver banks/scripts/backfill-formaspago-bankmovementid.js)
      // queda null para documentos históricos — movimientosDe() en
      // collection-request-asignaciones.js es el ÚNICO punto que hace fallback
      // al campo raíz bankMovementId (deprecado, ver abajo) en ese caso.
      bankMovementId: {
        type:    mongoose.Schema.Types.ObjectId,
        ref:     'BankMovement',
        default: null,
        index:   true,
      },
      // Depósitos EXTRA para ESTA MISMA forma de pago (2026-08-27) — caso real
      // confirmado contra Kore: una solicitud puede traer 1 sola formaPago pero
      // 2+ comprobantes, porque el cliente pagó ese único monto con 2 depósitos
      // bancarios separados (Kore no lo modela como 2 formasPago). `bankMovementId`
      // arriba sigue siendo el depósito PRIMARIO (compatibilidad con todo el código
      // que ya lo lee como valor único); acá van los adicionales. `montoEfectivo`
      // = el `deposito` REAL de ese BankMovement (nunca un reparto inventado a
      // mano) — ver identificar() en collection-request.service.js. Vacío en el
      // camino feliz (1 depósito), no hace falta backfill para documentos viejos.
      depositosAdicionales: {
        type: [{
          bankMovementId: { type: mongoose.Schema.Types.ObjectId, ref: 'BankMovement', required: true },
          montoEfectivo:  { type: Number, required: true },
        }],
        default: [],
      },
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
  // @deprecated — multi-bank-movement (D1): la fuente de verdad ahora es
  // formasPago[].bankMovementId. Se mantiene y se SIGUE escribiendo (= el
  // primer movimiento asignado) porque bank.service.js:736-751 y los scripts
  // de backfill de folioFiscal todavía lo consultan, y el índice
  // bankMovementId:1 ya existente depende de él. No leer este campo en código
  // nuevo — usar movimientosDe(cr) (collection-request-asignaciones.js), que
  // hace el fallback a este campo en el único lugar donde corresponde
  // (documentos previos al backfill).
  bankMovementId: {
    type:    mongoose.Schema.Types.ObjectId,
    ref:     'BankMovement',
    default: null,
    index:   true,
  },

  // ── Inconsistencia post-Kore (multi-bank-movement, D4) ───────────────────────
  // Kore ya aceptó aplicarSolicitudOperacion (paso irreversible desde Numo)
  // pero el commit de Mongo (conTransaccion: N x setErpIds + cr.save) falló o
  // abortó. Marca la solicitud para revisión manual — NO reintentar el cobro
  // automáticamente (buildErpLinksParaCobro acumula sobre saldos existentes,
  // así que un reintento ciego duplicaría el saldoPagado). Queda null en el
  // camino feliz (inmensa mayoría de los casos).
  inconsistenciaPostKore: {
    type: {
      at:      { type: Date,   default: null },
      mensaje: { type: String, default: null },
      // Grupos (movimientos) cuyo setErpIds/cr.save nunca llegó a comitear.
      movimientosPendientes: {
        type:    [{ type: mongoose.Schema.Types.ObjectId, ref: 'BankMovement' }],
        default: [],
      },
    },
    default: null,
  },

  // ── Estado ────────────────────────────────────────────────────────────────────
  status: {
    type:    String,
    enum:    ['pendiente', 'identificada', 'rechazada', 'cancelada'],
    default: 'pendiente',
    index:   true,
  },
  motivoRechazo: { type: String, trim: true, default: null },

  // ── Resolución (identificar/rechazar) ────────────────────────────────────────
  resueltoPorUserId: { type: String, default: null },
  resueltoPorNombre: { type: String, default: null },
  resueltoAt:        { type: Date,   default: null },

  // ── Cancelación (Kore, ver #cancelarPorErp) ──────────────────────────────────
  // Identidad de quien cancela DEL LADO DE KORE — a diferencia de resueltoPor*
  // (arriba), no siempre es un usuario que Numo pueda resolver por su cuenta;
  // se guarda tal cual lo manda el ERP en el body (sub de Auth0 + nombre) para
  // poder mostrar "Cancelado por el usuario X" en la bandeja sin adivinar.
  canceladoPorUserId: { type: String, default: null },
  canceladoPorNombre: { type: String, default: null },
  canceladoAt:        { type: Date,   default: null },

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

  // Saldo a favor / anticipo: sin saldosAplicados, Kore recibiría anticipos/
  // saldosAFavorAUsar vacíos y aplicaría el pago sin descontar del registro
  // correcto (mismo bug que se corrigió aquí: ver [[project_collection_requests]]).
  for (const f of (this.formasPago ?? [])) {
    const tipo = tipoSaldoEspecial(f);
    if (!tipo) continue;
    if (!f.saldosAplicados?.length) {
      return next(new Error(
        `La forma de pago "${f.formaPagoDescripcion}" requiere saldosAplicados (id + monto de cada saldo a favor/anticipo usado)`,
      ));
    }
    if (f.saldosAplicados.some(s => !s.id || !(s.monto > 0))) {
      return next(new Error(
        `La forma de pago "${f.formaPagoDescripcion}" tiene un saldoAplicado inválido (falta id o monto > 0)`,
      ));
    }
    const suma = Math.round(f.saldosAplicados.reduce((s, x) => s + x.monto, 0) * 100) / 100;
    if (Math.abs(suma - f.importe) > 0.01) {
      return next(new Error(
        `La forma de pago "${f.formaPagoDescripcion}": la suma de saldosAplicados (${suma}) no coincide con el importe (${f.importe})`,
      ));
    }
  }

  next();
});

collectionRequestSchema.index({ status: 1, createdAt: -1 });
collectionRequestSchema.index({ solicitanteUserId: 1, createdAt: -1 });
collectionRequestSchema.index({ 'cxcs.erpId': 1 });
collectionRequestSchema.index({ solicitudIdErp: 1 }, { unique: true, sparse: true });

module.exports = mongoose.model('CollectionRequest', collectionRequestSchema);
