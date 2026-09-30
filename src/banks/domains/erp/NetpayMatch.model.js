'use strict';
const mongoose = require('mongoose');

// NetpayMatch — v2 (netpay-matching-v2, ver design.md "Data Model"): la unidad de decisión
// ahora es un BUCKET (terminalID, dia, bucket), donde bucket es 'general' o una marca
// diferida (ej. 'AMEX') — ver netpay-evaluacion.service.js. A diferencia de v1 (donde
// "pendiente" nunca tenía documento propio), acá TODA decisión automática se persiste,
// incluida discrepancia — esto es lo que permite auditar por qué un bucket quedó sin
// resolver y habilitar el resolve manual. Los campos legacy (movementIdsConfirmados,
// confirmadoPor/En, descartadoManualmentePor/En) se conservan sin cambios: el matching
// automático 1:1 sigue usando confirmadoPor/En, y rechazado reutiliza
// descartadoManualmentePor/En (ver rechazoMotivo abajo).
const netpayMatchSchema = new mongoose.Schema({
  terminalID: { type: String, required: true },
  almacen:    { type: String, default: null },

  // Medianoche UTC del día agrupado (mismo criterio de "solo fecha, sin hora" que
  // CajaTransferencia.fechaRecepcion truncada) — junto con terminalID y bucket es la clave
  // real de un grupo. No se usa terminalID+almacen porque un almacén puede tener más de una
  // terminal (ver project_netpay_transacciones.md) y el depósito es POR terminal.
  dia: { type: Date, required: true },

  // 'general' (comportamiento pre-existente) o una marca diferida (ej. 'AMEX') — ver
  // bancos.NETPAY_MARCAS_DIFERIDAS en netpay-match.service.js#_marcasDiferidas. Separa
  // el bucket de una marca lenta (Amex liquida 2-3 días después) del resto del día, para
  // que un Amex pendiente no bloquee la confirmación automática de las demás marcas.
  bucket: { type: String, default: 'general' },

  // Snapshot del neto (monto - comisión) calculado al momento de resolver — para
  // trazabilidad/auditoría; el valor vivo se sigue recalculando en cada carga de la
  // bandeja contra Kore, este campo nunca se usa para decidir nada después de guardarse.
  netoEsperado: { type: Number, required: true },

  estatusMatch: {
    type: String,
    enum: [
      'confirmado_automatico',
      'pendiente_por_marca',
      'discrepancia',
      'resuelto_por_reporte',
      'rechazado',
      'resuelto_manual',
    ],
    required: true,
  },

  // Por qué un bucket quedó en discrepancia (o cómo llegó a un revertido) — null en
  // cualquier otro estatusMatch. Ver design.md "Data Model" para el significado de cada
  // valor (sin_candidato, multiples_candidatos, etc.).
  motivoDiscrepancia: {
    type: String,
    enum: [
      'sin_candidato',
      'multiples_candidatos',
      'candidato_en_conflicto',
      'cobertura_parcial',
      'revertido',
      'reporte_revertido',
      'vinculo_huerfano',
    ],
    default: null,
  },

  // Snapshot inmutable de la decisión (ver netpay-evaluacion.service.js) — independiente
  // del reporte/Kore que la originó, para que un soft-delete de NetpayReporte o un
  // recálculo posterior de Kore nunca alteren lo ya decidido.
  snapshot: {
    type: {
      terminalID:           { type: String, default: null },
      dia:                  { type: Date,   default: null },
      montoBruto:           { type: Number, default: null },
      comision:             { type: Number, default: null },
      netoEsperado:         { type: Number, default: null },
      folios: [{
        orderId:    { type: String, default: null },
        referencia: { type: String, default: null },
        marca:      { type: String, default: null },
        monto:      { type: Number, default: null },
        comision:   { type: Number, default: null },

        // Cache de la consulta puntual a Kore por folio (withAccountInfo=true) — mismo
        // shape EXACTO que NetpayReporte.model.js#folios[].koreCache, para el export de la
        // bandeja (netpay-match-export.service.js). Puramente informativo, NUNCA aplica
        // cobro ni decide matching.
        koreCache: {
          consultadoEn: { type: Date, default: null },
          cuenta:       { type: mongoose.Schema.Types.Mixed, default: null },
        },
      }],
      reporteIdOrigen:      { type: mongoose.Schema.Types.ObjectId, ref: 'NetpayReporte', default: null },
      claveRastreoOrigen:   { type: String, default: null },
      montoDepositoReporte: { type: Number, default: null },
    },
    default: null,
  },

  // Trazabilidad de confirmación automática 1:1 (solo si estatusMatch:'confirmado_automatico')
  // — mismo patrón que CajaTransferencia.confirmadoPor/confirmadoEn/movementIdsConfirmados.
  movementIdsConfirmados: { type: [mongoose.Schema.Types.ObjectId], ref: 'BankMovement', default: [] },
  confirmadoPor: {
    type: {
      userId: { type: String, default: null },
      nombre: { type: String, default: null },
    },
    default: null,
  },
  confirmadoEn: { type: Date, default: null },

  // Trazabilidad de resolución manual (solo si estatusMatch:'resuelto_manual') — ver
  // netpay-resolver.service.js. justificacion es obligatoria en el servicio (400 si viene
  // vacía), acá se modela como opcional porque el esquema no es el lugar de esa validación.
  resueltoManualPor: {
    type: {
      userId: { type: String, default: null },
      nombre: { type: String, default: null },
    },
    default: null,
  },
  resueltoManualEn: { type: Date, default: null },
  justificacion: { type: String, default: null },

  // Trazabilidad de rechazo (solo si estatusMatch:'rechazado') — reutiliza
  // descartadoManualmentePor/En (mismo patrón que v1); rechazoMotivo es nuevo (la ruta
  // POST .../rechazar recibe `motivo` en el body, mismo patrón que ErpReversion.motivo).
  descartadoManualmentePor: {
    type: {
      userId: { type: String, default: null },
      nombre: { type: String, default: null },
    },
    default: null,
  },
  descartadoManualmenteEn: { type: Date, default: null },
  rechazoMotivo: { type: String, default: null },

  // Marcado por el unlink hook 'NETPAY-' (ver netpay-match-revert.service.js) cuando el
  // BankMovement vinculado se desvincula — el bucket vuelve a discrepancia/revertido y
  // NUNCA se vuelve a subir automáticamente (ver design.md "Revert (unlink)").
  revertido: {
    type: {
      en:           { type: Date, default: null },
      movementIds:  { type: [mongoose.Schema.Types.ObjectId], ref: 'BankMovement', default: [] },
    },
    default: null,
  },

  // Valor original (v1) preservado por la migración (scripts/migrate-netpay-v2.js) para
  // poder revertir con --revert. null en cualquier documento creado ya en v2.
  estatusLegacy: { type: String, default: null },
}, { timestamps: true, collection: 'netpay_matches' });

// Una clave (terminalID, dia, bucket), un resultado — evita doble-resolución del mismo
// bucket por una condición de carrera (dos admins resolviendo el mismo bucket casi a la
// vez); netpay-evaluacion.service.js/netpay-resolver.service.js igual re-validan antes de
// escribir, este índice es la última línea de defensa a nivel de datos. La migración
// (scripts/migrate-netpay-v2.js --apply) DEBE dropear el índice viejo {terminalID,dia}
// antes de insertar, o el segundo bucket de un mismo día choca con E11000.
netpayMatchSchema.index({ terminalID: 1, dia: 1, bucket: 1 }, { unique: true });

module.exports = mongoose.model('NetpayMatch', netpayMatchSchema);
