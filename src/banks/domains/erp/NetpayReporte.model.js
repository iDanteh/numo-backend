'use strict';
const mongoose = require('mongoose');

// NetpayReporte — Implementación 1 de "Netpay: carga manual del reporte como fuente de
// verdad" (ver plan). El matching automático (netpay-match.service.js) calcula el neto
// esperado restando `commission` tal cual la devuelve Kore — pero ese campo usa una tasa
// FIJA por tipo de tarjeta (débito 1.59%/crédito 1.77%) en vez de la tasa real negociada
// por almacén (confirmado con evidencia real: hasta 2.36x de sobrecobro en la sucursal
// Ferrocarril). Por eso existe esta colección: persiste el reporte REAL de Netpay
// (Excel "Resumen" + "Ventas Tarjeta Presente") como fuente de verdad de comisión/IVA/neto,
// para conciliar manualmente contra BBVA. Coexiste con NetpayMatch (matching automático) —
// NO lo reemplaza, son dos mecanismos independientes; esta opción es PERMANENTE (sirve de
// respaldo si Kore vuelve a fallar aunque en el futuro se automatice el resto).
//
// A diferencia de NetpayMatch (que persiste solo lo YA RESUELTO, un grupo "pendiente" nunca
// tiene documento propio), acá el documento se crea al CARGAR el Excel — ahí arranca en
// estatus 'pendiente' y persiste el detalle completo parseado (no solo un agregado), porque
// el Excel no se vuelve a tener disponible una vez cargado (ver netpay-reporte-revert.service.js:
// revertir NO borra el documento, solo vuelve a 'pendiente' — evita tener que resubir el
// Excel).
const netpayReporteSchema = new mongoose.Schema({
  // Natural key del depósito (hoja "Resumen", tabla "Depósitos y cargos del periodo") —
  // único índice que bloquea cargar el mismo depósito dos veces.
  claveRastreo: { type: String, required: true },
  cuentaDeposito: { type: String, default: null },
  fechaMovimiento: { type: Date, required: true },

  // Rango del reporte (hoja "Resumen", "Periodo: DD-mmm-YYYY - DD-mmm-YYYY") — informativo,
  // no crítico para el matching (por eso no se valida su ausencia con 400).
  periodoDesde: { type: Date, default: null },
  periodoHasta: { type: Date, default: null },

  // Monto real depositado por Netpay en BBVA para este claveRastreo — fuente de verdad
  // contra la que se buscan candidatos BankMovement (en vez del neto calculado con la
  // comisión errónea de Kore).
  montoDepositoTotal: { type: Number, required: true },

  // Bloque resumen de "Ventas Tarjeta Presente" (hoja del mismo nombre, sección "Resumen de
  // ventas") — trazabilidad de auditoría, independiente del detalle por folio de abajo.
  resumenVentas: {
    montoTransaccionado: { type: Number, default: null },
    comisiones:          { type: Number, default: null },
    iva:                 { type: Number, default: null },
    montoDepositado:     { type: Number, default: null },
  },

  // Detalle COMPLETO parseado (no solo un agregado) — un elemento por fila de la tabla
  // "Ventas pagadas durante periodo". koreCache se llena bajo demanda (consultarFolioKore),
  // nunca al cargar el reporte — es informativo (CxC asociada), NUNCA aplica cobro.
  folios: [{
    referencia:         { type: String, default: null }, // columna "Referencia" — folio para consultar Kore
    // Prefijo (primeros 10 dígitos) de la porción numérica de "Order ID" posterior al
    // guión — ver netpay-reporte-parser.service.js#_extraerTerminalID. Informativo.
    terminalID:         { type: String, default: null },
    storeId:            { type: String, default: null },
    sucursal:           { type: String, default: null },
    nombreEmpresa:      { type: String, default: null },
    fechaTrx:           { type: Date,   default: null },
    horaTrx:            { type: String, default: null },
    montoTrx:           { type: Number, default: null },
    comisionBasePct:    { type: Number, default: null },
    comisionBaseMonto:  { type: Number, default: null },
    ivaComision:        { type: Number, default: null },
    comisionMasIva:     { type: Number, default: null },
    montoDeposito:      { type: Number, default: null },
    banco:              { type: String, default: null },
    tipoTarjeta:        { type: String, default: null },
    codigoAutorizacion: { type: String, default: null },
    orderId:            { type: String, default: null },

    // v2 (netpay-matching-v2): columna "Marca" (AD) — opcional, null si el reporte no la
    // trae (Kore no siempre expone cardTypeName por folio). Usada para cerrar buckets
    // 'pendiente_por_marca' cuando el folio de una marca diferida aparece en este reporte.
    marca: { type: String, default: null },

    // v2: seteado por el upload cuando este folio (mismo orderId, o 'REF:'+referencia si
    // orderId está ausente) ya estaba registrado en NetpayFolioRegistro por OTRO reporte
    // (E11000 al insertar) — ver netpay-reporte.service.js#cargarReporte. La fila queda
    // marcada pero el reporte entero NO se descarta solo por esto (ver
    // motivoDiscrepancia:'folio_duplicado' abajo).
    duplicadoDeReporteId: { type: mongoose.Schema.Types.ObjectId, ref: 'NetpayReporte', default: null },

    // Cache de la consulta puntual a Kore por folio (withAccountInfo=true) — Mixed a
    // propósito: se guarda tal cual viene de Kore (PascalCase, sin remapear), mismo
    // criterio que netpay-transacciones.service.js ("sin remapear los campos crudos de
    // Kore"). Puramente informativo, NUNCA aplica cobro.
    koreCache: {
      consultadoEn: { type: Date, default: null },
      cuenta:       { type: mongoose.Schema.Types.Mixed, default: null },
    },
  }],

  // v2: mismo enum de 6 estados que NetpayMatch.estatusMatch (ver design.md "Data Model":
  // "estatus: same enum. A report never takes pendiente_por_marca" — un reporte SIEMPRE
  // representa un depósito completo, así que en la práctica nunca toma ese valor, pero el
  // enum se mantiene idéntico entre ambas colecciones a propósito). Reemplaza el viejo
  // ['pendiente','confirmado','descartado'] — la migración (scripts/migrate-netpay-v2.js)
  // reescribe cada documento existente antes de que el índice/enum nuevo entre en vigor.
  estatus: {
    type: String,
    enum: [
      'confirmado_automatico',
      'pendiente_por_marca',
      'discrepancia',
      'resuelto_por_reporte',
      'rechazado',
      'resuelto_manual',
    ],
    default: 'discrepancia',
    required: true,
  },

  // Por qué este reporte quedó en discrepancia (o cómo llegó a un revertido) — agrega
  // 'folio_duplicado' (exclusivo de NetpayReporte, ver folios[].duplicadoDeReporteId
  // arriba) al set que comparte con NetpayMatch.motivoDiscrepancia.
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
      'folio_duplicado',
    ],
    default: null,
  },

  // Cómo se llegó a resuelto_por_reporte — 'erp-link' cuando este reporte generó el link
  // NETPAYRPT- directamente; 'corroborado' cuando solo confirmó un bucket ya
  // confirmado_automatico con el mismo monto, sin crear un link nuevo (ver design.md
  // "Report present" flow). null en cualquier otro estatus.
  vinculo: {
    type: String,
    enum: ['erp-link', 'corroborado'],
    default: null,
  },

  // Trazabilidad de confirmación (solo si estatus:'confirmado_automatico'|'resuelto_por_reporte')
  // — mismo patrón que NetpayMatch.confirmadoPor/confirmadoEn.
  movementIdConfirmado: { type: mongoose.Schema.Types.ObjectId, ref: 'BankMovement', default: null },
  confirmadoPor: {
    type: {
      userId: { type: String, default: null },
      nombre: { type: String, default: null },
    },
    default: null,
  },
  confirmadoEn: { type: Date, default: null },

  // Trazabilidad de descarte (solo si estatus:'rechazado').
  descartadoPor: {
    type: {
      userId: { type: String, default: null },
      nombre: { type: String, default: null },
    },
    default: null,
  },
  descartadoEn: { type: Date, default: null },
  // No estaba en el plan original — se agrega porque la ruta POST .../descartar recibe
  // `motivo` en el body (mismo patrón que ErpReversion.motivo) y descartarlo sin
  // persistirlo perdería el único dato humano de por qué se descartó.
  descartadoMotivo: { type: String, default: null },

  // v2: trazabilidad de resolución manual (solo si estatus:'resuelto_manual') — mismo
  // patrón que NetpayMatch.resueltoManualPor/En/justificacion (ver netpay-resolver.service.js).
  resueltoManualPor: {
    type: {
      userId: { type: String, default: null },
      nombre: { type: String, default: null },
    },
    default: null,
  },
  resueltoManualEn: { type: Date, default: null },
  justificacion: { type: String, default: null },

  // v2: marcado por el unlink hook 'NETPAYRPT-' (ver netpay-reporte-revert.service.js) —
  // el reporte vuelve a discrepancia/revertido y nunca se vuelve a subir automáticamente
  // (mismo patrón que NetpayMatch.revertido).
  revertido: {
    type: {
      en:          { type: Date, default: null },
      movementIds: { type: [mongoose.Schema.Types.ObjectId], ref: 'BankMovement', default: [] },
    },
    default: null,
  },

  // v2: valor original preservado por la migración (scripts/migrate-netpay-v2.js) para
  // poder revertir con --revert. null en cualquier documento cargado ya en v2.
  estatusLegacy: { type: String, default: null },

  // v2 (soft-delete): oculta el reporte de las listas/cierres por default sin borrar nada
  // — ver netpay-reporte.service.js#eliminarReporte/restaurarReporte. Nunca revierte un
  // match ya resuelto (confirmado_automatico/resuelto_por_reporte/resuelto_manual) ni
  // revive un rechazado (ver design.md "Report Soft-Delete").
  eliminado: { type: Boolean, default: false },
  eliminadoPor: {
    type: {
      userId: { type: String, default: null },
      nombre: { type: String, default: null },
    },
    default: null,
  },
  eliminadoEn: { type: Date, default: null },
  eliminadoMotivo: { type: String, default: null },

  cargadoPor: {
    type: {
      userId: { type: String, default: null },
      nombre: { type: String, default: null },
    },
    default: null,
  },
  cargadoEn: { type: Date, default: null },
  nombreArchivoOriginal: { type: String, default: null },
}, { timestamps: true, collection: 'netpay_reportes' });

// Bloquea cargar el mismo depósito (mismo claveRastreo) dos veces — última línea de
// defensa a nivel de datos; netpay-reporte.service.js#cargarReporte igual re-valida antes
// de escribir, para devolver un 409 con mensaje legible en vez del error crudo de Mongo.
netpayReporteSchema.index({ claveRastreo: 1 }, { unique: true });

// Fix 3 (2026-09-25, pedido explícito del usuario): ver folios relacionados desde el modal
// ERP de Bancos (erp-modal.component.ts#esErpIdNetpayReporte) sin depender de abrir el panel
// de Netpay ni exportar el Excel — obtenerPorMovimiento() busca por movementIdConfirmado, así
// que este índice evita un COLLSCAN en esa consulta. No único (a diferencia de claveRastreo):
// varios reportes pueden compartir movementIdConfirmado:null (todos los 'pendiente'/'descartado').
netpayReporteSchema.index({ movementIdConfirmado: 1 });

module.exports = mongoose.model('NetpayReporte', netpayReporteSchema);
