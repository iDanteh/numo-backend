'use strict';
const mongoose = require('mongoose');

/**
 * Ficha de efectivo GUARDADA (2026-11-05, Reportes › Fichas de efectivo): se
 * guarda al imprimir, con folio propio, banco, denominaciones y el DETALLE de
 * cada venta / cobro / salida que la forma, para no perder el rastro venta →
 * factura o global → ficha. Una caja (serie + día) solo puede estar en UNA
 * ficha vigente; cancelar la ficha libera sus cajas.
 */
const montosSchema = new mongoose.Schema({
  efectivoFacturado:   Number,
  efectivoPorFacturar: Number,
  miscelaneaEfectivo:  Number,
  cobranzaEfectivo:    Number,
  abonosEfectivo:      Number,
  devoluciones:        Number,
  retiroEfectivo:      Number,
  faltante:            Number,
  depositoEfectivo:    Number,
  saldoFavor:          Number,
  totalEntregar:       Number,
}, { _id: false });

const corteSchema = new mongoose.Schema({
  serie: String,
  dia:   String,
}, { _id: false, strict: false });

const detalleSchema = new mongoose.Schema({
  dia:         String,
  serie:       String,
  concepto:    String,
  remision:    String,
  factura:     String,
  facturaUuid: String,
  esGlobal:    Boolean,
  // La venta estaba sin factura al hacer la ficha y se facturó después (Anexo › Actualizar, 2026-11-06).
  facturadaDespues: Boolean,
  cliente:     String,
  cobro:       String,
  origen:      String,
  usuario:     String,
  fecha:       Date,
  importe:     Number,
}, { _id: false });

const denominacionSchema = new mongoose.Schema({
  tipo:     String,
  valor:    Number,
  cantidad: Number,
  importe:  Number,
}, { _id: false });

const fichaEfectivoSchema = new mongoose.Schema({
  /** 5 dígitos = mes (2) + consecutivo del mes (3): 11001, 11002… (2026-11-05). Único por año. */
  folio:          { type: Number, required: true, min: 1, max: 99999 },
  /** Año (México) en que se guardó: el folio solo es único dentro del año. */
  anio:           { type: Number, default: null },
  rfc:            { type: String, required: true },
  centroCostoId:  { type: Number, default: null },
  sucursal:       { type: String, default: null },
  series:         [String],
  fechaInicio:    String,
  fechaFin:       String,
  /** `SERIE|YYYY-MM-DD` de cada caja incluida. */
  cajas:          [String],
  /** Uno o varios bancos (2026-11-05) con lo que se deposita en cada uno. */
  bancos:         [new mongoose.Schema({ banco: String, numeroCuenta: String, importe: Number }, { _id: false })],
  sucursalBanco:  { type: String, default: null },
  denominaciones: [denominacionSchema],
  totalContado:   Number,
  diferencia:     Number,
  totales:        montosSchema,
  cortes:         [corteSchema],
  detalle:        [detalleSchema],
  anexoActualizadoEn: { type: Date, default: null },
  estado:         { type: String, enum: ['vigente', 'cancelada'], default: 'vigente' },
  creadoPor:      { id: String, nombre: String },
  canceladaPor:   { id: String, nombre: String },
  canceladaEn:    { type: Date, default: null },
  motivoCancelacion: { type: String, default: null },
}, { timestamps: true, collection: 'fichas_efectivo' });

fichaEfectivoSchema.index({ anio: 1, folio: 1 }, { unique: true });
fichaEfectivoSchema.index({ rfc: 1, estado: 1, cajas: 1 });
fichaEfectivoSchema.index({ rfc: 1, 'detalle.remision': 1 });
fichaEfectivoSchema.index({ rfc: 1, 'detalle.factura': 1 });
fichaEfectivoSchema.index({ rfc: 1, createdAt: -1 });

module.exports = mongoose.model('FichaEfectivo', fichaEfectivoSchema);
