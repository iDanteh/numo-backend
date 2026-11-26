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
  consignacionFacturada: Number, // 2026-11-25
  efectivoPorFacturar: Number,
  // 2026-11-26: cobro de una remisión de otra sucursal SIN factura (no suma hasta que se facture).
  cobroOtraSucursalSinFactura: Number,
  miscelaneaEfectivo:  Number,
  cobranzaEfectivo:    Number,
  abonosEfectivo:      Number,
  cobroOtraSucursal:   Number,
  devoluciones:        Number,
  retiroEfectivo:      Number,
  faltante:            Number,
  depositoEfectivo:    Number,
  saldoFavor:          Number,
  // 2026-11-26: BN que Kore aplica al timbrar la global (la factura sale por menos de lo cobrado).
  bonificacionGlobal:  Number,
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
  // Tipo de venta en Kore y saldo de la cuenta después del cobro (2026-11-25/26): el anexo
  // separa la consignación sin factura con saldo pendiente de la que quedó en saldo 0.
  tipoVenta:    String,
  saldoDespues: Number,
  // 2026-11-26: cobro sin factura que ampara una global de un día posterior — el día del cobro
  // va como sin factura (facturaPosterior / facturaPosteriorDia) y en la ficha del día de la
  // global suma con el día en que se cobró (cobradoEl).
  // Importe facturado del cobro = cobrado − BN de la global (2026-11-26); es lo que suma.
  facturado:           Number,
  facturaPosterior:    String,
  facturaPosteriorDia: String,
  cobradoEl:           String,
  // Fecha de creación de la remisión en Kore (la pide /cuentas-pendientes): para revisar
  // después si un cobro de otra sucursal sin factura ya se facturó (2026-11-26).
  remisionCreada:      Date,
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
  // 2026-11-26: el retiro de efectivo (RETEF) es informativo, no resta (fichas anteriores: sí restaba).
  retiroEfectivoInformativo: { type: Boolean, default: false },
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
