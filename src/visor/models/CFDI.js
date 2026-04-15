const mongoose = require('mongoose');

// Sub-schema para Documentos Relacionados del Complemento de Pago
const doctoRelacionadoSchema = new mongoose.Schema({
  idDocumento:      { type: String },
  serie:            { type: String },
  folio:            { type: String },
  monedaDR:         { type: String },
  tipoCambioDR:     { type: Number },
  metodoDePagoDR:   { type: String },
  numParcialidad:   { type: Number },
  impSaldoAnt:      { type: Number },
  impPagado:        { type: Number },
  impSaldoInsoluto: { type: Number },
}, { _id: false });

// Sub-schema para cada Pago dentro del Complemento
const pagoDetalleSchema = new mongoose.Schema({
  fechaPago:          { type: Date },
  formaDePagoP:       { type: String },
  monedaP:            { type: String, default: 'MXN' },
  tipoCambioP:        { type: Number },
  monto:              { type: Number },
  numOperacion:       { type: String },
  doctosRelacionados: [doctoRelacionadoSchema],
}, { _id: false });

// Sub-schema Complemento de Pago (solo en TipoComprobante === 'P')
const complementoPagoSchema = new mongoose.Schema({
  version: { type: String },
  pagos:   [pagoDetalleSchema],
  totales: {
    montoTotalPagos: { type: Number },
  },
}, { _id: false });

// Sub-schema para Emisor/Receptor
const contribuyenteSchema = new mongoose.Schema({
  rfc: { type: String, required: true, uppercase: true, trim: true },
  nombre: { type: String, trim: true },
  regimenFiscal: { type: String },
  domicilioFiscalReceptor: { type: String },
  residenciaFiscal: { type: String },
  numRegIdTrib: { type: String },
  usoCFDI: { type: String },
}, { _id: false });

// Sub-schema para Conceptos
const conceptoSchema = new mongoose.Schema({
  claveProdServ: { type: String },
  noIdentificacion: { type: String },
  cantidad: { type: Number },
  claveUnidad: { type: String },
  unidad: { type: String },
  descripcion: { type: String },
  valorUnitario: { type: Number },
  importe: { type: Number },
  descuento: { type: Number },
  objetoImp: { type: String },
  impuestos: {
    traslados: [{
      base: Number,
      impuesto: String,
      tipoFactor: String,
      tasaOCuota: Number,
      importe: Number,
    }],
    retenciones: [{
      base: Number,
      impuesto: String,
      tipoFactor: String,
      tasaOCuota: Number,
      importe: Number,
    }],
  },
}, { _id: false });

// Sub-schema para Impuestos globales
const impuestosSchema = new mongoose.Schema({
  totalImpuestosTrasladados: { type: Number, default: 0 },
  totalImpuestosRetenidos: { type: Number, default: 0 },
  traslados: [{
    base: Number,
    impuesto: String,
    tipoFactor: String,
    tasaOCuota: Number,
    importe: Number,
  }],
  retenciones: [{
    impuesto: String,
    importe: Number,
  }],
}, { _id: false });

const cfdiSchema = new mongoose.Schema({
  // Identificación
  uuid: {
    type: String,
    required: true,
    uppercase: true,
    trim: true,
  },

  // Origen del CFDI
  source: {
    type: String,
    enum: ['ERP', 'SAT', 'MANUAL', 'RECEPTOR'],
    required: true,
    uppercase: true,
    index: true,
  },

  // Versión CFDI
  version: { type: String, enum: ['3.3', '4.0'], default: '4.0' },

  // Datos principales
  serie: { type: String },
  folio: { type: String },
  fecha: { type: Date, required: true, index: true },
  sello: { type: String },
  formaPago: { type: String },
  noCertificado: { type: String },
  certificado: { type: String },
  condicionesDePago: { type: String },
  subTotal: { type: Number, required: true },
  descuento: { type: Number, default: 0 },
  moneda: { type: String, default: 'MXN' },
  tipoCambio: { type: Number, default: 1 },
  total: { type: Number, required: true, index: true },
  tipoDeComprobante: {
    type: String,
    // null se permite cuando el ERP envía un tipo no reconocido y se desea
    // guardar igualmente la factura (marcada con tieneErrores: true).
    enum: ['I', 'E', 'T', 'N', 'P', null],
    default: null,
    index: true,
  },
  exportacion: { type: String },
  metodoPago: { type: String },
  lugarExpedicion: { type: String },

  // Partes
  emisor: { type: contribuyenteSchema, required: true },
  receptor: { type: contribuyenteSchema, required: true },
  conceptos: [conceptoSchema],
  impuestos: impuestosSchema,

  // CfdiRelacionados
  cfdiRelacionados: [{
    tipoRelacion: String,
    uuids: [String],
  }],

  // Complemento de Pago (solo TipoComprobante === 'P')
  complementoPago: complementoPagoSchema,

  // Complementos (timbre fiscal, pagos, etc.)
  timbreFiscalDigital: {
    uuid: String,
    fechaTimbrado: Date,
    rfcProvCertif: String,
    selloCFD: String,
    noCertificadoSAT: String,
    selloSAT: String,
    version: String,
  },

  // Resultado de la última comparación ejecutada
  lastComparisonStatus: {
    type: String,
    enum: ['match', 'discrepancy', 'warning', 'not_in_sat', 'not_in_erp', 'cancelled', 'pending', 'error', null],
    default: null,
  },
  lastComparisonAt: { type: Date },

  // Estado en SAT
  satStatus: {
    type: String,
    enum: ['Vigente', 'Cancelado', 'No Encontrado', 'Pendiente', 'Error', 'Expresión Inválida', 'Desconocido', null],
    default: null,
    index: true,
  },
  satLastCheck: { type: Date },
  satCancelacionMotivo: { type: String },

  // Estado en el ERP (tal como lo reporta el sistema origen)
  erpStatus: {
    type: String,
    enum: ['Timbrado', 'Cancelado', 'Habilitado', 'Deshabilitado', 'Cancelacion Pendiente', null],
    default: null,
    index: true,
  },

  // XML original
  xmlContent: { type: String, select: false },
  xmlHash: { type: String },

  // ERP metadata
  erpId:     { type: String, index: true },
  erpSystem: { type: String },

  // Hash SHA-256 del archivo XML original (solo se guarda cuando hay archivo físico).
  // NO tiene default: los documentos sin archivo (ERP, Excel) simplemente no tendrán
  // este campo, evitando conflictos en el índice único.
  fileHash: { type: String },

  // Calidad del registro — se activa cuando el ERP envió campos inválidos
  // pero se decidió guardar igualmente la factura.
  uuidGenerado: { type: Boolean, default: false },   // true = UUID sintético (ERP no lo envió)
  tieneErrores: { type: Boolean, default: false },   // true = al menos un campo vino inválido
  errores:      [{ type: String }],                  // lista de mensajes descriptivos

  // Periodo fiscal al que pertenece este CFDI (asignado al subir)
  ejercicio: { type: Number, index: true },
  periodo:   { type: Number, min: 1, max: 12, index: true },

  // Auditoría
  uploadedBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  isActive: { type: Boolean, default: true },
}, {
  timestamps: true,
  collection: 'cfdis',
});

// Índice único compuesto: un UUID puede existir como ERP y como SAT
cfdiSchema.index({ uuid: 1, source: 1 }, { unique: true });

// Índices compuestos
cfdiSchema.index({ 'emisor.rfc': 1, fecha: -1 });
cfdiSchema.index({ 'receptor.rfc': 1, fecha: -1 });
cfdiSchema.index({ source: 1, satStatus: 1 });
cfdiSchema.index({ tipoDeComprobante: 1, fecha: -1 });
cfdiSchema.index({ total: 1, 'emisor.rfc': 1 });

// Índice para deduplicación de facturas ERP sin UUID (serie+folio+emisor.rfc+total)
cfdiSchema.index({ serie: 1, folio: 1, 'emisor.rfc': 1, total: 1, source: 1 });

// Índice único parcial para fileHash: solo indexa documentos donde fileHash
// es un string real. Documentos con fileHash: null (ERP, importaciones sin XML)
// quedan fuera del índice y no causan conflictos entre sí.
// Nota: sparse: true omite campos AUSENTES, pero no omite null explícito.
// partialFilterExpression con $type:'string' es la solución correcta.
cfdiSchema.index(
  { fileHash: 1 },
  { unique: true, partialFilterExpression: { fileHash: { $type: 'string' } } },
);

// Índice de texto para búsqueda
cfdiSchema.index({
  uuid: 'text',
  'emisor.rfc': 'text',
  'emisor.nombre': 'text',
  'receptor.rfc': 'text',
  'receptor.nombre': 'text',
});

module.exports = mongoose.model('CFDI', cfdiSchema);
