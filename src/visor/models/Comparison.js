const mongoose = require('mongoose');

/**
 * Resultado de comparar un CFDI del ERP contra el registro en SAT.
 * Cada comparación vincula ambas versiones y registra los campos que difieren.
 */
const fieldDiffSchema = new mongoose.Schema({
  field: { type: String, required: true },
  erpValue: { type: mongoose.Schema.Types.Mixed },
  satValue: { type: mongoose.Schema.Types.Mixed },
  severity: {
    type: String,
    enum: ['critical', 'warning', 'info'],
    default: 'warning',
  },
}, { _id: false });

const comparisonSchema = new mongoose.Schema({
  uuid: { type: String, required: true, uppercase: true, index: true },

  // Referencias a los documentos comparados
  erpCfdiId: { type: mongoose.Schema.Types.ObjectId, ref: 'CFDI', index: true },
  satCfdiId: { type: mongoose.Schema.Types.ObjectId, ref: 'CFDI' },

  // Resultado general
  status: {
    type: String,
    enum: [
      'match',                // Sin diferencias
      'match_cancelled',      // Coincide en ambos lados y SAT confirma cancelación
      'discrepancy',          // Diferencias críticas encontradas
      'warning',              // Solo advertencias (sin diferencias críticas)
      'not_in_sat',           // UUID no encontrado en SAT
      'not_in_erp',           // En SAT pero no en ERP
      'cancelled',            // Cancelado en SAT (sin copia ERP activa)
      'cancelled_not_in_erp', // Cancelado en SAT y no existe en ERP
      'pending',              // Pendiente de verificar
      'error',                // Error al consultar SAT
    ],
    required: true,
    index: true,
  },

  // Diferencias detalladas
  differences: [fieldDiffSchema],

  // Métricas rápidas
  totalDifferences: { type: Number, default: 0 },
  criticalCount: { type: Number, default: 0 },
  warningCount: { type: Number, default: 0 },

  // Información de la comparación
  comparedAt: { type: Date, default: Date.now, index: true },
  comparedBy: {
    type: String,
    enum: ['automatic', 'manual', 'scheduled'],
    default: 'automatic',
  },
  triggeredBy: { type: String },   // Auth0 sub del usuario que disparó la comparación

  // Resolución
  resolved: { type: Boolean, default: false },
  resolvedAt: { type: Date },
  resolvedBy: { type: String },    // Auth0 sub del usuario que resolvió
  resolutionNotes: { type: String },

  // Conciliación manual de "no en ERP" — registra la causa seleccionada por el usuario
  conciliacionCausa: {
    type: String,
    enum: [
      'proveedor_sin_registro',       // Factura de proveedor registrada fuera del ERP
      'cancelada_antes_de_registro',  // Cancelada antes de registrarse en ERP
      'periodo_anterior',             // Factura de período anterior no migrada
      'factura_global_sat',           // Factura global / ticket de caja del SAT
      'error_descarga_sat',           // Error en descarga SAT (duplicado o registro incorrecto)
      'tercero_sin_impacto',          // Factura de tercero sin impacto contable en ERP
      'otra',                         // Otra razón
    ],
  },

  // Ejercicio fiscal y periodo derivados de la fecha del CFDI ERP
  ejercicio:          { type: Number, index: true },              // año  (ej. 2024)
  periodo:            { type: Number, min: 1, max: 12, index: true }, // mes 1-12
  tipoDeComprobante:  { type: String, enum: ['I', 'E', 'T', 'N', 'P'], index: true }, // tipo del CFDI ERP

  // Sesión de comparación a la que pertenece (batch)
  sessionId: { type: mongoose.Schema.Types.ObjectId, ref: 'ComparisonSession', index: true, default: null },

  // Indica si la comparación campo a campo usó una copia local del XML SAT
  hasLocalSATCopy: { type: Boolean, default: false },

  // SAT response raw
  satRawResponse: { type: mongoose.Schema.Types.Mixed, select: false },
}, {
  timestamps: true,
  collection: 'comparisons',
});

comparisonSchema.index({ status: 1, comparedAt: -1 });
comparisonSchema.index({ resolved: 1, status: 1 });
comparisonSchema.index({ ejercicio: 1, periodo: 1, status: 1 });
// Cubre el pipeline de discrepanciasCriticas: sort por comparedAt + group por uuid
comparisonSchema.index({ uuid: 1, comparedAt: -1 });

module.exports = mongoose.model('Comparison', comparisonSchema);
