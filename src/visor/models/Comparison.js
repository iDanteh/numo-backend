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
      'match',        // Sin diferencias
      'discrepancy',  // Diferencias críticas encontradas
      'warning',      // Solo advertencias (sin diferencias críticas)
      'not_in_sat',   // UUID no encontrado en SAT
      'not_in_erp',   // En SAT pero no en ERP
      'cancelled',    // Cancelado en SAT
      'pending',      // Pendiente de verificar
      'error',        // Error al consultar SAT
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
  triggeredBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },

  // Resolución
  resolved: { type: Boolean, default: false },
  resolvedAt: { type: Date },
  resolvedBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  resolutionNotes: { type: String },

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

module.exports = mongoose.model('Comparison', comparisonSchema);
