const mongoose = require('mongoose');

/**
 * Anomalías o discrepancias detectadas durante la comparación.
 * Incluye tipos predefinidos para facilitar reportes fiscales.
 */
const discrepancySchema = new mongoose.Schema({
  comparisonId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Comparison',
    required: true,
    index: true,
  },
  uuid: { type: String, required: true, uppercase: true, index: true },

  type: {
    type: String,
    enum: [
      'UUID_NOT_FOUND_SAT',       // UUID no existe en SAT
      'AMOUNT_MISMATCH',          // Diferencia en montos
      'RFC_MISMATCH',             // RFC emisor/receptor no coincide
      'DATE_MISMATCH',            // Fecha difiere
      'CANCELLED_IN_SAT',         // Cancelado en SAT, vigente en ERP
      'DUPLICATE_UUID',           // UUID duplicado en ERP
      'MISSING_IN_ERP',           // En SAT pero no en ERP
      'TAX_CALCULATION_ERROR',    // Error en cálculo de impuestos
      'CFDI_VERSION_MISMATCH',    // Versión CFDI diferente
      'SIGNATURE_INVALID',        // Sello/certificado inválido
      'COMPLEMENT_MISSING',       // Complemento ausente o inválido
      'REGIME_MISMATCH',          // Régimen fiscal no coincide
      'OTHER',
    ],
    required: true,
    index: true,
  },

  severity: {
    type: String,
    enum: ['critical', 'warning', 'high', 'medium', 'low', 'info'],
    required: true,
    index: true,
  },

  description: { type: String, required: true },
  detail: { type: mongoose.Schema.Types.Mixed },

  // Valores en conflicto
  erpValue: { type: mongoose.Schema.Types.Mixed },
  satValue: { type: mongoose.Schema.Types.Mixed },

  // RFC involucrado
  rfcEmisor: { type: String, uppercase: true },
  rfcReceptor: { type: String, uppercase: true },

  // Estado del CFDI en el SAT al momento de la comparación
  satStatus: { type: String },

  // Estado de la discrepancia
  status: {
    type: String,
    enum: ['open', 'in_review', 'resolved', 'ignored', 'escalated'],
    default: 'open',
    index: true,
  },

  // Gestión
  assignedTo: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  resolvedAt: { type: Date },
  resolvedBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  resolutionType: {
    type: String,
    enum: ['corrected', 'accepted', 'erp_error', 'sat_error', 'false_positive'],
  },
  notes: [{ type: String }],

  // Ejercicio fiscal y periodo derivados de la fecha del CFDI ERP
  ejercicio:         { type: Number, index: true },
  periodo:           { type: Number, min: 1, max: 12, index: true },
  tipoDeComprobante: { type: String, enum: ['I', 'E', 'T', 'N', 'P'], index: true },

  // Impacto fiscal estimado
  fiscalImpact: {
    amount: { type: Number, default: 0 },
    currency: { type: String, default: 'MXN' },
    taxType: { type: String },
  },
}, {
  timestamps: true,
  collection: 'discrepancies',
});

discrepancySchema.index({ type: 1, severity: 1, status: 1 });
discrepancySchema.index({ rfcEmisor: 1, createdAt: -1 });
discrepancySchema.index({ ejercicio: 1, periodo: 1, status: 1 });

module.exports = mongoose.model('Discrepancy', discrepancySchema);
