const mongoose = require('mongoose');

const periodoFiscalSchema = new mongoose.Schema(
  {
    ejercicio: { type: Number, required: true },
    periodo:   { type: Number, min: 1, max: 12, default: null }, // null = año completo
    label:     { type: String, trim: true },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  },
  { timestamps: true }
);

periodoFiscalSchema.index({ ejercicio: 1, periodo: 1 }, { unique: true });

module.exports = mongoose.model('PeriodoFiscal', periodoFiscalSchema);
