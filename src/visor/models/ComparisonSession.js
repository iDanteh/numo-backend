const mongoose = require('mongoose');

const comparisonSessionSchema = new mongoose.Schema({
  name: { type: String, required: true },

  triggeredBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },

  status: {
    type: String,
    enum: ['running', 'completed', 'failed'],
    default: 'running',
    index: true,
  },

  totalCFDIs:  { type: Number, default: 0 },
  processed:   { type: Number, default: 0 },
  failedCount: { type: Number, default: 0 },

  results: {
    match:       { type: Number, default: 0 },
    discrepancy: { type: Number, default: 0 },
    not_in_sat:  { type: Number, default: 0 },
    not_in_erp:  { type: Number, default: 0 },
    cancelled:   { type: Number, default: 0 },
    error:       { type: Number, default: 0 },
  },

  startedAt:   { type: Date, default: Date.now, index: true },
  completedAt: { type: Date },

  filters: { type: mongoose.Schema.Types.Mixed },
}, {
  timestamps: true,
  collection: 'comparison_sessions',
});

module.exports = mongoose.model('ComparisonSession', comparisonSessionSchema);
