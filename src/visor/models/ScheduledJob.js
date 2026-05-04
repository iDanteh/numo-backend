'use strict';

const mongoose = require('mongoose');

const ScheduledJobSchema = new mongoose.Schema({
  ejercicio:  { type: Number, required: true },
  periodo:    { type: Number, required: true },
  hora:       { type: String, required: true },
  ejecutaEn:  { type: Date,   required: true },
  estado:     { type: String, enum: ['pendiente', 'en_proceso', 'completado', 'error'], default: 'pendiente' },
  fin:        { type: Date },
  error:      { type: String },
}, { timestamps: true });

module.exports = mongoose.model('ScheduledJob', ScheduledJobSchema);
