'use strict';
const mongoose = require('mongoose');

const condicionSchema = new mongoose.Schema({
  campo: {
    type: String,
    required: true,
    enum: ['concepto', 'deposito', 'retiro', 'referenciaNumerica', 'numeroAutorizacion'],
  },
  operador: {
    type: String,
    required: true,
    enum: [
      'contiene', 'no_contiene', 'igual',
      'empieza_con', 'termina_con',
      'mayor_que', 'menor_que', 'mayor_igual', 'menor_igual',
    ],
  },
  valor: { type: String, required: true, trim: true },
}, { _id: false });

const bankRuleSchema = new mongoose.Schema({
  banco:       { type: String, required: true, index: true },
  nombre:      { type: String, required: true, trim: true },
  condiciones: { type: [condicionSchema], default: [] },
  logica:      { type: String, enum: ['Y', 'O'], default: 'Y' },
  orden:       { type: Number, default: 0 },
}, {
  timestamps: true,
  collection: 'bank_rules',
});

bankRuleSchema.index({ banco: 1, orden: 1 });

module.exports = mongoose.model('BankRule', bankRuleSchema);
