const mongoose = require('mongoose');

/**
 * BankConfig — configuración por banco: cuenta contable, número de cuenta.
 * Un documento por banco (upsert por campo `banco`).
 */
const bankConfigSchema = new mongoose.Schema({
  banco: {
    type:     String,
    enum:     ['Banamex', 'BBVA', 'Santander', 'Azteca'],
    required: true,
    unique:   true,
    index:    true,
  },
  cuentaContable: { type: String, trim: true, default: null },
  numeroCuenta:   { type: String, trim: true, default: null },
}, {
  timestamps: true,
  collection: 'bank_configs',
});

module.exports = mongoose.model('BankConfig', bankConfigSchema);
