'use strict';

const mongoose = require('mongoose');

const bankAuxiliarySchema = new mongoose.Schema(
  {
    referencia: { type: String, required: true, trim: true },
    nombre:     { type: String, required: true, trim: true },
  },
  { timestamps: true },
);

bankAuxiliarySchema.index({ referencia: 1 }, { unique: true });

module.exports = mongoose.model('BankAuxiliary', bankAuxiliarySchema);
