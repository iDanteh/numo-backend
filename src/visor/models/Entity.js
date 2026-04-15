const mongoose = require('mongoose');
const bcrypt = require('bcryptjs');

// Entidad fiscal (empresa/persona) registrada en el sistema
const entitySchema = new mongoose.Schema({
  rfc: { type: String, required: true, unique: true, uppercase: true, trim: true },
  nombre: { type: String, required: true, trim: true },
  regimenFiscal: { type: String },
  domicilioFiscal: {
    calle: String,
    noExterior: String,
    noInterior: String,
    colonia: String,
    municipio: String,
    estado: String,
    pais: { type: String, default: 'MEX' },
    codigoPostal: String,
  },
  tipo: {
    type: String,
    enum: ['moral', 'fisica'],
    required: true,
  },
  isOwn: { type: Boolean, default: false }, // Empresa propia del sistema

  // FIEL para descarga masiva SAT
  fiel: {
    cerPath: { type: String, select: false },
    keyPath: { type: String, select: false },
    keyPasswordEncrypted: { type: String, select: false },
    validFrom: Date,
    validTo: Date,
    isActive: { type: Boolean, default: false },
  },

  // Configuración de sincronización
  syncConfig: {
    autoSync: { type: Boolean, default: false },
    syncFrequency: { type: String, enum: ['daily', 'weekly', 'monthly'], default: 'daily' },
    lastSync: Date,
    nextSync: Date,
    syncEmitidos: { type: Boolean, default: true },
    syncRecibidos: { type: Boolean, default: true },
  },

  isActive: { type: Boolean, default: true },
  notes: { type: String },
}, {
  timestamps: true,
  collection: 'entities',
});

module.exports = mongoose.model('Entity', entitySchema);
