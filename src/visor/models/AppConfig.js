'use strict';

const mongoose = require('mongoose');

/**
 * AppConfig — configuración global de la aplicación.
 * Almacena pares clave/valor para configuraciones dinámicas como
 * horarios de jobs, flags de feature, etc.
 */
const AppConfigSchema = new mongoose.Schema({
  key:   { type: String, required: true, unique: true },
  value: { type: mongoose.Schema.Types.Mixed, required: true },
}, {
  timestamps: true,
  collection: 'appconfigs',
});

module.exports = mongoose.model('AppConfig', AppConfigSchema);
