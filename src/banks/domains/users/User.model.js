'use strict';

const mongoose = require('mongoose');

const userSchema = new mongoose.Schema({
  auth0Sub: { type: String, required: true, unique: true },
  nombre:   { type: String, default: '' },
  email:    { type: String, default: '' },
  role:     {
    type:    String,
    enum:    ['admin', 'contabilidad', 'cobranza', 'tienda'],
    default: 'tienda',
  },
  isActive:  { type: Boolean, default: true },
  lastLogin: { type: Date, default: null },
}, {
  timestamps: true,
  collection: 'users',
});

module.exports = mongoose.model('User', userSchema);
