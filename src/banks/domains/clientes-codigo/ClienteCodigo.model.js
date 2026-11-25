'use strict';

const mongoose = require('mongoose');

// Catálogo de clientes del ERP (Clientes-ids.xlsx), importado periódicamente a mano desde
// el panel de Admins — ver clientes-codigo.service.js#importarCatalogo. NUMO nunca tuvo este
// catálogo sincronizado; `codigo` es el identificador que el ERP le pide a cada cliente que
// escriba en el concepto de sus depósitos bancarios (ver clientes-codigo.service.js para el
// porqué del matching probabilístico contra BankMovement.concepto).
const clienteCodigoSchema = new mongoose.Schema({
  codigo: { type: Number, required: true, unique: true, index: true },
  nombre: { type: String, default: null },
  rfc:    { type: String, default: null },

  // Se actualiza en cada importación donde este código vuelve a aparecer — la importación es
  // acumulativa (nunca borra códigos ausentes de una corrida nueva), este timestamp es la única
  // señal de "qué tan fresco" es el nombre/rfc de un código puntual.
  importadoEn: { type: Date, default: Date.now },
}, {
  timestamps: true,
  collection: 'cliente_codigos',
});

module.exports = mongoose.model('ClienteCodigo', clienteCodigoSchema);
