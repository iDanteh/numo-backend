'use strict';

const mongoose = require('mongoose');

// Match PERSISTIDO entre un BankMovement y un codigo de ClienteCodigo (ver
// clientes-codigo.service.js#actualizarMatches). Antes (2026-11-25, primera versión de este
// indicador) el match se recalculaba al vuelo en cada GET /reporte-matches — el usuario pidió
// que se guarde: lo ya encontrado nunca se recalcula ni se pierde, y cada importación nueva del
// catálogo solo procesa los BankMovement que todavía NO tengan un doc acá (incremental, cubre
// tanto depósitos nuevos como códigos nuevos del catálogo).
//
// Un match persistido es DEFINITIVO: si el catálogo cambia después (ej. un código nuevo crea
// una ambigüedad hipotética con un movimiento YA matcheado), ese movimiento YA matcheado NO se
// re-evalúa ni se borra — `actualizarMatches()` solo mira movimientos sin doc acá.
//
// `nombre` del cliente NO se denormaliza a propósito: se resuelve con un lookup a ClienteCodigo
// por `codigo` al momento de leer (ver clientes-codigo.service.js#listarMatchesPersistidos), así
// un rename en una importación posterior del catálogo se refleja solo, sin tener que reprocesar
// matches viejos. `banco`/`fecha`/`deposito` SÍ se denormalizan (copiados de BankMovement en el
// momento del match) para poder calcular mediana/percentil 90/series por periodo sin un join
// extra en cada consulta de estadísticas (ver bank-indicadores.service.js#getTiempoIdentificacionGlobal).
const bankMovementClienteCodigoMatchSchema = new mongoose.Schema({
  bankMovementId: { type: mongoose.Schema.Types.ObjectId, ref: 'BankMovement', required: true, unique: true },
  codigo:         { type: Number, required: true, index: true },
  confianza:      { type: String, enum: ['alta', 'media', 'baja'], required: true },
  banco:          { type: String, required: true },
  fecha:          { type: Date, required: true },
  deposito:       { type: Number, default: null },
  detectadoEn:    { type: Date, default: Date.now },
}, {
  timestamps: true,
  collection: 'bank_movement_cliente_codigo_matches',
});

bankMovementClienteCodigoMatchSchema.index({ banco: 1, fecha: -1 });

module.exports = mongoose.model('BankMovementClienteCodigoMatch', bankMovementClienteCodigoMatchSchema);
