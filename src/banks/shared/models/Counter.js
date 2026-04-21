'use strict';
const mongoose = require('mongoose');

/**
 * Counter — secuencias atómicas para campos auto-incrementales.
 * Usa findOneAndUpdate + $inc para garantizar unicidad incluso
 * bajo concurrencia (elimina la race condition de "find last + 1").
 *
 * Uso:
 *   const { seq } = await Counter.nextSeq('journal_INGRESO');
 */
const counterSchema = new mongoose.Schema(
  {
    _id: { type: String, required: true }, // ej: "journal_INGRESO"
    seq: { type: Number, default: 0 },
  },
  { collection: 'counters' },
);

counterSchema.statics.nextSeq = async function (key) {
  return this.findOneAndUpdate(
    { _id: key },
    { $inc: { seq: 1 } },
    { upsert: true, new: true },
  );
};

// Reserva `count` secuenciales en una sola operación atómica.
// Devuelve el primer número del bloque reservado.
// Ej: si seq estaba en 10 y count=5 → reserva 11-15, retorna 11.
counterSchema.statics.nextBatchSeq = async function (key, count) {
  const doc = await this.findOneAndUpdate(
    { _id: key },
    { $inc: { seq: count } },
    { upsert: true, new: true },
  );
  return doc.seq - count + 1;
};

module.exports = mongoose.model('Counter', counterSchema);
