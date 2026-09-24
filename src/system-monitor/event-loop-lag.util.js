'use strict';

// event-loop-lag.util.js — mide qué tan "atascado" está el event loop de Node, señal
// temprana de que el servidor está degradado aunque Mongo/Postgres respondan bien
// (ej. un job pesado corriendo en el mismo proceso). Usa el histograma nativo de
// perf_hooks, sin dependencias nuevas.
const { monitorEventLoopDelay } = require('perf_hooks');

const histograma = monitorEventLoopDelay({ resolution: 20 });
histograma.enable();

/**
 * Devuelve el lag promedio (ms) medido DESDE LA ÚLTIMA LECTURA (resetea el
 * histograma después de leerlo) — así el valor refleja el estado reciente del
 * event loop, no un promedio desde que arrancó el proceso que un pico único al
 * inicio dejaría inflado para siempre.
 */
function leerYReiniciarLagMs() {
  const lagMs = histograma.mean / 1e6; // nanosegundos → milisegundos
  histograma.reset();
  return Number.isFinite(lagMs) ? lagMs : 0;
}

module.exports = { leerYReiniciarLagMs };
