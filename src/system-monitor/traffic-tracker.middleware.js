'use strict';

// traffic-tracker.middleware.js — captura EN MEMORIA (sin persistencia, es "tráfico
// actual" no historial) cada request que pasa por el servidor. Dos ring buffers de
// tamaño fijo (60 slots cada uno, indexados por epoch % ventana, mismo criterio que un
// rate-limiter de ventana deslizante): uno por segundo (últimos 60s, para la sensación
// de tiempo real) y uno por minuto (últimos 60min, para el gráfico y la tasa de error).
// El tamaño NUNCA crece con el volumen de tráfico — un slot se resetea solo la primera
// vez que le toca turno de nuevo con un `ts` distinto al que tenía guardado.

const { logger } = require('../shared/utils/logger');

const VENTANA_SEGUNDOS = 60;
const VENTANA_MINUTOS = 60;
const MAX_ERRORES_RECIENTES = 20;

function _bucketVacio() {
  return { ts: null, total: 0, c2xx: 0, c3xx: 0, c4xx: 0, c5xx: 0, sumaDuracionMs: 0, muestrasDuracion: 0 };
}

const segundos = Array.from({ length: VENTANA_SEGUNDOS }, _bucketVacio);
const minutos = Array.from({ length: VENTANA_MINUTOS }, _bucketVacio);

let enCurso = 0;
// Más antiguo primero, más reciente al final — se recorta a MAX_ERRORES_RECIENTES.
const erroresRecientes = [];

function _registrar(bucketArr, ventana, tsUnidad, statusCode, duracionMs) {
  const idx = ((tsUnidad % ventana) + ventana) % ventana;
  const slot = bucketArr[idx];
  if (slot.ts !== tsUnidad) {
    slot.ts = tsUnidad;
    slot.total = 0; slot.c2xx = 0; slot.c3xx = 0; slot.c4xx = 0; slot.c5xx = 0;
    slot.sumaDuracionMs = 0; slot.muestrasDuracion = 0;
  }
  slot.total += 1;
  if (statusCode >= 500) slot.c5xx += 1;
  else if (statusCode >= 400) slot.c4xx += 1;
  else if (statusCode >= 300) slot.c3xx += 1;
  else slot.c2xx += 1;
  slot.sumaDuracionMs += duracionMs;
  slot.muestrasDuracion += 1;
}

/**
 * Middleware Express — se monta LO MÁS TEMPRANO posible en app.js (antes incluso
 * de helmet/rate-limit) para que también cuente requests que el rate-limiter
 * corta con 429: esas también son tráfico real y justo las que más importa ver
 * en un pico de caída.
 */
function trafficTracker(req, res, next) {
  enCurso += 1;
  // 'finish' NO se emite si la conexión se aborta/cancela (ej. el frontend corta un
  // /snapshot lento en el siguiente tick de su propio polling) — Node sí emite
  // 'close' en ese caso. Ambos pueden dispararse en el camino feliz (finish y
  // después close), así que se decrementa UNA sola vez con esta bandera.
  let decrementado = false;
  const decrementarEnCurso = () => {
    if (decrementado) return;
    decrementado = true;
    enCurso -= 1;
  };
  const inicioNs = process.hrtime.bigint();

  res.on('finish', () => {
    decrementarEnCurso();
    // Este middleware corre en TODAS las requests, montado antes que
    // helmet/rate-limit — una excepción acá (aunque hoy no se ve ninguna) no debe
    // poder tumbar el proceso entero vía el uncaughtException/shutdown de app.js.
    try {
      const duracionMs = Number(process.hrtime.bigint() - inicioNs) / 1e6;
      const ahoraMs = Date.now();
      const tsSeg = Math.floor(ahoraMs / 1000);
      const tsMin = Math.floor(ahoraMs / 60000);
      _registrar(segundos, VENTANA_SEGUNDOS, tsSeg, res.statusCode, duracionMs);
      _registrar(minutos, VENTANA_MINUTOS, tsMin, res.statusCode, duracionMs);

      if (res.statusCode >= 500) {
        erroresRecientes.push({
          ts: ahoraMs,
          metodo: req.method,
          path: req.originalUrl || req.path,
          status: res.statusCode,
        });
        if (erroresRecientes.length > MAX_ERRORES_RECIENTES) erroresRecientes.shift();
      }
    } catch (err) {
      logger.error('[system-monitor] Error registrando métricas de tráfico (request de negocio no afectada):', err.message);
    }
  });

  res.on('close', decrementarEnCurso);

  next();
}

/**
 * Filtra los slots VÁLIDOS (no obsoletos) de un ring buffer dentro de la ventana
 * [ahoraUnidad - ventana + 1, ahoraUnidad], en orden cronológico ascendente. Un slot
 * con `ts` fuera de ese rango es descarte de una vuelta anterior del buffer, no
 * tráfico real — se omite en vez de devolverse en cero, así el consumidor (pure
 * builder de system-monitor.service.js) decide cómo rellenar huecos.
 */
function _bucketsValidos(bucketArr, ventana, ahoraUnidad) {
  const out = [];
  for (let i = 0; i < ventana; i++) {
    const unidad = ahoraUnidad - (ventana - 1) + i;
    const slot = bucketArr[((unidad % ventana) + ventana) % ventana];
    if (slot.ts === unidad) out.push({ ...slot });
  }
  return out;
}

function getEstadoCrudo(ahoraMs = Date.now()) {
  const tsSeg = Math.floor(ahoraMs / 1000);
  const tsMin = Math.floor(ahoraMs / 60000);
  return {
    ahoraMs,
    enCurso,
    segundos: _bucketsValidos(segundos, VENTANA_SEGUNDOS, tsSeg),
    minutos: _bucketsValidos(minutos, VENTANA_MINUTOS, tsMin),
    erroresRecientes: [...erroresRecientes],
  };
}

// Solo para tests — limpia el estado del módulo (singleton, vive mientras el proceso
// esté arriba) entre casos.
function _resetParaTests() {
  for (let i = 0; i < VENTANA_SEGUNDOS; i++) segundos[i] = _bucketVacio();
  for (let i = 0; i < VENTANA_MINUTOS; i++) minutos[i] = _bucketVacio();
  enCurso = 0;
  erroresRecientes.length = 0;
}

module.exports = { trafficTracker, getEstadoCrudo, _resetParaTests };
