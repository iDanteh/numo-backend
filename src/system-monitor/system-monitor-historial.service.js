'use strict';

const SystemMonitorSnapshot = require('./SystemMonitorSnapshot.model');
const SystemMonitorErrorLog = require('./SystemMonitorErrorLog.model');
const { checkMongoOk } = require('../shared/utils/db-health');
const { logger } = require('../shared/utils/logger');

// Mismo criterio EXACTO que _inicioDiaMx/_finDiaMx (banks/domains/banks/bank.service.js)
// y _medianocheMx (collection-request-indicadores.service.js): México sin horario de
// verano desde 2022, offset fijo UTC-6 — medianoche en México como instante UTC real es
// el mismo día calendario + 6 horas. Se duplica acá (2 líneas) en vez de importarlas de
// bank.service.js para no acoplar este dominio transversal a los internals de banks.
function _inicioDiaMx(fechaStr) {
  return new Date(`${fechaStr}T06:00:00.000Z`);
}
function _finDiaMx(fechaStr) {
  return new Date(_inicioDiaMx(fechaStr).getTime() + 24 * 60 * 60 * 1000 - 1);
}

const CAMPOS_HISTORIAL = '-_id fecha requestsPorMinuto requestsEnCurso erroresUltimoMinuto '
  + 'tasaErrorPct tiempoRespuestaPromedioMs estadoGeneral eventLoopLagMs uptimeSegundos '
  + 'memoria memoriaHost cpu';

/**
 * Guarda un RESUMEN del snapshot actual — llamado por system-monitor.cron.js cada 5
 * minutos, nunca desde la ruta HTTP (esa solo LEE el historial). No guarda el detalle
 * completo de serieUltimaHora/erroresRecientes (eso ya lo expone /snapshot en vivo) —
 * solo el último punto de la serie (minuto más reciente) como `erroresUltimoMinuto`.
 */
async function guardarSnapshot(snapshot) {
  const erroresUltimoMinuto = snapshot.serieUltimaHora[snapshot.serieUltimaHora.length - 1]?.errores ?? 0;

  await SystemMonitorSnapshot.create({
    fecha: new Date(snapshot.generadoEn),
    requestsPorMinuto: snapshot.requestsPorMinuto,
    requestsEnCurso: snapshot.requestsEnCurso,
    erroresUltimoMinuto,
    tasaErrorPct: snapshot.tasaErrorPct,
    tiempoRespuestaPromedioMs: snapshot.tiempoRespuestaPromedioMs,
    estadoGeneral: snapshot.estadoGeneral,
    eventLoopLagMs: snapshot.eventLoopLagMs,
    uptimeSegundos: snapshot.uptimeSegundos,
    memoria: snapshot.memoria,
    memoriaHost: snapshot.memoriaHost,
    cpu: snapshot.cpu,
  });
}

const HORAS_DEFAULT_MS = 24 * 60 * 60 * 1000;

/**
 * Serie de resúmenes en el rango [fechaInicio, fechaFin] (yyyy-mm-dd, día calendario
 * completo en hora de México), orden cronológico ascendente. Sin rango explícito, cae
 * a las últimas 24 horas reales — a diferencia de los paneles de Bancos, este
 * histórico no tiene un "día operativo" establecido al que caerle por default.
 */
async function getHistorial({ fechaInicio, fechaFin } = {}) {
  const gte = fechaInicio ? _inicioDiaMx(fechaInicio) : new Date(Date.now() - HORAS_DEFAULT_MS);
  const lte = fechaFin ? _finDiaMx(fechaFin) : new Date();

  return SystemMonitorSnapshot.find({ fecha: { $gte: gte, $lte: lte } })
    .select(CAMPOS_HISTORIAL)
    .sort({ fecha: 1 })
    .lean();
}

const CAMPOS_ERRORES_HISTORIAL = '-_id ts metodo path status';

/**
 * Guarda UN error (5xx, o de negocio — ver CODIGOS_NEGOCIO_A_REGISTRAR en
 * traffic-tracker.middleware.js) — llamado fire-and-forget desde ese middleware
 * (nunca awaited en el camino de la respuesta). No lleva try/catch propio: el mismo
 * criterio que guardarSnapshot() — quien la llama decide cómo tratar el fallo (acá,
 * el middleware solo loguea con logger.error y sigue, nunca bloquea ni tumba el
 * proceso; un 5xx puede ser justo PORQUE Mongo está caído).
 *
 * Guard de `checkMongoOk()` ANTES de escribir: sin esto, con Mongo caído cada
 * intento queda buffereado por Mongoose hasta `bufferTimeoutMS` (5 min, ver
 * database.mongo.js) antes de rechazar — bajo una ráfaga real de errores, eso
 * amontona escrituras en vuelo compitiendo por el mismo pool de conexiones
 * (maxPoolSize: 20) que necesitan los flujos de negocio para recuperarse. Fail-fast:
 * si Mongo ya está caído, ni se intenta.
 */
async function guardarError({ ts, metodo, path, status }) {
  if (!checkMongoOk()) {
    logger.error(`[system-monitor] Mongo no disponible, se omite persistencia de error: ${metodo} ${path} ${status}`);
    return;
  }
  await SystemMonitorErrorLog.create({ ts, metodo, path, status });
}

const MAX_ERRORES_HISTORIAL = 2000;

/**
 * Serie de errores (5xx + negocio) en el rango [fechaInicio, fechaFin] (yyyy-mm-dd,
 * día calendario completo en hora de México), orden cronológico ascendente. Mismo
 * criterio de default que getHistorial(): sin rango explícito, últimas 24 horas
 * reales.
 *
 * A diferencia de SystemMonitorSnapshot (un documento cada 5 min por cron, acotado
 * por diseño), este log crece 1:1 con el volumen real de errores capturados — sin
 * tope, un rango amplio pedido justo después de un incidente real (o con mucho
 * tráfico de errores de negocio) podría devolver un array arbitrariamente grande.
 * Se corta a los MAX_ERRORES_HISTORIAL más recientes del rango (ordenando desc,
 * limitando, y revirtiendo a asc para el consumidor).
 */
async function getErroresHistorial({ fechaInicio, fechaFin } = {}) {
  const gte = fechaInicio ? _inicioDiaMx(fechaInicio) : new Date(Date.now() - HORAS_DEFAULT_MS);
  const lte = fechaFin ? _finDiaMx(fechaFin) : new Date();

  const docs = await SystemMonitorErrorLog.find({ ts: { $gte: gte, $lte: lte } })
    .select(CAMPOS_ERRORES_HISTORIAL)
    .sort({ ts: -1 })
    .limit(MAX_ERRORES_HISTORIAL)
    .lean();

  return docs.reverse();
}

module.exports = { guardarSnapshot, getHistorial, guardarError, getErroresHistorial };
