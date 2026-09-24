'use strict';

const SystemMonitorSnapshot = require('./SystemMonitorSnapshot.model');

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

module.exports = { guardarSnapshot, getHistorial };
