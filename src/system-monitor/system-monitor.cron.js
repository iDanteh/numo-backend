'use strict';

/**
 * system-monitor.cron.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Orquestador COMPARTIDO de las mejoras #1 (alertas por correo) y #3 (histórico
 * persistente) del Panel de Tráfico del Sistema: pide getSnapshot() UNA sola vez
 * cada 5 minutos y reparte el MISMO resultado a ambas — evita pedirle a
 * Mongo/Postgres el estado de salud 2 veces por tick. Cada mejora sigue viviendo
 * en su propio archivo (system-monitor-alerta.job.js / system-monitor-historial.
 * service.js); esto solo coordina CUÁNDO se llaman, no mezcla su lógica.
 */

const cron = require('node-cron');
const { logger } = require('../shared/utils/logger');
const { getSnapshot } = require('./system-monitor.service');
const alertaJob = require('./system-monitor-alerta.job');
const historialSvc = require('./system-monitor-historial.service');

async function tick() {
  const snapshot = await getSnapshot();

  // Promise.allSettled: cada mejora se aísla — si el guardado del histórico
  // falla (ej. Mongo con un problema puntual), no debe impedir que la alerta
  // por correo salga (y viceversa). Comparten el mismo snapshot, no una cadena
  // donde un fallo bloquea al otro.
  const resultados = await Promise.allSettled([
    alertaJob.procesarAlerta(snapshot),
    historialSvc.guardarSnapshot(snapshot),
  ]);

  const NOMBRES = ['alerta', 'historial'];
  resultados.forEach((r, i) => {
    if (r.status === 'rejected') {
      logger.error(`[system-monitor.cron] Error en ${NOMBRES[i]}: ${r.reason?.message}`);
    }
  });
}

cron.schedule('*/5 * * * *', async () => {
  try { await tick(); }
  catch (err) { logger.error(`[system-monitor.cron] Error fatal: ${err.message}`); }
}, { timezone: 'America/Mexico_City' });

module.exports = { tick };
