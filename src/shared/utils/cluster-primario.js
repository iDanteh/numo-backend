'use strict';

/**
 * shared/utils/cluster-primario.js
 * ─────────────────────────────────────────────────────────────────────────────
 * `esWorkerPrimario()` — true si este proceso debe correr tareas de arranque
 * "una sola vez" (cron jobs, seed, housekeeping) bajo cluster nativo (ver
 * src/cluster.js): solo el worker con id===1. Sin cluster (node src/app.js
 * directo, como en dev local o en tests de Jest, que corren como proceso
 * normal, nunca forkeado), `cluster.isWorker` es false y esto es simplemente
 * true siempre — cero regresión para quien no usa cluster.js.
 *
 * Hallazgo real (2026-11-11): el gate original vivía SOLO en app.js,
 * envolviendo los `require(...)` de los cron jobs dentro de `startServer()`.
 * Eso NO alcanza para archivos que se requieren TAMBIÉN desde otro lado del
 * código que se carga sin condición en los dos workers (controllers/rutas,
 * nunca detrás del gate) — Node solo ejecuta el código de top-level de un
 * módulo en el PRIMER require() de ese proceso, desde CUALQUIER sitio que
 * llegue primero. Por eso el auto-registro de cron (`cron.schedule(...)`
 * top-level, IIFEs de arranque) tiene que gatearse DENTRO de cada archivo de
 * job que tenga este riesgo, no solo en el require-site de app.js. Ver
 * satSyncJob.js y credencialesAlertJob.js para los dos casos reales
 * encontrados — ambos se requieren también desde controllers de rutas.
 */
const cluster = require('cluster');

function esWorkerPrimario() {
  return !cluster.isWorker || cluster.worker.id === 1;
}

module.exports = { esWorkerPrimario };
