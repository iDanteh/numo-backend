'use strict';

/**
 * cluster.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Primario de cluster nativo de Node — forkea N workers que corren app.js
 * cada uno. Reemplaza a `node src/app.js` como entrypoint SOLO donde se
 * active explícitamente (ver docker-compose.yml del servidor de test:
 * `command: ["node", "src/cluster.js"]` — producción y el Dockerfile por
 * default siguen arrancando con `node src/app.js`, un solo proceso, hasta que
 * se decida activar clustering ahí también).
 *
 * CLUSTER_WORKERS (env, opcional): cantidad de workers a forkear. Default:
 * todos los vCPU visibles (os.cpus().length) — en un servidor compartido o
 * con cgroups limitando CPU, fijalo explícito para no sobre-suscribir.
 *
 * app.js (cada worker) usa `cluster.isWorker && cluster.worker.id === 1` para
 * decidir si le toca correr los cron jobs/seed de arranque una sola vez — ver
 * el gate ES_WORKER_PRIMARIO en app.js.
 */
require('dotenv').config();

const cluster = require('cluster');
const os = require('os');
const { logger } = require('./shared/utils/logger');

if (cluster.isPrimary) {
  const numWorkers = parseInt(process.env.CLUSTER_WORKERS, 10) || os.cpus().length;
  logger.info(`[cluster] Primario ${process.pid} — forkeando ${numWorkers} worker(s)`);

  for (let i = 0; i < numWorkers; i += 1) {
    cluster.fork();
  }

  let shuttingDown = false;

  cluster.on('exit', (worker, code, signal) => {
    if (shuttingDown) {
      logger.info(`[cluster] Worker ${worker.process.pid} salió durante el shutdown — no se revive`);
      return;
    }
    logger.error(`[cluster] Worker ${worker.process.pid} murió inesperadamente (código=${code}, señal=${signal}) — reviviendo`);
    cluster.fork();
  });

  const reenviarYSalir = (signal) => {
    logger.warn(`[cluster] ${signal} recibido en el primario — reenviando a los workers`);
    shuttingDown = true;
    for (const id of Object.keys(cluster.workers)) {
      cluster.workers[id].process.kill(signal);
    }
  };
  process.on('SIGTERM', () => reenviarYSalir('SIGTERM'));
  process.on('SIGINT',  () => reenviarYSalir('SIGINT'));
} else {
  require('./app');
}
