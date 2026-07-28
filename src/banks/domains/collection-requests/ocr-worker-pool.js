'use strict';

// ocr-worker-pool.js — pool fijo de worker_threads para correr el motor de OCR
// (ocr-engine.js, vía ocr-worker-entry.js) FUERA del hilo principal.
//
// Por qué (2026-07-28, revisión de Solicitudes de Cobro): todo el backend
// (Bancos, ERP-Kore, Solicitudes de Cobro) corre en UN SOLO proceso Node sin
// cluster/PM2 (ver Dockerfile: `CMD ["node", "src/app.js"]`). El OCR es
// CPU-bound y síncrono — antes de este cambio, un límite de concurrencia
// (OCR_MAX_CONCURRENCY) evitaba que demasiados análisis corrieran a la vez,
// pero SEGUÍAN ejecutándose en el mismo hilo que atiende TODO lo demás,
// compitiendo por el mismo núcleo. Este pool saca ese trabajo a hilos
// separados: mientras un worker procesa un comprobante, el hilo principal
// (y por lo tanto Bancos/ERP-Kore) sigue respondiendo con normalidad.
//
// Tamaño del pool = OCR_MAX_CONCURRENCY (la misma variable que ya gateaba el
// límite anterior) — el pool en sí YA es el límite de concurrencia: si los N
// workers están ocupados, el siguiente trabajo espera en cola (FIFO) en vez de
// crear un worker N+1 — un worker nuevo pagaría de nuevo el costo de inicializar
// el modelo de PaddleOCR (~25MB, carga bajo demanda la primera vez que un
// worker lo usa) y no está acotado por diseño, así que el pool nunca crece.
const { Worker } = require('worker_threads');
const path        = require('path');
const { withTimeout } = require('../../../shared/utils/with-timeout');
const { logger }      = require('../../../shared/utils/logger');

const WORKER_ENTRY = path.join(__dirname, 'ocr-worker-entry.js');
const POOL_SIZE     = parseInt(process.env.OCR_MAX_CONCURRENCY, 10) || 2;
// Generoso a propósito: un comprobante normal tarda 1-8s (ver smoke-tests
// previos), pero un PDF de varias páginas o una imagen grande puede tardar
// más. Este timeout es defensa en profundidad para el ÚNICO hueco que la
// auditoría de 2026-07-13 dejó sin cubrir: detectSkewAngle/adaptiveThreshold
// (bucles síncronos, sin timeout propio, solo mitigados con yields al event
// loop) — antes, un bucle colgado ahí congelaba TODO el servidor; ahora, a lo
// sumo cuelga UN worker, que este timeout detecta y reemplaza sin afectar a
// los demás trabajos que sí están corriendo bien en otros workers del pool.
const JOB_TIMEOUT_MS = parseInt(process.env.OCR_JOB_TIMEOUT_MS, 10) || 60_000;

let _jobIdSeq   = 0;
const _cola     = []; // trabajos esperando un worker libre: { job, resolve, reject }
const _workers  = []; // entries: { worker, busy, currentJobId, resolve, reject }

function _crearWorkerEntry() {
  const worker = new Worker(WORKER_ENTRY);
  const entry  = { worker, busy: false, currentJobId: null, resolve: null, reject: null };

  worker.on('message', (msg) => {
    const { resolve, reject } = entry;
    entry.busy         = false;
    entry.currentJobId = null;
    entry.resolve      = null;
    entry.reject       = null;
    if (!resolve) return; // mensaje inesperado sin trabajo pendiente (ej. llegó tarde tras un timeout) — ignorar
    if (msg.ok) {
      resolve(msg.result);
    } else {
      const err = new Error(msg.error.message);
      if (msg.error.statusCode != null) err.statusCode = msg.error.statusCode;
      if (msg.error.name) err.name = msg.error.name;
      reject(err);
    }
    _despacharSiguiente(entry);
  });

  // Un worker puede morir por una excepción no capturada o un crash nativo
  // (sharp/onnxruntime) — se rechaza el trabajo que tenía pendiente (si
  // había uno) y se reemplaza el worker para que el pool no encoja para
  // siempre.
  worker.on('error', (err) => {
    logger.warn(`[ocrWorkerPool] worker de OCR falló: ${err.message}`);
    if (entry.reject) entry.reject(err);
    _reemplazarWorkerMuerto(entry);
  });

  worker.on('exit', (code) => {
    if (code === 0) return; // salida normal (terminate() explícito, ver timeout de abajo)
    logger.warn(`[ocrWorkerPool] worker de OCR terminó con código ${code} inesperadamente.`);
    if (entry.reject) entry.reject(new Error(`Worker de OCR terminó inesperadamente (código ${code})`));
    _reemplazarWorkerMuerto(entry);
  });

  // No debe mantener vivo el proceso por sí solo (importante para Jest y para
  // que cerrar el servidor no tenga que esperar nada) — mientras SÍ hay un
  // trabajo en curso, la propia promesa pendiente en el hilo principal ya
  // mantiene vivo lo que hace falta.
  worker.unref();
  return entry;
}

function _reemplazarWorkerMuerto(entryMuerto) {
  const idx = _workers.indexOf(entryMuerto);
  if (idx === -1) return; // ya reemplazado (ej. 'error' y 'exit' dispararon los dos)
  _workers[idx] = _crearWorkerEntry();
  _despacharSiguiente(_workers[idx]);
}

function _despacharSiguiente(entry) {
  if (entry.busy) return;
  const siguiente = _cola.shift();
  if (!siguiente) return;
  entry.busy         = true;
  entry.currentJobId = siguiente.job.id;
  entry.resolve      = siguiente.resolve;
  entry.reject       = siguiente.reject;
  entry.worker.postMessage(siguiente.job);
}

function _pool() {
  if (_workers.length === 0) {
    for (let i = 0; i < POOL_SIZE; i++) _workers.push(_crearWorkerEntry());
  }
  return _workers;
}

// Punto de entrada público — mismo contrato que runExtraction/extractReceiptData:
// (imageBuffer, mimeType, label) → objeto con los campos extraídos, o rechaza
// con el mismo tipo de error (statusCode/name preservados) que si hubiera
// corrido en el hilo principal.
function runInPool(imageBuffer, mimeType, label) {
  const id  = ++_jobIdSeq;
  const job = { id, imageBuffer, mimeType, label };

  const promesaTrabajo = new Promise((resolve, reject) => {
    const libre = _pool().find(e => !e.busy);
    if (libre) {
      libre.busy         = true;
      libre.currentJobId = id;
      libre.resolve       = resolve;
      libre.reject        = reject;
      libre.worker.postMessage(job);
    } else {
      logger.info(`[ocrWorkerPool] los ${POOL_SIZE} workers están ocupados — encolando trabajo #${id}, ${_cola.length + 1} en espera.`);
      _cola.push({ job, resolve, reject });
    }
  });

  return withTimeout(promesaTrabajo, JOB_TIMEOUT_MS, `[ocrWorkerPool] trabajo #${id}`, () => {
    // Termina SOLO el worker que tenía asignado ESTE job.id — un timeout de
    // este trabajo no debe afectar a otros trabajos que sigan corriendo bien
    // en otros workers del pool. worker.terminate() mata de verdad el hilo
    // (a diferencia de abandonar la promesa, que no podía hacer nada); el
    // handler 'exit' de arriba se encarga de reemplazarlo.
    const conEsteTrabajo = _workers.find(e => e.currentJobId === id);
    if (conEsteTrabajo) {
      logger.warn(`[ocrWorkerPool] trabajo #${id}: terminando worker colgado tras ${JOB_TIMEOUT_MS}ms.`);
      conEsteTrabajo.worker.terminate();
    }
  });
}

module.exports = { runInPool };
