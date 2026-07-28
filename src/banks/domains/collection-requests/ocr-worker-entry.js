'use strict';

// ocr-worker-entry.js — código que corre DENTRO de cada hilo del pool (ver
// ocr-worker-pool.js, que crea las instancias de Worker apuntando a este
// archivo). Un solo trabajo a la vez por hilo — el pool nunca manda un
// segundo mensaje a un worker antes de recibir la respuesta del anterior
// (ver `busy` en ocr-worker-pool.js), así que no hace falta lógica de cola
// aquí adentro.
const { parentPort } = require('worker_threads');
const { runExtraction } = require('./ocr-engine');

parentPort.on('message', async (job) => {
  const { id, imageBuffer, mimeType, label } = job;
  try {
    const result = await runExtraction(imageBuffer, mimeType, label);
    parentPort.postMessage({ id, ok: true, result });
  } catch (err) {
    // Un Error no sobrevive structured clone con sus propiedades custom (ej.
    // err.statusCode de UnprocessableError) — se serializan a mano los campos
    // que ocr-worker-pool.js necesita para reconstruir un Error equivalente
    // del lado del hilo principal (mismo mensaje, mismo statusCode/name).
    parentPort.postMessage({
      id,
      ok: false,
      error: { message: err.message, statusCode: err.statusCode ?? null, name: err.name },
    });
  }
});
