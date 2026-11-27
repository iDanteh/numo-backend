'use strict';

// Hallazgo real (2026-11-11): satSyncJob.js se require() también desde
// sat.controller.js y schedule.controller.js (sin gate), así que el gate de
// "una sola vez por cluster" de app.js no alcanzaba para evitar que un worker
// secundario (bajo cluster nativo, src/cluster.js) terminara auto-registrando
// sus cron.schedule(...) de todos modos. Este test cubre el caso real: worker
// secundario (id !== 1) → cero cron.schedule(...) al requerir el módulo.
//
// Archivo separado de los demás satSyncJob.*.test.js a propósito (no tocarlos)
// porque acá 'cluster' se mockea como worker secundario para TODO el archivo —
// mezclarlo con los tests existentes (que corren como proceso normal,
// isWorker: false → esWorkerPrimario() siempre true) cambiaría su comportamiento.

jest.mock('cluster', () => ({ isWorker: true, worker: { id: 2 } }));
jest.mock('node-cron', () => ({ schedule: jest.fn(() => ({ stop: jest.fn() })) }));
jest.mock('../models/AppConfig', () => ({ find: () => ({ lean: async () => [] }) }));
jest.mock('../controllers/schedule.controller', () => ({ restaurarProgramados: jest.fn() }));
jest.mock('../models/SatDescargaLog', () => ({ create: jest.fn(async () => ({ _id: 'log1' })), updateOne: jest.fn(async () => ({})) }));
jest.mock('../models/SatJobCheckpoint', () => ({
  findOne: jest.fn(), find: jest.fn(() => ({ lean: async () => [] })), findById: jest.fn(),
  findOneAndUpdate: jest.fn(), updateOne: jest.fn(async () => ({})), updateMany: jest.fn(async () => ({})),
}));
jest.mock('../sat/download', () => ({
  solicitar: jest.fn(),
  verificar: jest.fn(),
  descargarPaquete: jest.fn(async () => []),
  descargarPaqueteMetadata: jest.fn(async () => []),
}));
jest.mock('../sat/credenciales', () => ({ obtener: jest.fn(async () => ({ cer: 1 })), tieneCredenciales: jest.fn() }));
jest.mock('../sat/rateLimiter', () => ({ puedeIniciar: jest.fn(async () => ({ puede: false })), registrarInicio: jest.fn(), registrarFin: jest.fn() }));

test('worker secundario (cluster.worker.id !== 1): ningún cron.schedule se registra al requerir satSyncJob', async () => {
  jest.resetModules();
  const cron = require('node-cron');

  require('./satSyncJob');

  // La IIFE de arranque es async (lee AppConfig y llama reprogramarJobs) — dejarla
  // terminar antes de assertar, aunque acá el early-return de esWorkerPrimario()
  // debería cortarla antes de llegar a ese require.
  await new Promise((resolve) => process.nextTick(resolve));

  expect(cron.schedule).not.toHaveBeenCalled();
});
