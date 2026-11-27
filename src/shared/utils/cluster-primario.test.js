'use strict';

// Ver src/shared/utils/cluster-primario.js para el hallazgo real que motiva este
// util (2026-11-11): el gate de "una sola vez por cluster" tiene que vivir DENTRO
// de cada archivo de job con riesgo de doble require-site, no solo en app.js.

describe('esWorkerPrimario', () => {
  afterEach(() => {
    jest.resetModules();
    jest.dontMock('cluster');
  });

  test('sin cluster (isWorker: false) → true', () => {
    jest.doMock('cluster', () => ({ isWorker: false, worker: undefined }));
    const { esWorkerPrimario } = require('./cluster-primario');
    expect(esWorkerPrimario()).toBe(true);
  });

  test('worker primario (isWorker: true, worker.id: 1) → true', () => {
    jest.doMock('cluster', () => ({ isWorker: true, worker: { id: 1 } }));
    const { esWorkerPrimario } = require('./cluster-primario');
    expect(esWorkerPrimario()).toBe(true);
  });

  test('worker secundario (isWorker: true, worker.id: 2) → false', () => {
    jest.doMock('cluster', () => ({ isWorker: true, worker: { id: 2 } }));
    const { esWorkerPrimario } = require('./cluster-primario');
    expect(esWorkerPrimario()).toBe(false);
  });
});
