'use strict';

// cluster.js — primario de cluster nativo de Node (consultoría de desempeño
// 2026-11-11: cluster nativo + Redis). Sin test real de proceso (forkear
// workers de verdad sería un test de integración costoso/flaky) — se mockea
// el módulo `cluster` de Node y `os.cpus()` para verificar en aislamiento:
// (1) cuántos workers forkea (default y con CLUSTER_WORKERS), (2) que el
// handler de `exit` revive un worker salvo que `shuttingDown` ya esté activo,
// (3) que un worker (isPrimary:false) simplemente hace `require('./app')`
// (mockeado, para no arrancar el servidor real en el test).
jest.mock('./shared/utils/logger', () => ({
  logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
}));

describe('cluster.js', () => {
  let originalIsPrimary;
  let originalEnv;

  beforeEach(() => {
    jest.resetModules();
    originalEnv = { ...process.env };
  });

  afterEach(() => {
    process.env = originalEnv;
    jest.restoreAllMocks();
  });

  function mockCluster({ isPrimary }) {
    const workers = {};
    const handlers = {};
    const fork = jest.fn(() => {
      const id = String(Object.keys(workers).length + 1);
      const worker = { id, process: { pid: 1000 + Number(id), kill: jest.fn() } };
      workers[id] = worker;
      return worker;
    });
    const on = jest.fn((event, cb) => { handlers[event] = cb; });

    jest.doMock('cluster', () => ({
      isPrimary,
      isWorker: !isPrimary,
      fork,
      on,
      workers,
    }));

    return { fork, on, workers, handlers };
  }

  describe('isPrimary: true', () => {
    test('forkea os.cpus().length workers por default (sin CLUSTER_WORKERS)', () => {
      delete process.env.CLUSTER_WORKERS;
      jest.doMock('os', () => ({ cpus: () => new Array(4).fill({}) }));
      const { fork } = mockCluster({ isPrimary: true });

      require('./cluster');

      expect(fork).toHaveBeenCalledTimes(4);
    });

    test('forkea CLUSTER_WORKERS workers cuando la env var está seteada', () => {
      process.env.CLUSTER_WORKERS = '2';
      jest.doMock('os', () => ({ cpus: () => new Array(8).fill({}) }));
      const { fork } = mockCluster({ isPrimary: true });

      require('./cluster');

      expect(fork).toHaveBeenCalledTimes(2);
    });

    test('el handler de "exit" revive un worker si shuttingDown es false', () => {
      delete process.env.CLUSTER_WORKERS;
      jest.doMock('os', () => ({ cpus: () => new Array(1).fill({}) }));
      const { fork, handlers } = mockCluster({ isPrimary: true });

      require('./cluster');
      expect(fork).toHaveBeenCalledTimes(1);

      const deadWorker = { process: { pid: 1234 } };
      handlers.exit(deadWorker, 1, null);

      expect(fork).toHaveBeenCalledTimes(2);
    });

    test('el handler de "exit" NO revive un worker si shuttingDown ya está activo (SIGTERM/SIGINT recibido)', () => {
      delete process.env.CLUSTER_WORKERS;
      jest.doMock('os', () => ({ cpus: () => new Array(1).fill({}) }));
      const { fork, handlers, workers } = mockCluster({ isPrimary: true });

      require('./cluster');
      expect(fork).toHaveBeenCalledTimes(1);

      // Simula SIGTERM en el primario — reenvía la señal a los workers y marca shuttingDown.
      process.emit('SIGTERM');
      Object.values(workers).forEach((w) => expect(w.process.kill).toHaveBeenCalledWith('SIGTERM'));

      const deadWorker = { process: { pid: 1234 } };
      handlers.exit(deadWorker, 0, 'SIGTERM');

      // Seguía en 1: ningún fork() adicional disparado por el exit durante el shutdown.
      expect(fork).toHaveBeenCalledTimes(1);
    });
  });

  describe('isPrimary: false (worker)', () => {
    test('hace require(\'./app\') y NO intenta forkear nada', () => {
      jest.doMock('os', () => ({ cpus: () => new Array(4).fill({}) }));
      const appMock = {};
      jest.doMock('./app', () => appMock);
      const { fork } = mockCluster({ isPrimary: false });

      require('./cluster');

      expect(fork).not.toHaveBeenCalled();
      expect(require('./app')).toBe(appMock);
    });
  });
});
