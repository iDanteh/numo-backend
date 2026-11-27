'use strict';

// Hallazgo real (2026-11-11): credencialesAlertJob.js se require() también desde
// entity.controller.js (sin gate), así que el gate de "una sola vez por
// cluster" de app.js no alcanzaba para evitar que un worker secundario (bajo
// cluster nativo, src/cluster.js) terminara auto-registrando sus 2
// cron.schedule(...) de todos modos. Este archivo cubre ambos casos: worker
// primario (o sin cluster) → se registran los 2; worker secundario → ninguno.
//
// Dependencias reales mockeadas por completo (entity.repository, sat/credenciales,
// email.service, config/env) para que requerir el módulo DOS VECES en el mismo
// archivo (con jest.resetModules() entre medio, para variar el mock de 'cluster')
// no toque modelos reales de Mongo/Postgres — evita errores de "modelo ya
// registrado" al recargar el módulo en el mismo proceso de Jest.

jest.mock('node-cron', () => ({ schedule: jest.fn(() => ({ stop: jest.fn() })) }));
jest.mock('../../config/env', () => ({ alertasSat: { horasAntes: 4 } }));
jest.mock('../repositories/entity.repository', () => ({ findByRfc: jest.fn(), findWithAutoSync: jest.fn() }));
jest.mock('../sat/credenciales', () => ({ listarParaAlertas: jest.fn(), marcarAlertaEnviada: jest.fn(), tieneCredenciales: jest.fn() }));
jest.mock('../../shared/services/email.service', () => ({ enviarCorreo: jest.fn() }));

afterEach(() => {
  jest.dontMock('cluster');
});

test('worker primario (sin cluster bajo Jest, default): se registran los 2 cron.schedule', () => {
  jest.resetModules();
  const cron = require('node-cron');

  require('./credencialesAlertJob');

  expect(cron.schedule).toHaveBeenCalledTimes(2);
});

test('worker secundario (cluster.worker.id !== 1): ningún cron.schedule se registra', () => {
  jest.resetModules();
  jest.doMock('cluster', () => ({ isWorker: true, worker: { id: 2 } }));
  const cron = require('node-cron');

  require('./credencialesAlertJob');

  expect(cron.schedule).not.toHaveBeenCalled();
});
