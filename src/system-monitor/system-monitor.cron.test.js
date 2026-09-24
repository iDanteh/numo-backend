'use strict';

// system-monitor.cron.js llama cron.schedule() al cargar el módulo — se mockea
// node-cron para capturar el callback sin que un timer real corra durante los
// tests, y se invoca manualmente para probar el manejo de errores.
jest.mock('node-cron', () => ({ schedule: jest.fn() }));
jest.mock('../shared/utils/logger', () => ({ logger: { error: jest.fn() } }));
jest.mock('./system-monitor.service', () => ({ getSnapshot: jest.fn() }));
jest.mock('./system-monitor-alerta.job', () => ({ procesarAlerta: jest.fn() }));
jest.mock('./system-monitor-historial.service', () => ({ guardarSnapshot: jest.fn() }));

const cron = require('node-cron');
const { logger } = require('../shared/utils/logger');
const { getSnapshot } = require('./system-monitor.service');
const alertaJob = require('./system-monitor-alerta.job');
const historialSvc = require('./system-monitor-historial.service');
const { tick } = require('./system-monitor.cron');

// cron.schedule() se llama UNA sola vez, como efecto de CARGA del módulo — hay
// que capturar esa llamada ACÁ, antes de que el primer jest.clearAllMocks() (en
// el beforeEach de abajo) la borre del historial del mock.
const [cronExpresion, callbackRegistrado, cronOpciones] = cron.schedule.mock.calls[0];

describe('system-monitor.cron', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('se registra con cron.schedule cada 5 minutos, hora de México', () => {
    expect(cronExpresion).toBe('*/5 * * * *');
    expect(typeof callbackRegistrado).toBe('function');
    expect(cronOpciones).toEqual({ timezone: 'America/Mexico_City' });
  });

  test('tick(): pide getSnapshot UNA sola vez y reparte el MISMO snapshot a alerta e historial', async () => {
    const snap = { estadoGeneral: 'normal' };
    getSnapshot.mockResolvedValue(snap);
    alertaJob.procesarAlerta.mockResolvedValue(undefined);
    historialSvc.guardarSnapshot.mockResolvedValue(undefined);

    await tick();

    expect(getSnapshot).toHaveBeenCalledTimes(1);
    expect(alertaJob.procesarAlerta).toHaveBeenCalledWith(snap);
    expect(historialSvc.guardarSnapshot).toHaveBeenCalledWith(snap);
  });

  test('si procesarAlerta falla, guardarSnapshot IGUAL se ejecuta (aislados) y tick() no propaga', async () => {
    getSnapshot.mockResolvedValue({ estadoGeneral: 'normal' });
    alertaJob.procesarAlerta.mockRejectedValue(new Error('SMTP caído'));
    historialSvc.guardarSnapshot.mockResolvedValue(undefined);

    await expect(tick()).resolves.not.toThrow();

    expect(historialSvc.guardarSnapshot).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('alerta'));
  });

  test('si guardarSnapshot falla, procesarAlerta IGUAL se ejecuta (aislados) y tick() no propaga', async () => {
    getSnapshot.mockResolvedValue({ estadoGeneral: 'normal' });
    alertaJob.procesarAlerta.mockResolvedValue(undefined);
    historialSvc.guardarSnapshot.mockRejectedValue(new Error('Mongo caído'));

    await expect(tick()).resolves.not.toThrow();

    expect(alertaJob.procesarAlerta).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('historial'));
  });

  test('el callback registrado en cron.schedule atrapa un fallo de getSnapshot sin propagar', async () => {
    getSnapshot.mockRejectedValue(new Error('todo caído'));

    await expect(callbackRegistrado()).resolves.not.toThrow();
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('Error fatal'));
  });
});
