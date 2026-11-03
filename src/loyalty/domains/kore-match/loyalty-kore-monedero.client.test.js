'use strict';

/**
 * loyalty-kore-monedero.client.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * axios + Configuraciones Globales mockeados — nunca se llama a Kore real.
 * Mismo criterio de fake timers que erp-reversion.routes.test.js para probar
 * el reintento con backoff sin esperas reales.
 */

jest.mock('axios');
jest.mock('../../../shared/services/global-config.service', () => ({
  getValue: jest.fn(),
}));
jest.mock('../../../shared/utils/logger', () => ({
  logger: { warn: jest.fn(), error: jest.fn(), info: jest.fn() },
}));

const axios = require('axios');
const globalConfigService = require('../../../shared/services/global-config.service');
const { logger } = require('../../../shared/utils/logger');
const { obtenerClientesConMonedero } = require('./loyalty-kore-monedero.client');

const URL = 'https://app.login.tubosyconexiones.mx/clientes/monedero';

beforeEach(() => {
  jest.clearAllMocks();
  globalConfigService.getValue.mockImplementation(async (section, clave) => {
    if (section === 'kore' && clave === 'PUNTOS_KORE') return `${URL}/`; // con slash final, se recorta
    if (section === 'bancos' && clave === 'TOKEN') return 'tok-123';
    throw new Error(`config no mockeada: ${section}.${clave}`);
  });
});

afterEach(() => {
  jest.useRealTimers();
});

describe('obtenerClientesConMonedero', () => {
  test('una sola página (total <= limit): una sola llamada, con bearer token y params correctos', async () => {
    axios.get.mockResolvedValue({
      data: { Mensaje: 'Consultado correctamente', Data: { clientes: [{ id: '1066bvfaevunm', monedero: '8268' }], total: 1 }, Codigo: 200 },
    });

    const clientes = await obtenerClientesConMonedero();

    expect(clientes).toEqual([{ id: '1066bvfaevunm', monedero: '8268' }]);
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(URL, {
      params:  { page: 1, limit: 100, tieneMonedero: true },
      headers: { Authorization: 'Bearer tok-123' },
      timeout: 60000,
    });
  });

  test('pagina hasta completar Data.total', async () => {
    axios.get
      .mockResolvedValueOnce({ data: { Data: { clientes: Array.from({ length: 100 }, (_, i) => ({ id: String(i) })), total: 150 } } })
      .mockResolvedValueOnce({ data: { Data: { clientes: Array.from({ length: 50 }, (_, i) => ({ id: String(100 + i) })), total: 150 } } });

    const clientes = await obtenerClientesConMonedero();

    expect(clientes).toHaveLength(150);
    expect(axios.get).toHaveBeenCalledTimes(2);
    expect(axios.get.mock.calls[0][1].params).toEqual({ page: 1, limit: 100, tieneMonedero: true });
    expect(axios.get.mock.calls[1][1].params).toEqual({ page: 2, limit: 100, tieneMonedero: true });
  });

  test('corta el loop si Kore deja de devolver filas, sin esperar a que total coincida', async () => {
    axios.get
      .mockResolvedValueOnce({ data: { Data: { clientes: Array.from({ length: 10 }, (_, i) => ({ id: String(i) })), total: 150 } } })
      .mockResolvedValueOnce({ data: { Data: { clientes: [], total: 150 } } });

    const clientes = await obtenerClientesConMonedero();

    expect(clientes).toHaveLength(10);
    expect(axios.get).toHaveBeenCalledTimes(2);
  });

  test('token vacío (bancos.TOKEN sin configurar en Configuraciones Globales): error explícito, nunca llama a Kore', async () => {
    globalConfigService.getValue.mockImplementation(async (section, clave) => {
      if (clave === 'PUNTOS_KORE') return URL;
      if (section === 'bancos' && clave === 'TOKEN') return '';
      throw new Error('no mockeado');
    });

    await expect(obtenerClientesConMonedero()).rejects.toThrow(/bancos\.TOKEN/);
    expect(axios.get).not.toHaveBeenCalled();
  });

  test('429: reintenta respetando el "retry after" del cuerpo y tiene éxito en el 2do intento', async () => {
    jest.useFakeTimers();
    const err429 = { response: { status: 429, data: { Data: 'retry after: 0.01 segundos' } } };
    axios.get
      .mockRejectedValueOnce(err429)
      .mockResolvedValueOnce({ data: { Data: { clientes: [{ id: '1', monedero: '5' }], total: 1 } } });

    const promise = obtenerClientesConMonedero();
    await jest.advanceTimersByTimeAsync(2000); // 0.01 + 1s de margen ≈ 1.01s
    const clientes = await promise;

    expect(clientes).toHaveLength(1);
    expect(axios.get).toHaveBeenCalledTimes(2);
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('429 en'));
  });

  test('timeout: reintenta con backoff fijo (3*intento segundos) y tiene éxito en el 2do intento', async () => {
    jest.useFakeTimers();
    const errTimeout = { code: 'ECONNABORTED', message: 'timeout of 60000ms exceeded' };
    axios.get
      .mockRejectedValueOnce(errTimeout)
      .mockResolvedValueOnce({ data: { Data: { clientes: [{ id: '1', monedero: '5' }], total: 1 } } });

    const promise = obtenerClientesConMonedero();
    await jest.advanceTimersByTimeAsync(4000); // backoff de 3s en el intento 1
    const clientes = await promise;

    expect(clientes).toHaveLength(1);
    expect(axios.get).toHaveBeenCalledTimes(2);
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('timeout en'));
  });

  test('agota los 3 intentos sin éxito: propaga el error, sin loop infinito', async () => {
    jest.useFakeTimers();
    const errTimeout = { code: 'ECONNABORTED', message: 'timeout of 60000ms exceeded' };
    axios.get.mockRejectedValue(errTimeout);

    const promise = obtenerClientesConMonedero();
    promise.catch(() => {}); // evita unhandledRejection mientras avanzamos los timers
    await jest.advanceTimersByTimeAsync(3000);
    await jest.advanceTimersByTimeAsync(6000);

    await expect(promise).rejects.toBe(errTimeout);
    expect(axios.get).toHaveBeenCalledTimes(3);
  });

  test('un error que no es 429 ni timeout (ej. 401) nunca se reintenta', async () => {
    const err401 = { response: { status: 401, data: { Mensaje: 'No autorizado' } } };
    axios.get.mockRejectedValue(err401);

    await expect(obtenerClientesConMonedero()).rejects.toBe(err401);
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('401'));
  });
});
