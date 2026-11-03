'use strict';

/**
 * loyalty-kore-catalogos.client.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * axios + Configuraciones Globales mockeados — nunca se llama a Kore real.
 * Mismo criterio que loyalty-kore-monedero.client.test.js.
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
const { obtenerConceptosKore, obtenerFormasPagoKore } = require('./loyalty-kore-catalogos.client');

const CONCEPTOS_URL = 'https://test.cajas.koreingenieria.com/conceptos';
const FORMAS_PAGO_URL = 'https://test.formaspagos.koreingenieria.com/api/formasdepago';

beforeEach(() => {
  jest.clearAllMocks();
  globalConfigService.getValue.mockImplementation(async (section, clave) => {
    if (section === 'kore' && clave === 'CONCEPTOS_URL') return CONCEPTOS_URL;
    if (section === 'kore' && clave === 'FORMAS_PAGO_URL') return FORMAS_PAGO_URL;
    if (section === 'bancos' && clave === 'TOKEN') return 'tok-123';
    throw new Error(`config no mockeada: ${section}.${clave}`);
  });
});

afterEach(() => {
  jest.useRealTimers();
});

describe('obtenerConceptosKore', () => {
  test('devuelve Data tal cual, con bearer token', async () => {
    const data = [{ Id: 'a1', Nombre: 'COBRO CONTRA ENTREGA', Abreviatura: 'CCE', Tipo: 'ENTRADA' }];
    axios.get.mockResolvedValue({ data: { Mensaje: 'ok', Data: data } });

    const conceptos = await obtenerConceptosKore();

    expect(conceptos).toEqual(data);
    expect(axios.get).toHaveBeenCalledWith(CONCEPTOS_URL, {
      headers: { Authorization: 'Bearer tok-123' },
      timeout: 60000,
    });
  });

  test('Data ausente o no es array: devuelve []', async () => {
    axios.get.mockResolvedValue({ data: { Mensaje: 'ok' } });
    expect(await obtenerConceptosKore()).toEqual([]);
  });

  test('token vacío: error explícito, nunca llama a Kore', async () => {
    globalConfigService.getValue.mockImplementation(async (section, clave) => {
      if (clave === 'CONCEPTOS_URL') return CONCEPTOS_URL;
      if (section === 'bancos' && clave === 'TOKEN') return '';
      throw new Error('no mockeado');
    });

    await expect(obtenerConceptosKore()).rejects.toThrow(/bancos\.TOKEN/);
    expect(axios.get).not.toHaveBeenCalled();
  });

  test('429: reintenta respetando el "retry after" del cuerpo y tiene éxito en el 2do intento', async () => {
    jest.useFakeTimers();
    const err429 = { response: { status: 429, data: { Data: 'retry after: 0.01 segundos' } } };
    axios.get
      .mockRejectedValueOnce(err429)
      .mockResolvedValueOnce({ data: { Data: [{ Id: '1' }] } });

    const promise = obtenerConceptosKore();
    await jest.advanceTimersByTimeAsync(2000);
    const conceptos = await promise;

    expect(conceptos).toEqual([{ Id: '1' }]);
    expect(axios.get).toHaveBeenCalledTimes(2);
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('429 en'));
  });

  test('agota los 3 intentos sin éxito: propaga el error', async () => {
    jest.useFakeTimers();
    const errTimeout = { code: 'ECONNABORTED', message: 'timeout of 60000ms exceeded' };
    axios.get.mockRejectedValue(errTimeout);

    const promise = obtenerConceptosKore();
    promise.catch(() => {});
    await jest.advanceTimersByTimeAsync(3000);
    await jest.advanceTimersByTimeAsync(6000);

    await expect(promise).rejects.toBe(errTimeout);
    expect(axios.get).toHaveBeenCalledTimes(3);
  });

  test('un error que no es 429 ni timeout (ej. 401) nunca se reintenta', async () => {
    const err401 = { response: { status: 401, data: { Mensaje: 'No autorizado' } } };
    axios.get.mockRejectedValue(err401);

    await expect(obtenerConceptosKore()).rejects.toBe(err401);
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('401'));
  });
});

describe('obtenerFormasPagoKore', () => {
  test('devuelve Data tal cual, con bearer token', async () => {
    const data = [{ ID: 'f1', Nombre: 'EFECTIVO', AcumulaPuntosTuberos: true, EsPuntos: false }];
    axios.get.mockResolvedValue({ data: { Mensaje: 'ok', Data: data } });

    const formasPago = await obtenerFormasPagoKore();

    expect(formasPago).toEqual(data);
    expect(axios.get).toHaveBeenCalledWith(FORMAS_PAGO_URL, {
      headers: { Authorization: 'Bearer tok-123' },
      timeout: 60000,
    });
  });

  test('Data ausente o no es array: devuelve []', async () => {
    axios.get.mockResolvedValue({ data: {} });
    expect(await obtenerFormasPagoKore()).toEqual([]);
  });
});
