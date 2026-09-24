'use strict';

// event-loop-lag.util.js habilita el histograma como efecto de carga del módulo
// (top-level) — cada test necesita jest.resetModules() + jest.doMock() (NO
// jest.mock hoisteado: necesitamos una referencia FRESCA de monitorEventLoopDelay
// después del reset, no la de arriba del archivo, que quedaría stale) + un require
// fresco de ambos módulos para que el histograma fake tome efecto.
function fakeHistograma(mean) {
  return { enable: jest.fn(), reset: jest.fn(), mean };
}

function requerirConHistograma(mean) {
  const histograma = fakeHistograma(mean);
  jest.resetModules();
  jest.doMock('perf_hooks', () => ({ monitorEventLoopDelay: jest.fn().mockReturnValue(histograma) }));
  const util = require('./event-loop-lag.util');
  return { histograma, util };
}

describe('event-loop-lag.util', () => {
  afterEach(() => {
    jest.dontMock('perf_hooks');
  });

  test('habilita el histograma al cargar el módulo', () => {
    const { histograma } = requerirConHistograma(0);
    expect(histograma.enable).toHaveBeenCalledTimes(1);
  });

  test('sin muestras (mean = NaN) devuelve 0 en vez de NaN', () => {
    const { util } = requerirConHistograma(NaN);
    expect(util.leerYReiniciarLagMs()).toBe(0);
  });

  test('convierte nanosegundos a milisegundos', () => {
    const { util } = requerirConHistograma(5_000_000); // 5ms expresados en ns
    expect(util.leerYReiniciarLagMs()).toBe(5);
  });

  test('resetea el histograma después de cada lectura', () => {
    const { histograma, util } = requerirConHistograma(1_000_000);
    util.leerYReiniciarLagMs();
    util.leerYReiniciarLagMs();
    expect(histograma.reset).toHaveBeenCalledTimes(2);
  });
});
