'use strict';

// netpay-match-confirm.service.test.js — netpay-matching-v2 (design.md "File Changes":
// "keeps _recalcularNetoEnVivo, drops confirm/discard"): confirmarMatchNetpay/
// descartarMatchNetpay (candidate picker manual) se ELIMINAN de este archivo —
// netpay-evaluacion.service.js reemplaza la confirmación automática, y
// netpay-resolver.service.js reemplaza la resolución/rechazo manual (ver design.md
// "Approach": "Candidate picker removed for Netpay"). _recalcularNetoEnVivo SOBREVIVE
// intacta (misma implementación) — sigue recalculando el neto EN VIVO contra Kore para un
// terminalID+día exacto, reusada por netpay-resolver.service.js para mostrar el neto
// actualizado antes de una resolución manual.
jest.mock('./netpay-transacciones.service');

const { consultarTransaccionesNetpay } = require('./netpay-transacciones.service');
const { _diaMx } = require('./netpay-match.service');
const { _recalcularNetoEnVivo } = require('./netpay-match-confirm.service');

const TERMINAL = '2840403056';

function fakeTransaccion(overrides = {}) {
  return { amount: 300, commission: 20, almacen: 'A0', terminalID: TERMINAL, transactionDate: '2026-09-10T14:00:00Z', ...overrides };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('_recalcularNetoEnVivo', () => {
  test('recalcula el neto EN VIVO mandando la fecha pelada (YYYY-MM-DD) a consultarTransaccionesNetpay, no un ISO completo en UTC', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [fakeTransaccion()] });
    const diaMarcador = new Date('2026-09-10T00:00:00.000Z');

    const { netoEsperado, cantidadTransacciones } = await _recalcularNetoEnVivo(TERMINAL, diaMarcador);

    expect(consultarTransaccionesNetpay).toHaveBeenCalledWith({ dateFrom: '2026-09-10', dateTo: '2026-09-10', terminalID: TERMINAL });
    expect(netoEsperado).toBe(280);
    expect(cantidadTransacciones).toBe(1);
  });

  test('filtra en memoria por _diaMx: descarta transacciones que Kore devolviera fuera del día exacto pedido', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({
      transacciones: [
        fakeTransaccion({ transactionDate: '2026-09-10T14:00:00Z' }), // día 10 real
        fakeTransaccion({ amount: 999, commission: 0, transactionDate: '2026-09-11T13:00:00Z' }), // día 11 real (07:00 MX) — descartada
      ],
    });
    const diaMarcador = _diaMx('2026-09-10T14:00:00Z');

    const { netoEsperado, cantidadTransacciones } = await _recalcularNetoEnVivo(TERMINAL, diaMarcador);

    expect(netoEsperado).toBe(280);
    expect(cantidadTransacciones).toBe(1);
  });

  test('sin transacciones ese día: neto 0, cantidad 0', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [] });
    const diaMarcador = new Date('2026-09-10T00:00:00.000Z');

    const { netoEsperado, cantidadTransacciones } = await _recalcularNetoEnVivo(TERMINAL, diaMarcador);

    expect(netoEsperado).toBe(0);
    expect(cantidadTransacciones).toBe(0);
  });
});

// Guard de diseño: confirmarMatchNetpay/descartarMatchNetpay ya NO se exportan desde este
// archivo — el candidate picker manual fue removido (ver design.md "Approach").
test('confirmarMatchNetpay/descartarMatchNetpay ya no se exportan (removidos, ver netpay-evaluacion.service.js / netpay-resolver.service.js)', () => {
  // eslint-disable-next-line global-require
  const mod = require('./netpay-match-confirm.service');
  expect(mod.confirmarMatchNetpay).toBeUndefined();
  expect(mod.descartarMatchNetpay).toBeUndefined();
});
