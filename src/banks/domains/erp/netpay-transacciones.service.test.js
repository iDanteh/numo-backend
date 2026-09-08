'use strict';

// netpay-transacciones.service.test.js — Fase 1 de la sección Netpay (consulta en
// vivo): consultarTransaccionesNetpay() agrega totales/porAlmacen sobre TODAS las
// páginas que devuelve buscarTransaccionesNetpay() (kore-caja.service.js, mockeada
// acá) — Kore pagina (default 20, máximo real 100 confirmado por el usuario), no
// alcanza con agregar sobre una sola respuesta.

jest.mock('./kore-caja.service');

const { buscarTransaccionesNetpay } = require('./kore-caja.service');
const { consultarTransaccionesNetpay } = require('./netpay-transacciones.service');

function fakePage(transactions, { page = 1, totalPages = 1 } = {}) {
  return {
    raw: {
      Mensaje: 'Transacciones encontradas',
      Data: { transactions, page, pageSize: 100, totalCount: transactions.length, totalPages },
      Codigo: 200,
    },
  };
}

beforeEach(() => {
  jest.clearAllMocks();
});

test('lista vacía (1 sola página): totales en 0 y porAlmacen []', async () => {
  buscarTransaccionesNetpay.mockResolvedValue(fakePage([]));

  const resultado = await consultarTransaccionesNetpay({});

  expect(resultado.transacciones).toEqual([]);
  expect(resultado.totales).toEqual({ monto: 0, comision: 0, neto: 0 });
  expect(resultado.porAlmacen).toEqual([]);
  expect(buscarTransaccionesNetpay).toHaveBeenCalledTimes(1);
});

test('una transacción (1 sola página): totales y porAlmacen reflejan ese único registro', async () => {
  const t = { ID: 1, amount: 2495.44, commission: 28.65763296, almacen: 'A0' };
  buscarTransaccionesNetpay.mockResolvedValue(fakePage([t]));

  const resultado = await consultarTransaccionesNetpay({});

  expect(resultado.transacciones).toEqual([t]);
  expect(resultado.totales.monto).toBeCloseTo(2495.44);
  expect(resultado.totales.comision).toBeCloseTo(28.65763296);
  expect(resultado.totales.neto).toBeCloseTo(2495.44 - 28.65763296);
  expect(resultado.porAlmacen).toEqual([
    { almacen: 'A0', totalMonto: 2495.44, totalComision: 28.65763296, neto: 2495.44 - 28.65763296 },
  ]);
});

test('varias transacciones del MISMO almacén (1 sola página): se suman en un solo grupo', async () => {
  const transacciones = [
    { amount: 100, commission: 10, almacen: 'A0' },
    { amount: 200, commission: 20, almacen: 'A0' },
  ];
  buscarTransaccionesNetpay.mockResolvedValue(fakePage(transacciones));

  const resultado = await consultarTransaccionesNetpay({});

  expect(resultado.totales).toEqual({ monto: 300, comision: 30, neto: 270 });
  expect(resultado.porAlmacen).toEqual([
    { almacen: 'A0', totalMonto: 300, totalComision: 30, neto: 270 },
  ]);
});

test('almacenes DISTINTOS (1 sola página): grupos separados, ordenados alfabéticamente', async () => {
  const transacciones = [
    { amount: 100, commission: 10, almacen: 'N0' },
    { amount: 50, commission: 5, almacen: 'A0' },
  ];
  buscarTransaccionesNetpay.mockResolvedValue(fakePage(transacciones));

  const resultado = await consultarTransaccionesNetpay({});

  expect(resultado.porAlmacen).toEqual([
    { almacen: 'A0', totalMonto: 50, totalComision: 5, neto: 45 },
    { almacen: 'N0', totalMonto: 100, totalComision: 10, neto: 90 },
  ]);
});

test('commission null/undefined se trata como 0, no rompe ni contamina el total', async () => {
  const transacciones = [
    { amount: 100, commission: null, almacen: 'A0' },
    { amount: 50, almacen: 'A0' }, // sin campo commission
  ];
  buscarTransaccionesNetpay.mockResolvedValue(fakePage(transacciones));

  const resultado = await consultarTransaccionesNetpay({});

  expect(resultado.totales.comision).toBe(0);
  expect(resultado.porAlmacen).toEqual([
    { almacen: 'A0', totalMonto: 150, totalComision: 0, neto: 150 },
  ]);
});

test('almacen null/undefined agrupa bajo "(sin almacén)"', async () => {
  const transacciones = [{ amount: 100, commission: 0, almacen: null }];
  buscarTransaccionesNetpay.mockResolvedValue(fakePage(transacciones));

  const resultado = await consultarTransaccionesNetpay({});

  expect(resultado.porAlmacen).toEqual([
    { almacen: '(sin almacén)', totalMonto: 100, totalComision: 0, neto: 100 },
  ]);
});

test('responseCode/almacenes/dateFrom/dateTo se pasan tal cual, con page/pageSize agregados', async () => {
  buscarTransaccionesNetpay.mockResolvedValue(fakePage([]));

  await consultarTransaccionesNetpay({
    responseCode: '00', almacenes: 'A0,N0', dateFrom: '2026-09-04T00:00:00Z', dateTo: '2026-09-04T23:59:59Z',
  });

  expect(buscarTransaccionesNetpay).toHaveBeenCalledWith({
    responseCode: '00', almacenes: 'A0,N0', dateFrom: '2026-09-04T00:00:00Z', dateTo: '2026-09-04T23:59:59Z',
    page: 1, pageSize: 100,
  });
});

// CORRECCIÓN 2026-09-08 (hallazgo real del usuario): Kore pagina — caso real
// reportado (totalCount=59, pageSize=20 default, 3 páginas). consultarTransaccionesNetpay
// debe recorrer TODAS las páginas antes de agregar, no solo la primera.
describe('paginación de Kore — recorre TODAS las páginas antes de agregar totales', () => {
  test('3 páginas: junta las 3 respuestas y agrega sobre el total real, no solo la página 1', async () => {
    const pagina1 = [{ amount: 10, commission: 1, almacen: 'A0' }, { amount: 10, commission: 1, almacen: 'A0' }];
    const pagina2 = [{ amount: 20, commission: 2, almacen: 'A0' }, { amount: 20, commission: 2, almacen: 'A0' }];
    const pagina3 = [{ amount: 30, commission: 3, almacen: 'A0' }];

    buscarTransaccionesNetpay
      .mockResolvedValueOnce(fakePage(pagina1, { page: 1, totalPages: 3 }))
      .mockResolvedValueOnce(fakePage(pagina2, { page: 2, totalPages: 3 }))
      .mockResolvedValueOnce(fakePage(pagina3, { page: 3, totalPages: 3 }));

    const resultado = await consultarTransaccionesNetpay({});

    expect(buscarTransaccionesNetpay).toHaveBeenCalledTimes(3);
    expect(buscarTransaccionesNetpay).toHaveBeenNthCalledWith(1, expect.objectContaining({ page: 1, pageSize: 100 }));
    expect(buscarTransaccionesNetpay).toHaveBeenNthCalledWith(2, expect.objectContaining({ page: 2, pageSize: 100 }));
    expect(buscarTransaccionesNetpay).toHaveBeenNthCalledWith(3, expect.objectContaining({ page: 3, pageSize: 100 }));
    expect(resultado.transacciones.length).toBe(5);
    expect(resultado.totales).toEqual({ monto: 90, comision: 9, neto: 81 });
  });

  test('respeta el tope defensivo de páginas si Kore reporta un totalPages irreal (no entra en loop infinito)', async () => {
    buscarTransaccionesNetpay.mockImplementation(async ({ page }) =>
      fakePage([{ amount: 1, commission: 0, almacen: 'A0' }], { page, totalPages: 999999 }));

    const resultado = await consultarTransaccionesNetpay({});

    expect(buscarTransaccionesNetpay).toHaveBeenCalledTimes(50); // MAX_PAGINAS
    expect(resultado.transacciones.length).toBe(50);
  });
});
