'use strict';

// loyalty-kore-venta-log.service.test.js (2026-11-17, pedido explícito del usuario) — escritura y
// lectura de LoyaltyKoreVentaLog, auditoría append-only de cada request real a /venta y /ventas.
// Mismo patrón que loyalty-security-event.service.test.js.

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyKoreVentaLog: { findAndCountAll: jest.fn(), create: jest.fn() },
}));

const { Op } = require('sequelize');
const { LoyaltyKoreVentaLog } = require('../../../shared/models/postgres');
const { registrarVentaLog, listarVentasLog } = require('./loyalty-kore-venta-log.service');

beforeEach(() => jest.clearAllMocks());

describe('registrarVentaLog', () => {
  test('crea la fila con endpoint/payload/resultado, detalle null si no viene', async () => {
    LoyaltyKoreVentaLog.create.mockResolvedValue({ id: 1 });

    await registrarVentaLog({ endpoint: 'venta', payload: { serieFolio: 'A0-1' }, resultado: 'exito' });

    expect(LoyaltyKoreVentaLog.create).toHaveBeenCalledWith({
      endpoint: 'venta', payload: { serieFolio: 'A0-1' }, resultado: 'exito', detalle: null,
    });
  });

  test('persiste el detalle cuando resultado=error', async () => {
    LoyaltyKoreVentaLog.create.mockResolvedValue({ id: 2 });

    await registrarVentaLog({
      endpoint: 'ventas', payload: { ventas: [] }, resultado: 'error', detalle: 'La venta no existe en Kore.',
    });

    expect(LoyaltyKoreVentaLog.create).toHaveBeenCalledWith({
      endpoint: 'ventas', payload: { ventas: [] }, resultado: 'error', detalle: 'La venta no existe en Kore.',
    });
  });
});

describe('listarVentasLog', () => {
  test('sin filtros, devuelve los últimos logs con el límite default (100)', async () => {
    LoyaltyKoreVentaLog.findAndCountAll.mockResolvedValue({ count: 2, rows: [{ id: 1 }, { id: 2 }] });

    const resultado = await listarVentasLog();

    expect(resultado).toEqual({ total: 2, logs: [{ id: 1 }, { id: 2 }] });
    expect(LoyaltyKoreVentaLog.findAndCountAll).toHaveBeenCalledWith(expect.objectContaining({
      where: {}, order: [['createdAt', 'DESC']], limit: 100, offset: 0,
    }));
  });

  test('filtra por resultado y endpoint', async () => {
    LoyaltyKoreVentaLog.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });

    await listarVentasLog({ resultado: 'error', endpoint: 'ventas' });

    expect(LoyaltyKoreVentaLog.findAndCountAll).toHaveBeenCalledWith(expect.objectContaining({
      where: { resultado: 'error', endpoint: 'ventas' },
    }));
  });

  test('resultado inválido (ni exito ni error) → BadRequestError, nunca llega a la base', async () => {
    await expect(listarVentasLog({ resultado: 'otracosa' })).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyKoreVentaLog.findAndCountAll).not.toHaveBeenCalled();
  });

  test('endpoint inválido (ni venta ni ventas) → BadRequestError, nunca llega a la base', async () => {
    await expect(listarVentasLog({ endpoint: 'otracosa' })).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyKoreVentaLog.findAndCountAll).not.toHaveBeenCalled();
  });

  test('desde/hasta arman un rango en createdAt con Op.gte/Op.lte', async () => {
    LoyaltyKoreVentaLog.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });

    await listarVentasLog({ desde: '2026-11-01', hasta: '2026-11-17' });

    const [{ where }] = LoyaltyKoreVentaLog.findAndCountAll.mock.calls[0];
    expect(where.createdAt[Op.gte]).toEqual(new Date('2026-11-01'));
    expect(where.createdAt[Op.lte]).toEqual(new Date('2026-11-17'));
  });

  test('fecha inválida en "desde" → BadRequestError, nunca llega a la base', async () => {
    await expect(listarVentasLog({ desde: 'no-es-una-fecha' })).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyKoreVentaLog.findAndCountAll).not.toHaveBeenCalled();
  });

  test('limit por encima del máximo (500) se recorta', async () => {
    LoyaltyKoreVentaLog.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });

    await listarVentasLog({ limit: 9999 });

    expect(LoyaltyKoreVentaLog.findAndCountAll).toHaveBeenCalledWith(expect.objectContaining({ limit: 500 }));
  });

  test('offset se respeta para paginar', async () => {
    LoyaltyKoreVentaLog.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });

    await listarVentasLog({ offset: 50 });

    expect(LoyaltyKoreVentaLog.findAndCountAll).toHaveBeenCalledWith(expect.objectContaining({ offset: 50 }));
  });

  // `findAndCountAll` está mockeado (no hay Postgres real en estos tests), así que estos dos tests
  // simulan el ILIKE sobre payload::text que haría la base: extraen los patrones `%valor%` armados
  // por el servicio (vía Op.and/where/cast/col) y los corren contra JSON.stringify(payload), igual
  // que haría un ILIKE de texto sin importar el nivel de anidado.
  function mockFindConILikeSobrePayload(filas) {
    LoyaltyKoreVentaLog.findAndCountAll.mockImplementation(async ({ where: whereClause }) => {
      const condiciones = (whereClause && whereClause[Op.and]) || [];
      const patrones = condiciones.map((c) => {
        const simbolo = Object.getOwnPropertySymbols(c.logic)[0];
        return c.logic[simbolo];
      });
      const rows = filas.filter((fila) => {
        const texto = JSON.stringify(fila.payload).toLowerCase();
        return patrones.every((patron) => texto.includes(patron.slice(1, -1).toLowerCase()));
      });
      return { count: rows.length, rows };
    });
  }

  test('filtra por serieFolio/telefono con ILIKE sobre payload::text (endpoint "venta", a nivel raíz)', async () => {
    const filas = [
      { id: 1, payload: { serieFolio: 'A0-261100034', telefono: '9511771923' } },
      { id: 2, payload: { serieFolio: 'A0-OTRO-99', telefono: '5500000000' } },
    ];
    mockFindConILikeSobrePayload(filas);

    const porSerieFolio = await listarVentasLog({ serieFolio: 'A0-261100034' });
    expect(porSerieFolio.logs).toEqual([filas[0]]);

    const porTelefono = await listarVentasLog({ telefono: '9511771923' });
    expect(porTelefono.logs).toEqual([filas[0]]);

    const sinCoincidencia = await listarVentasLog({ serieFolio: 'NO-EXISTE' });
    expect(sinCoincidencia.logs).toEqual([]);
  });

  test('filtra por puntosAbonados anidado en abonos[] (endpoint "ventas" en lote, un nivel más profundo)', async () => {
    const filas = [
      {
        id: 1,
        payload: { ventas: [{ serieFolio: 'B0-1', abonos: [{ puntosAbonados: 8.69 }] }] },
      },
      {
        id: 2,
        payload: { ventas: [{ serieFolio: 'B0-2', abonos: [{ puntosAbonados: 3.14 }] }] },
      },
    ];
    mockFindConILikeSobrePayload(filas);

    const resultado = await listarVentasLog({ puntosAbonados: '8.69' });
    expect(resultado.logs).toEqual([filas[0]]);

    const sinCoincidencia = await listarVentasLog({ puntosAbonados: '999.99' });
    expect(sinCoincidencia.logs).toEqual([]);
  });
});
