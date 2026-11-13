'use strict';

// loyalty-security-event.service.test.js (2026-11-13, pedido explícito del usuario) — lectura de
// LoyaltySecurityEvent para auditoría de los flujos OTP (consulta pública y canje con Kore). Nunca
// expone el código en sí (no existe en el modelo) — eso es a propósito, ver docstring del service.

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltySecurityEvent: { findAndCountAll: jest.fn() },
}));

const { Op } = require('sequelize');
const { LoyaltySecurityEvent } = require('../../../shared/models/postgres');
const { listarEventosSeguridad } = require('./loyalty-security-event.service');

beforeEach(() => jest.clearAllMocks());

describe('listarEventosSeguridad', () => {
  test('sin filtros, devuelve los últimos eventos con el límite default (100)', async () => {
    LoyaltySecurityEvent.findAndCountAll.mockResolvedValue({ count: 3, rows: [{ id: 1 }, { id: 2 }, { id: 3 }] });

    const resultado = await listarEventosSeguridad();

    expect(resultado).toEqual({ total: 3, eventos: [{ id: 1 }, { id: 2 }, { id: 3 }] });
    expect(LoyaltySecurityEvent.findAndCountAll).toHaveBeenCalledWith(expect.objectContaining({
      where: {},
      order: [['createdAt', 'DESC']],
      limit: 100,
      offset: 0,
    }));
  });

  test('filtra por teléfono, tipo y resultado', async () => {
    LoyaltySecurityEvent.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });

    await listarEventosSeguridad({ telefono: '5512345678', tipo: 'solicitud_codigo_canje', resultado: 'fallo' });

    expect(LoyaltySecurityEvent.findAndCountAll).toHaveBeenCalledWith(expect.objectContaining({
      where: { telefono: '5512345678', tipo: 'solicitud_codigo_canje', resultado: 'fallo' },
    }));
  });

  test('resultado inválido (ni exito ni fallo) → BadRequestError, nunca llega a la base', async () => {
    await expect(listarEventosSeguridad({ resultado: 'otracosa' })).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltySecurityEvent.findAndCountAll).not.toHaveBeenCalled();
  });

  test('desde/hasta arman un rango en createdAt con Op.gte/Op.lte', async () => {
    LoyaltySecurityEvent.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });

    await listarEventosSeguridad({ desde: '2026-11-01', hasta: '2026-11-13' });

    const [{ where }] = LoyaltySecurityEvent.findAndCountAll.mock.calls[0];
    expect(where.createdAt[Op.gte]).toEqual(new Date('2026-11-01'));
    expect(where.createdAt[Op.lte]).toEqual(new Date('2026-11-13'));
  });

  test('fecha inválida en "desde" → BadRequestError, nunca llega a la base', async () => {
    await expect(listarEventosSeguridad({ desde: 'no-es-una-fecha' })).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltySecurityEvent.findAndCountAll).not.toHaveBeenCalled();
  });

  test('limit por encima del máximo (500) se recorta', async () => {
    LoyaltySecurityEvent.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });

    await listarEventosSeguridad({ limit: 9999 });

    expect(LoyaltySecurityEvent.findAndCountAll).toHaveBeenCalledWith(expect.objectContaining({ limit: 500 }));
  });

  test('offset se respeta para paginar', async () => {
    LoyaltySecurityEvent.findAndCountAll.mockResolvedValue({ count: 0, rows: [] });

    await listarEventosSeguridad({ offset: 50 });

    expect(LoyaltySecurityEvent.findAndCountAll).toHaveBeenCalledWith(expect.objectContaining({ offset: 50 }));
  });

  test('nunca pide ni devuelve el código en texto plano (no existe esa columna en el modelo)', async () => {
    LoyaltySecurityEvent.findAndCountAll.mockResolvedValue({
      count: 1,
      rows: [{ id: 1, telefono: '5512345678', tipo: 'confirmacion_codigo_canje', resultado: 'exito', motivo: null, createdAt: new Date() }],
    });

    const resultado = await listarEventosSeguridad();

    expect(JSON.stringify(resultado)).not.toMatch(/codigo.{0,3}:.{0,3}"\d{6}"/i);
  });
});
