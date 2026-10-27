'use strict';

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyKoreVentaFallida: { findOne: jest.fn(), create: jest.fn(), findAll: jest.fn(), findByPk: jest.fn() },
}));

const { LoyaltyKoreVentaFallida } = require('../../../shared/models/postgres');
const service = require('./loyalty-venta-fallida.service');

function makeFila(overrides = {}) {
  const fila = { id: 1, serieFolio: 'A0-1', motivo: 'x', intentos: 1, resuelto: false, ...overrides };
  fila.update = jest.fn(async (changes) => { Object.assign(fila, changes); return fila; });
  return fila;
}

beforeEach(() => jest.clearAllMocks());

describe('registrarVentaFallida', () => {
  it('crea una fila nueva si no hay una sin resolver para ese serieFolio', async () => {
    LoyaltyKoreVentaFallida.findOne.mockResolvedValue(null);
    LoyaltyKoreVentaFallida.create.mockResolvedValue(makeFila());
    await service.registrarVentaFallida('A0-1', { a: 1 }, 'motivo x');
    expect(LoyaltyKoreVentaFallida.create).toHaveBeenCalledWith(expect.objectContaining({
      serieFolio: 'A0-1', motivo: 'motivo x', intentos: 1, resuelto: false,
    }));
  });

  it('incrementa intentos en la fila existente sin resolver', async () => {
    const existente = makeFila({ intentos: 2 });
    LoyaltyKoreVentaFallida.findOne.mockResolvedValue(existente);
    await service.registrarVentaFallida('A0-1', { a: 1 }, 'motivo nuevo');
    expect(existente.update).toHaveBeenCalledWith(expect.objectContaining({ motivo: 'motivo nuevo', intentos: 3 }));
    expect(LoyaltyKoreVentaFallida.create).not.toHaveBeenCalled();
  });

  it('nunca lanza, aunque falle la escritura', async () => {
    LoyaltyKoreVentaFallida.findOne.mockRejectedValue(new Error('db caída'));
    await expect(service.registrarVentaFallida('A0-1', {}, 'x')).resolves.toBeNull();
  });
});

describe('listarVentasFallidas / marcarVentaFallidaResuelta', () => {
  it('filtra por soloNoResueltas', async () => {
    LoyaltyKoreVentaFallida.findAll.mockResolvedValue([]);
    await service.listarVentasFallidas({ soloNoResueltas: true });
    expect(LoyaltyKoreVentaFallida.findAll).toHaveBeenCalledWith(expect.objectContaining({ where: { resuelto: false } }));
  });

  it('marca resuelto con usuario y fecha', async () => {
    const fila = makeFila();
    LoyaltyKoreVentaFallida.findByPk.mockResolvedValue(fila);
    await service.marcarVentaFallidaResuelta(1, { dbId: 7, nombre: 'Admin' });
    expect(fila.update).toHaveBeenCalledWith(expect.objectContaining({
      resuelto: true, resueltoPorUserId: '7', resueltoPorNombre: 'Admin',
    }));
  });

  it('404 si el id no existe', async () => {
    LoyaltyKoreVentaFallida.findByPk.mockResolvedValue(null);
    await expect(service.marcarVentaFallidaResuelta(99, {})).rejects.toMatchObject({ statusCode: 404 });
  });
});
