'use strict';

/**
 * domains/cards/loyalty-card.service.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad (PR2) — alta/edición de tarjetas y cambio de nivel.
 * `nip.util` se usa REAL (no mockeado): así se verifica que el NIP que vuelve
 * en la respuesta nunca es el mismo valor que se persiste como `nipHash`.
 */

const { Transaction } = require('sequelize');

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard:        { findOne: jest.fn(), findByPk: jest.fn(), create: jest.fn() },
  LoyaltyTierHistory: { create: jest.fn() },
}));
jest.mock('../../../config/database.postgres', () => ({
  sequelize: { transaction: jest.fn() },
}));

const { LoyaltyCard, LoyaltyTierHistory } = require('../../../shared/models/postgres');
const { sequelize } = require('../../../config/database.postgres');
const cardService = require('./loyalty-card.service');

function makeCard(overrides = {}) {
  const card = {
    id: 1,
    telefono: '5512345678',
    nombre: 'Juan Pérez',
    nivel: 'Bronce',
    saldoPuntos: '0.00',
    activa: true,
    nipHash: 'hash-existente',
    ...overrides,
  };
  card.update = jest.fn(async (changes) => {
    Object.assign(card, changes);
    return card;
  });
  return card;
}

const USER = { dbId: 7, nombre: 'Admin' };

beforeEach(() => {
  jest.clearAllMocks();
  sequelize.transaction.mockImplementation(async (cb) => cb({ fake: true }));
  LoyaltyCard.create.mockImplementation(async (data) => ({ ...makeCard(), ...data }));
});

describe('crearTarjeta', () => {
  test('teléfono duplicado (tarjeta activa o inactiva) → 409, no inserta', async () => {
    LoyaltyCard.findOne.mockResolvedValue(makeCard({ activa: false }));

    await expect(
      cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Otro' }, USER),
    ).rejects.toMatchObject({ statusCode: 409 });

    expect(LoyaltyCard.create).not.toHaveBeenCalled();
  });

  test('teléfono único crea la tarjeta con nivel Bronce por default, saldo 0, activa', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    const { card, nip } = await cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Juan Pérez' }, USER);

    expect(LoyaltyCard.create).toHaveBeenCalledWith(expect.objectContaining({
      telefono: '5512345678', nombre: 'Juan Pérez', nivel: 'Bronce', saldoPuntos: 0, activa: true,
    }));
    expect(nip).toMatch(/^\d{4}$/);
    expect(card).toBeDefined();
  });

  test('el NIP devuelto nunca es igual al hash que se persiste', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    const { nip } = await cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Juan Pérez' }, USER);
    const [[createArgs]] = LoyaltyCard.create.mock.calls;

    expect(createArgs.nipHash).toBeDefined();
    expect(createArgs.nipHash).not.toBe(nip);
  });

  test('staff puede fijar el nivel inicial explícitamente', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Juan Pérez', nivel: 'Oro' }, USER);

    expect(LoyaltyCard.create).toHaveBeenCalledWith(expect.objectContaining({ nivel: 'Oro' }));
  });

  test('normaliza el teléfono antes de buscar/crear (+52, espacios, guiones)', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await cardService.crearTarjeta({ telefono: '+52 55-1234-5678', nombre: 'Juan Pérez' }, USER);

    expect(LoyaltyCard.findOne).toHaveBeenCalledWith({ where: { telefono: '5512345678' } });
    expect(LoyaltyCard.create).toHaveBeenCalledWith(expect.objectContaining({ telefono: '5512345678' }));
  });

  test('captura SequelizeUniqueConstraintError como 409 (carrera entre findOne y create)', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);
    const err = new Error('duplicate');
    err.name = 'SequelizeUniqueConstraintError';
    LoyaltyCard.create.mockRejectedValue(err);

    await expect(
      cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Juan Pérez' }, USER),
    ).rejects.toMatchObject({ statusCode: 409 });
  });

  // PR8 (migración): crearTarjeta se reutiliza dentro de la transacción única del
  // script de migración (design.md "Apply. One Postgres transaction for all ok rows") —
  // necesita poder recibir y propagar esa transacción externa a findOne/create en vez
  // de abrir su propio connection slot implícito, igual que cambiarNivel ya hace con su
  // propia transacción interna.
  test('con transaction explícito (3er argumento), la propaga a findOne y create', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);
    const fakeT = { fake: true };

    await cardService.crearTarjeta(
      { telefono: '5512345678', nombre: 'Juan Pérez' },
      USER,
      { transaction: fakeT },
    );

    expect(LoyaltyCard.findOne).toHaveBeenCalledWith({ where: { telefono: '5512345678' }, transaction: fakeT });
    expect(LoyaltyCard.create).toHaveBeenCalledWith(expect.objectContaining({ telefono: '5512345678' }), { transaction: fakeT });
  });

  test('sin transaction explícito, el llamado a rutas existente sigue invocando findOne/create con un solo argumento', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await cardService.crearTarjeta({ telefono: '5512345678', nombre: 'Juan Pérez' }, USER);

    expect(LoyaltyCard.findOne.mock.calls[0]).toHaveLength(1);
    expect(LoyaltyCard.create.mock.calls[0]).toHaveLength(1);
  });
});

describe('editarTarjeta', () => {
  test('editar solo nombre deja teléfono/saldo/nivel sin cambios', async () => {
    const card = makeCard();
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.editarTarjeta(1, { nombre: 'Juan P. Editado' }, USER);

    expect(card.nombre).toBe('Juan P. Editado');
    expect(card.telefono).toBe('5512345678');
    expect(card.saldoPuntos).toBe('0.00');
    expect(card.nivel).toBe('Bronce');
  });

  test('cambiar teléfono a uno ya usado por otra tarjeta → 409, no se aplica', async () => {
    const card = makeCard();
    LoyaltyCard.findByPk.mockResolvedValue(card);
    LoyaltyCard.findOne.mockResolvedValue(makeCard({ id: 2, telefono: '5599999999' }));

    await expect(
      cardService.editarTarjeta(1, { telefono: '5599999999' }, USER),
    ).rejects.toMatchObject({ statusCode: 409 });

    expect(card.telefono).toBe('5512345678');
  });

  test('cambiar teléfono a uno libre se aplica', async () => {
    const card = makeCard();
    LoyaltyCard.findByPk.mockResolvedValue(card);
    LoyaltyCard.findOne.mockResolvedValue(null);

    await cardService.editarTarjeta(1, { telefono: '5599999999' }, USER);

    expect(card.telefono).toBe('5599999999');
  });

  test('registra updatedById/updatedByNombre en cualquier edición', async () => {
    const card = makeCard();
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.editarTarjeta(1, { nombre: 'Nuevo Nombre' }, USER);

    expect(card.updatedById).toBe('7');
    expect(card.updatedByNombre).toBe('Admin');
  });
});

describe('cambiarNip', () => {
  test('genera un nuevo hash y resetea los contadores de intentos/bloqueo', async () => {
    const card = makeCard({ nipHash: 'hash-viejo', nipIntentosFallidos: 3, nipBloqueadoHasta: new Date() });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const { nip } = await cardService.cambiarNip(1, USER);

    expect(nip).toMatch(/^\d{4}$/);
    expect(card.nipHash).not.toBe('hash-viejo');
    expect(card.nipHash).not.toBe(nip);
    expect(card.nipIntentosFallidos).toBe(0);
    expect(card.nipBloqueadoHasta).toBeNull();
  });

  test('bug real corregido (2026-10-19): registra quién hizo el cambio (updatedById/updatedByNombre)', async () => {
    const card = makeCard({ updatedById: null, updatedByNombre: null });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.cambiarNip(1, USER);

    expect(card.updatedById).toBe(String(USER.dbId));
    expect(card.updatedByNombre).toBe(USER.nombre);
  });
});

describe('cambiarNivel', () => {
  test('actualiza el nivel y escribe LoyaltyTierHistory en la misma transacción', async () => {
    const card = makeCard({ nivel: 'Bronce' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.cambiarNivel(1, 'Plata', { motivo: 'promoción manual' }, USER);

    expect(card.nivel).toBe('Plata');
    expect(LoyaltyTierHistory.create).toHaveBeenCalledWith(
      expect.objectContaining({ cardId: 1, nivelAnterior: 'Bronce', nivelNuevo: 'Plata', motivo: 'promoción manual' }),
      expect.objectContaining({ transaction: expect.anything() }),
    );
  });

  test('no mueve saldoPuntos', async () => {
    const card = makeCard({ nivel: 'Bronce', saldoPuntos: '123.45' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.cambiarNivel(1, 'Plata', {}, USER);

    expect(card.saldoPuntos).toBe('123.45');
  });

  test('nivel sin cambio real → 400', async () => {
    const card = makeCard({ nivel: 'Plata' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(cardService.cambiarNivel(1, 'Plata', {}, USER)).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyTierHistory.create).not.toHaveBeenCalled();
  });

  test('nivel inválido → 400', async () => {
    const card = makeCard({ nivel: 'Bronce' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(cardService.cambiarNivel(1, 'Diamante', {}, USER)).rejects.toMatchObject({ statusCode: 400 });
  });

  test('usa lock de fila (Transaction.LOCK.UPDATE) sobre la tarjeta', async () => {
    const card = makeCard({ nivel: 'Bronce' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await cardService.cambiarNivel(1, 'Plata', {}, USER);

    expect(LoyaltyCard.findByPk).toHaveBeenCalledWith(1, expect.objectContaining({ lock: Transaction.LOCK.UPDATE }));
  });
});
