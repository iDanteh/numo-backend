'use strict';

/**
 * loyalty-kore-cliente.service.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Webhook Kore→Numo de catálogo de clientes. `LoyaltyCard` y `crearTarjeta`
 * (de loyalty-card.service.js) mockeados — `normalizarTelefono` se usa REAL
 * (función pura), mismo criterio que loyalty-kore-match.service.test.js con
 * `normalizarTarjeta`.
 */

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard: { findOne: jest.fn() },
}));
jest.mock('../cards/loyalty-card.service', () => ({
  crearTarjeta: jest.fn(),
}));

const { LoyaltyCard } = require('../../../shared/models/postgres');
const { crearTarjeta } = require('../cards/loyalty-card.service');
const { procesarClienteKore } = require('./loyalty-kore-cliente.service');

function card(overrides = {}) {
  return {
    id: 1, telefono: '5551234567', nombre: 'CLIENTE', koreClienteId: null, koreMonedero: null,
    activa: true,
    update: jest.fn(async function update(changes) { Object.assign(this, changes); }),
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('procesarClienteKore — validación de entrada', () => {
  test('evento inválido -> BadRequestError (400)', async () => {
    await expect(
      procesarClienteKore({ evento: 'otracosa', koreClienteId: 'K1', telefono: '5551234567', nombre: 'X' }),
    ).rejects.toMatchObject({ statusCode: 400 });
  });

  test('koreClienteId faltante -> BadRequestError (400)', async () => {
    await expect(
      procesarClienteKore({ evento: 'alta', telefono: '5551234567', nombre: 'X' }),
    ).rejects.toMatchObject({ statusCode: 400 });
  });

  test('telefono con formato inválido en alta -> BadRequestError (400)', async () => {
    await expect(
      procesarClienteKore({ evento: 'alta', koreClienteId: 'K1', telefono: '123', nombre: 'X' }),
    ).rejects.toMatchObject({ statusCode: 400 });
  });

  test('telefono con formato inválido en actualizacion -> BadRequestError (400)', async () => {
    await expect(
      procesarClienteKore({ evento: 'actualizacion', koreClienteId: 'K1', telefono: 'abc' }),
    ).rejects.toMatchObject({ statusCode: 400 });
  });
});

describe('procesarClienteKore — evento alta', () => {
  test('teléfono nuevo sin monedero: crea tarjeta vía crearTarjeta() y persiste solo koreClienteId', async () => {
    LoyaltyCard.findOne.mockResolvedValueOnce(null); // no existe por teléfono
    const nueva = card({ id: 42, telefono: '5551234567' });
    crearTarjeta.mockResolvedValue({ card: nueva, nip: '7777' });

    const resultado = await procesarClienteKore({
      evento: 'alta', koreClienteId: 'K1', telefono: '5551234567', nombre: 'Juan Pérez',
    });

    expect(crearTarjeta).toHaveBeenCalledWith(
      { telefono: '5551234567', nombre: 'Juan Pérez' },
      expect.objectContaining({ dbId: 'kore-webhook' }),
    );
    expect(nueva.update).toHaveBeenCalledWith({ koreClienteId: 'K1', revisionPendiente: true });
    expect(resultado).toEqual({ ok: true, evento: 'alta', cardId: 42, creada: true });
  });

  test('el NIP generado por crearTarjeta() nunca se expone en la respuesta', async () => {
    LoyaltyCard.findOne.mockResolvedValueOnce(null);
    crearTarjeta.mockResolvedValue({ card: card(), nip: '9999' });

    const resultado = await procesarClienteKore({
      evento: 'alta', koreClienteId: 'K1', monedero: 'M1', telefono: '5551234567', nombre: 'Juan Pérez',
    });

    expect(JSON.stringify(resultado)).not.toContain('9999');
  });

  test('teléfono ya existente SIN monedero: no exige monedero (es una actualización, no una creación)', async () => {
    const existente = card({ id: 10, telefono: '5551234567' });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    const resultado = await procesarClienteKore({
      evento: 'alta', koreClienteId: 'K2', telefono: '5551234567', nombre: 'Juan Pérez',
    });

    expect(crearTarjeta).not.toHaveBeenCalled();
    expect(resultado).toEqual({ ok: true, evento: 'actualizacion', cardId: 10, creada: false });
  });

  test('teléfono ya existente: NO duplica, se trata como actualización', async () => {
    const existente = card({ id: 10, telefono: '5551234567' });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    const resultado = await procesarClienteKore({
      evento: 'alta', koreClienteId: 'K2', telefono: '5551234567', nombre: 'Juan Pérez',
    });

    expect(crearTarjeta).not.toHaveBeenCalled();
    expect(existente.update).toHaveBeenCalledWith(expect.objectContaining({ koreClienteId: 'K2' }));
    expect(resultado).toEqual({ ok: true, evento: 'actualizacion', cardId: 10, creada: false });
  });

  test('nombre faltante en alta de tarjeta nueva -> BadRequestError (400)', async () => {
    LoyaltyCard.findOne.mockResolvedValueOnce(null);

    await expect(
      procesarClienteKore({ evento: 'alta', koreClienteId: 'K1', telefono: '5551234567' }),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(crearTarjeta).not.toHaveBeenCalled();
  });
});

describe('procesarClienteKore — evento actualizacion', () => {
  test('encuentra por koreClienteId y actualiza nombre', async () => {
    const existente = card({ id: 20, koreClienteId: 'K9' });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    const resultado = await procesarClienteKore({
      evento: 'actualizacion', koreClienteId: 'K9', telefono: '5551234567', nombre: 'Nombre Nuevo',
    });

    expect(LoyaltyCard.findOne).toHaveBeenCalledWith({ where: { koreClienteId: 'K9' } });
    expect(existente.update).toHaveBeenCalledWith(expect.objectContaining({
      nombre: 'Nombre Nuevo',
    }));
    expect(resultado).toEqual({ ok: true, evento: 'actualizacion', cardId: 20, creada: false });
  });

  test('no encuentra por koreClienteId, hace fallback por teléfono', async () => {
    const existente = card({ id: 21 });
    LoyaltyCard.findOne
      .mockResolvedValueOnce(null)       // por koreClienteId
      .mockResolvedValueOnce(existente); // fallback por teléfono

    await procesarClienteKore({
      evento: 'actualizacion', koreClienteId: 'K-desconocido', telefono: '5551234567', nombre: 'X',
    });

    expect(LoyaltyCard.findOne).toHaveBeenNthCalledWith(2, { where: { telefono: '5551234567' } });
  });

  test('no encuentra la tarjeta por ningún criterio -> NotFoundError (404), no crea nada', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await expect(
      procesarClienteKore({ evento: 'actualizacion', koreClienteId: 'K-x', telefono: '5551234567', nombre: 'X' }),
    ).rejects.toMatchObject({ statusCode: 404 });
    expect(crearTarjeta).not.toHaveBeenCalled();
  });

  test('teléfono nuevo ya usado por OTRA tarjeta -> ConflictError (409) controlado, no 500', async () => {
    const existente = card({ id: 22, telefono: '5550000000' });
    existente.update.mockRejectedValueOnce(Object.assign(new Error('dup'), { name: 'SequelizeUniqueConstraintError' }));
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    await expect(
      procesarClienteKore({ evento: 'actualizacion', koreClienteId: 'K22', telefono: '5551234567', nombre: 'X' }),
    ).rejects.toMatchObject({ statusCode: 409 });
  });

  test('nunca toca nivel ni saldoPuntos', async () => {
    const existente = card({ id: 23 });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    await procesarClienteKore({ evento: 'actualizacion', koreClienteId: 'K23', telefono: '5551234567', nombre: 'X' });

    const updates = existente.update.mock.calls[0][0];
    expect(updates).not.toHaveProperty('nivel');
    expect(updates).not.toHaveProperty('saldoPuntos');
  });
});

describe('procesarClienteKore — evento baja', () => {
  test('marca activa=false y NO borra el registro', async () => {
    const existente = card({ id: 30, activa: true });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    const resultado = await procesarClienteKore({ evento: 'baja', koreClienteId: 'K30' });

    expect(existente.destroy).toBeUndefined(); // ni siquiera se define un método de borrado en el mock
    expect(existente.update).toHaveBeenCalledWith(expect.objectContaining({ activa: false }));
    expect(resultado).toEqual({ ok: true, evento: 'baja', cardId: 30 });
  });

  test('baja sin telefono (solo koreClienteId) es válida', async () => {
    const existente = card({ id: 31 });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    await expect(procesarClienteKore({ evento: 'baja', koreClienteId: 'K31' })).resolves.toMatchObject({ ok: true });
  });

  test('no encuentra la tarjeta -> NotFoundError (404) controlado', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await expect(
      procesarClienteKore({ evento: 'baja', koreClienteId: 'K-x', telefono: '5551234567' }),
    ).rejects.toMatchObject({ statusCode: 404 });
  });
});
