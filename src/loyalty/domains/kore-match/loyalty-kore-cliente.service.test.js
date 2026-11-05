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
  LoyaltyCardAuditLog: { create: jest.fn() },
}));
jest.mock('../cards/loyalty-card.service', () => ({
  crearTarjeta: jest.fn(),
}));

const { LoyaltyCard, LoyaltyCardAuditLog } = require('../../../shared/models/postgres');
const { crearTarjeta } = require('../cards/loyalty-card.service');
const { procesarClienteKore } = require('./loyalty-kore-cliente.service');

function card(overrides = {}) {
  return {
    id: 1, telefono: '5551234567', nombre: 'CLIENTE', koreClienteId: null, koreMonedero: null,
    activa: true, tarjetaLegada: null,
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

  // Bug real 2026-11-05, rediseño pedido por el usuario: `koreClienteId` identifica al CLIENTE en
  // Kore, no al monedero — si elimina su tarjeta y se registra de nuevo con OTRO teléfono, Kore
  // reusa el mismo koreClienteId. En vez de crear una fila nueva (y terminar con 2 registros para
  // el mismo cliente real), un 'alta' con un koreClienteId ya conocido reactiva y actualiza la
  // MISMA fila — mismo saldo/nivel, línea de tiempo continua en LoyaltyCardAuditLog.
  test('alta con koreClienteId de una tarjeta existente (otro teléfono): reactiva la MISMA fila, no crea una nueva', async () => {
    const vieja = card({
      id: 50, telefono: '5550000001', koreClienteId: 'K1', activa: false, nombre: 'CLIENTE',
    });
    LoyaltyCard.findOne.mockResolvedValueOnce(vieja); // encontrada por koreClienteId

    const resultado = await procesarClienteKore({
      evento: 'alta', koreClienteId: 'K1', telefono: '5559999999', nombre: 'Juan Pérez',
    });

    expect(LoyaltyCard.findOne).toHaveBeenCalledWith({ where: { koreClienteId: 'K1' } });
    expect(crearTarjeta).not.toHaveBeenCalled();
    expect(vieja.update).toHaveBeenCalledWith(expect.objectContaining({
      telefono: '5559999999', activa: true,
    }));
    expect(resultado).toEqual({ ok: true, evento: 'actualizacion', cardId: 50, creada: false });
  });

  test('alta con koreClienteId nuevo (ninguna tarjeta lo tiene aún): crea una tarjeta de verdad', async () => {
    LoyaltyCard.findOne
      .mockResolvedValueOnce(null)  // por koreClienteId
      .mockResolvedValueOnce(null); // fallback por teléfono
    const nueva = card({ id: 52, telefono: '5551112222' });
    crearTarjeta.mockResolvedValue({ card: nueva, nip: '1234' });

    const resultado = await procesarClienteKore({
      evento: 'alta', koreClienteId: 'K-nuevo', telefono: '5551112222', nombre: 'Juan Pérez',
    });

    expect(crearTarjeta).toHaveBeenCalled();
    expect(resultado).toEqual({ ok: true, evento: 'alta', cardId: 52, creada: true });
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

  // `activo` (2026-11-03, pedido explícito del usuario): reactiva una tarjeta que un `baja`
  // anterior dejó en activa:false — antes NINGÚN evento podía revertir eso.
  test('activo:true reactiva una tarjeta que estaba dada de baja', async () => {
    const existente = card({ id: 24, activa: false });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    const resultado = await procesarClienteKore({
      evento: 'actualizacion', koreClienteId: 'K24', telefono: '5551234567', nombre: 'X', activo: true,
    });

    expect(existente.update).toHaveBeenCalledWith(expect.objectContaining({ activa: true }));
    expect(resultado).toEqual({ ok: true, evento: 'actualizacion', cardId: 24, creada: false });
  });

  test('activo:false desactiva una tarjeta (mismo efecto que un evento baja, vía actualizacion)', async () => {
    const existente = card({ id: 25, activa: true });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    await procesarClienteKore({
      evento: 'actualizacion', koreClienteId: 'K25', telefono: '5551234567', nombre: 'X', activo: false,
    });

    expect(existente.update).toHaveBeenCalledWith(expect.objectContaining({ activa: false }));
  });

  test('sin activo en el payload -> no toca activa (deja el valor que ya tenía la tarjeta)', async () => {
    const existente = card({ id: 26, activa: false });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    await procesarClienteKore({ evento: 'actualizacion', koreClienteId: 'K26', telefono: '5551234567', nombre: 'X' });

    const updates = existente.update.mock.calls[0][0];
    expect(updates).not.toHaveProperty('activa');
  });

  // Bug real 2026-11-05: Kore manda 'alta' tanto para un cliente nuevo como para uno que había
  // eliminado su tarjeta (evento `baja`) y se registra de nuevo — antes de esta fix, ese segundo
  // caso dejaba la tarjeta reactivada por koreClienteId/nombre pero SIGUIENDO inactiva para
  // siempre, porque 'alta' nunca forzaba `activa:true`.
  test('un alta sobre tarjeta existente (cae a actualización) SIEMPRE la reactiva (activa:true)', async () => {
    const existente = card({ id: 27, activa: false });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    await procesarClienteKore({
      evento: 'alta', koreClienteId: 'K27', telefono: '5551234567', nombre: 'X',
    });

    expect(existente.update).toHaveBeenCalledWith(expect.objectContaining({ activa: true }));
  });

  // `tarjetaLegada` (gap real cerrado): un cliente ya matcheado que cambia de número de
  // tarjeta/monedero en Kore ahora sí se actualiza del lado de Numo vía este evento.
  test('tarjetaLegada: actualiza el número de tarjeta normalizado', async () => {
    const existente = card({ id: 28, tarjetaLegada: '44' });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    const resultado = await procesarClienteKore({
      evento: 'actualizacion', koreClienteId: 'K28', telefono: '5551234567', nombre: 'X', tarjetaLegada: '0098765432',
    });

    // normalizarTarjeta() quita ceros a la izquierda de valores puramente numéricos.
    expect(existente.update).toHaveBeenCalledWith(expect.objectContaining({ tarjetaLegada: '98765432' }));
    expect(resultado).toEqual({ ok: true, evento: 'actualizacion', cardId: 28, creada: false });
  });

  test('tarjetaLegada con formato "CF-": se normaliza igual que en el import/match', async () => {
    const existente = card({ id: 29 });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    await procesarClienteKore({
      evento: 'actualizacion', koreClienteId: 'K29', telefono: '5551234567', nombre: 'X', tarjetaLegada: 'CF 2221',
    });

    expect(existente.update).toHaveBeenCalledWith(expect.objectContaining({ tarjetaLegada: 'CF-2221' }));
  });

  test('sin tarjetaLegada en el payload -> no toca el campo (deja el valor que ya tenía la tarjeta)', async () => {
    const existente = card({ id: 30, tarjetaLegada: '5' });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    await procesarClienteKore({ evento: 'actualizacion', koreClienteId: 'K30', telefono: '5551234567', nombre: 'X' });

    const updates = existente.update.mock.calls[0][0];
    expect(updates).not.toHaveProperty('tarjetaLegada');
  });

  test('tarjetaLegada vacío -> no borra el valor existente', async () => {
    const existente = card({ id: 31, tarjetaLegada: '5' });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    await procesarClienteKore({
      evento: 'actualizacion', koreClienteId: 'K31', telefono: '5551234567', nombre: 'X', tarjetaLegada: '   ',
    });

    const updates = existente.update.mock.calls[0][0];
    expect(updates).not.toHaveProperty('tarjetaLegada');
  });

  test('un alta sobre tarjeta existente (cae a actualización) NUNCA toca tarjetaLegada, aunque el payload lo traiga', async () => {
    const existente = card({ id: 32, tarjetaLegada: '7' });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    // 'alta' no desestructura `tarjetaLegada` del payload — mismo criterio que `activo`.
    await procesarClienteKore({
      evento: 'alta', koreClienteId: 'K32', telefono: '5551234567', nombre: 'X', tarjetaLegada: '999',
    });

    const updates = existente.update.mock.calls[0][0];
    expect(updates).not.toHaveProperty('tarjetaLegada');
  });

  // Historial de cambios de catálogo (gap real cerrado 2026-11-04).
  test('registra en LoyaltyCardAuditLog el teléfono/tarjetaLegada anteriores y nuevos', async () => {
    const existente = card({
      id: 33, telefono: '5551234567', tarjetaLegada: '5', koreClienteId: 'K-viejo',
    });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    await procesarClienteKore({
      evento: 'actualizacion', koreClienteId: 'K33', telefono: '5559999999', nombre: 'X', tarjetaLegada: '0099',
    });

    expect(LoyaltyCardAuditLog.create).toHaveBeenCalledWith(expect.objectContaining({
      cardId: 33,
      accion: 'webhook_kore_actualizacion',
      origen: 'kore',
      cambios: {
        koreClienteId: { antes: 'K-viejo', despues: 'K33' },
        nombre: { antes: 'CLIENTE', despues: 'X' },
        telefono: { antes: '5551234567', despues: '5559999999' },
        tarjetaLegada: { antes: '5', despues: '99' },
      },
    }));
  });

  test('si nada cambió de verdad (Kore reenvía los mismos valores), no registra nada en el historial', async () => {
    const existente = card({
      id: 34, telefono: '5551234567', nombre: 'CLIENTE', koreClienteId: 'K34', tarjetaLegada: '5',
    });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    await procesarClienteKore({
      evento: 'actualizacion', koreClienteId: 'K34', telefono: '5551234567', nombre: 'CLIENTE', tarjetaLegada: '5',
    });

    expect(LoyaltyCardAuditLog.create).not.toHaveBeenCalled();
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

  test('registra la baja en LoyaltyCardAuditLog', async () => {
    const existente = card({ id: 35, activa: true });
    LoyaltyCard.findOne.mockResolvedValueOnce(existente);

    await procesarClienteKore({ evento: 'baja', koreClienteId: 'K35' });

    expect(LoyaltyCardAuditLog.create).toHaveBeenCalledWith(expect.objectContaining({
      cardId: 35,
      accion: 'webhook_kore_baja',
      origen: 'kore',
      cambios: { activa: { antes: true, despues: false } },
    }));
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
