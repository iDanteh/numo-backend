'use strict';

/**
 * loyalty-kore-nivel.service.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Webhook Kore→Numo de catálogo de niveles (2026-11-06). `LoyaltyKoreNivel` y
 * `_buscarOAdoptarNivelKore` (reusada de loyalty-config.service.js, mismo
 * criterio de adopción por nombre que el sync) mockeados.
 */

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyKoreNivel: { findOne: jest.fn(), create: jest.fn() },
}));
jest.mock('../config/loyalty-config.service', () => ({
  _buscarOAdoptarNivelKore: jest.fn(),
}));
// Notificación en vivo (2026-11-10) — ver mismo criterio en loyalty-kore-cliente.service.test.js.
jest.mock('../../../banks/shared/socket', () => ({ emitToAll: jest.fn() }));

const { LoyaltyKoreNivel } = require('../../../shared/models/postgres');
const { _buscarOAdoptarNivelKore } = require('../config/loyalty-config.service');
const { emitToAll } = require('../../../banks/shared/socket');
const { procesarNivelKore } = require('./loyalty-kore-nivel.service');

function nivel(overrides = {}) {
  return {
    id: 1, koreId: 'n1', nombre: 'Bronce', porcentaje: 1.5, esInicial: false, activo: true,
    update: jest.fn(async function update(changes) { Object.assign(this, changes); }),
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('procesarNivelKore — validación de sobre', () => {
  test('evento inválido -> 400', async () => {
    await expect(procesarNivelKore({ evento: 'foo', koreId: 'n1' }))
      .rejects.toMatchObject({ statusCode: 400 });
  });

  test('sin koreId -> 400', async () => {
    await expect(procesarNivelKore({ evento: 'alta', nombre: 'Bronce', porcentaje: 1 }))
      .rejects.toMatchObject({ statusCode: 400 });
  });
});

describe('evento alta', () => {
  test('nivel nuevo (sin fila transicional): lo crea con esInicial:false', async () => {
    _buscarOAdoptarNivelKore.mockResolvedValue(null);
    LoyaltyKoreNivel.create.mockResolvedValue(nivel({ id: 9 }));

    const resultado = await procesarNivelKore({
      evento: 'alta', koreId: 'n9', nombre: 'Platino', porcentaje: 3, estatus: true,
    });

    expect(LoyaltyKoreNivel.create).toHaveBeenCalledWith({
      koreId: 'n9', nombre: 'Platino', porcentaje: 3, activo: true, esInicial: false,
    });
    expect(resultado).toEqual({ ok: true, evento: 'alta', nivelId: 9, creado: true });
  });

  test('adopta una fila transicional (koreId:null) por nombre en vez de duplicarla', async () => {
    const transicional = nivel({ id: 5, koreId: null, nombre: 'Bronce', porcentaje: null });
    _buscarOAdoptarNivelKore.mockResolvedValue(transicional);

    const resultado = await procesarNivelKore({
      evento: 'alta', koreId: 'n1', nombre: 'Bronce', porcentaje: 1.5, estatus: true,
    });

    expect(transicional.update).toHaveBeenCalledWith({ koreId: 'n1', nombre: 'Bronce', porcentaje: 1.5, activo: true });
    expect(LoyaltyKoreNivel.create).not.toHaveBeenCalled();
    expect(resultado).toEqual({ ok: true, evento: 'alta', nivelId: 5, creado: false });
  });

  test('sin nombre -> 400, no crea nada', async () => {
    await expect(procesarNivelKore({
      evento: 'alta', koreId: 'n1', porcentaje: 1, estatus: true,
    }))
      .rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyKoreNivel.create).not.toHaveBeenCalled();
  });

  test('sin porcentaje -> 400, no crea nada', async () => {
    await expect(procesarNivelKore({
      evento: 'alta', koreId: 'n1', nombre: 'Bronce', estatus: true,
    }))
      .rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyKoreNivel.create).not.toHaveBeenCalled();
  });

  test('porcentaje <= 0 -> 400', async () => {
    await expect(procesarNivelKore({
      evento: 'alta', koreId: 'n1', nombre: 'Bronce', porcentaje: 0, estatus: true,
    }))
      .rejects.toMatchObject({ statusCode: 400 });
  });

  test('estatus:false crea el nivel ya inactivo', async () => {
    _buscarOAdoptarNivelKore.mockResolvedValue(null);
    LoyaltyKoreNivel.create.mockResolvedValue(nivel({ id: 9 }));

    await procesarNivelKore({
      evento: 'alta', koreId: 'n9', nombre: 'Platino', porcentaje: 3, estatus: false,
    });

    expect(LoyaltyKoreNivel.create).toHaveBeenCalledWith({
      koreId: 'n9', nombre: 'Platino', porcentaje: 3, activo: false, esInicial: false,
    });
  });

  test('sin estatus -> 400, no crea nada', async () => {
    await expect(procesarNivelKore({
      evento: 'alta', koreId: 'n9', nombre: 'Platino', porcentaje: 3,
    }))
      .rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyKoreNivel.create).not.toHaveBeenCalled();
  });

  test('carrera de índice único en create: recupera la fila ganadora y actualiza en vez de fallar', async () => {
    _buscarOAdoptarNivelKore.mockResolvedValue(null);
    const err = new Error('duplicate');
    err.name = 'SequelizeUniqueConstraintError';
    LoyaltyKoreNivel.create.mockRejectedValue(err);
    const ganador = nivel({ id: 7 });
    LoyaltyKoreNivel.findOne.mockResolvedValue(ganador);

    const resultado = await procesarNivelKore({
      evento: 'alta', koreId: 'n1', nombre: 'Bronce', porcentaje: 1.5, estatus: true,
    });

    expect(ganador.update).toHaveBeenCalledWith({ koreId: 'n1', nombre: 'Bronce', porcentaje: 1.5, activo: true });
    expect(resultado).toEqual({ ok: true, evento: 'alta', nivelId: 7, creado: false });
  });
});

describe('evento actualizacion', () => {
  test('ajusta nombre y porcentaje por koreId', async () => {
    const existente = nivel();
    LoyaltyKoreNivel.findOne.mockResolvedValue(existente);

    const resultado = await procesarNivelKore({
      evento: 'actualizacion', koreId: 'n1', nombre: 'Bronce Plus', porcentaje: 2.25, estatus: true,
    });

    expect(existente.update).toHaveBeenCalledWith({ nombre: 'Bronce Plus', porcentaje: 2.25, activo: true });
    expect(resultado).toEqual({ ok: true, evento: 'actualizacion', nivelId: 1 });
  });

  test('campos omitidos: no se tocan (mismo criterio que el webhook de clientes)', async () => {
    const existente = nivel();
    LoyaltyKoreNivel.findOne.mockResolvedValue(existente);

    await procesarNivelKore({ evento: 'actualizacion', koreId: 'n1', estatus: true });

    expect(existente.update).toHaveBeenCalledWith({ activo: true });
  });

  test('porcentaje <= 0 -> 400, no actualiza', async () => {
    const existente = nivel();
    LoyaltyKoreNivel.findOne.mockResolvedValue(existente);

    await expect(procesarNivelKore({
      evento: 'actualizacion', koreId: 'n1', porcentaje: -1, estatus: true,
    }))
      .rejects.toMatchObject({ statusCode: 400 });
    expect(existente.update).not.toHaveBeenCalled();
  });

  test('koreId no encontrado -> 404', async () => {
    LoyaltyKoreNivel.findOne.mockResolvedValue(null);
    await expect(procesarNivelKore({
      evento: 'actualizacion', koreId: 'n404', nombre: 'X', estatus: true,
    }))
      .rejects.toMatchObject({ statusCode: 404 });
  });

  test('estatus:false desactiva', async () => {
    const existente = nivel();
    LoyaltyKoreNivel.findOne.mockResolvedValue(existente);

    await procesarNivelKore({ evento: 'actualizacion', koreId: 'n1', estatus: false });

    expect(existente.update).toHaveBeenCalledWith({ activo: false });
  });

  test('estatus:true reactiva', async () => {
    const existente = nivel({ activo: false });
    LoyaltyKoreNivel.findOne.mockResolvedValue(existente);

    await procesarNivelKore({ evento: 'actualizacion', koreId: 'n1', estatus: true });

    expect(existente.update).toHaveBeenCalledWith({ activo: true });
  });

  test('sin estatus -> 400, no actualiza', async () => {
    const existente = nivel();
    LoyaltyKoreNivel.findOne.mockResolvedValue(existente);

    await expect(procesarNivelKore({ evento: 'actualizacion', koreId: 'n1', nombre: 'Bronce Plus' }))
      .rejects.toMatchObject({ statusCode: 400 });
    expect(existente.update).not.toHaveBeenCalled();
  });
});

describe('evento baja', () => {
  test('desactiva por koreId, nunca borra la fila', async () => {
    const existente = nivel();
    LoyaltyKoreNivel.findOne.mockResolvedValue(existente);

    const resultado = await procesarNivelKore({ evento: 'baja', koreId: 'n1' });

    expect(existente.update).toHaveBeenCalledWith({ activo: false });
    expect(resultado).toEqual({ ok: true, evento: 'baja', nivelId: 1 });
  });

  test('koreId no encontrado -> 404', async () => {
    LoyaltyKoreNivel.findOne.mockResolvedValue(null);
    await expect(procesarNivelKore({ evento: 'baja', koreId: 'n404' }))
      .rejects.toMatchObject({ statusCode: 404 });
  });
});

// Notificación en vivo (2026-11-10, pedido explícito del usuario — "relevante para el registro
// de acumulaciones manuales"): broadcast GLOBAL tras alta/actualización/baja con éxito, alimenta
// la tabla "Niveles Kore" de Configuración de Lealtad.
describe('procesarNivelKore — notificación en vivo (emitToAll)', () => {
  test('alta: emite lealtad-niveles-kore:actualizado con el nivelId creado', async () => {
    _buscarOAdoptarNivelKore.mockResolvedValue(null);
    LoyaltyKoreNivel.create.mockResolvedValue(nivel({ id: 70 }));

    await procesarNivelKore({
      evento: 'alta', koreId: 'n70', nombre: 'Platino', porcentaje: 3, estatus: true,
    });

    expect(emitToAll).toHaveBeenCalledWith('lealtad-niveles-kore:actualizado', { nivelId: 70 });
  });

  test('actualizacion: emite con el nivelId existente', async () => {
    const existente = nivel({ id: 71 });
    LoyaltyKoreNivel.findOne.mockResolvedValue(existente);

    await procesarNivelKore({ evento: 'actualizacion', koreId: 'n71', estatus: false });

    expect(emitToAll).toHaveBeenCalledWith('lealtad-niveles-kore:actualizado', { nivelId: 71 });
  });

  test('baja: emite con el nivelId desactivado', async () => {
    const existente = nivel({ id: 72 });
    LoyaltyKoreNivel.findOne.mockResolvedValue(existente);

    await procesarNivelKore({ evento: 'baja', koreId: 'n72' });

    expect(emitToAll).toHaveBeenCalledWith('lealtad-niveles-kore:actualizado', { nivelId: 72 });
  });

  test('error de validación (400): nunca emite', async () => {
    await expect(procesarNivelKore({ evento: 'foo', koreId: 'n1' }))
      .rejects.toMatchObject({ statusCode: 400 });

    expect(emitToAll).not.toHaveBeenCalled();
  });
});
