'use strict';

/**
 * loyalty-kore-congelamiento.service.test.js — webhook Kore→Numo de congelamiento por
 * inactividad (cancelación de puntos + desactivación de tarjeta). Mismo patrón de mocking que
 * loyalty-kore-venta-retiro.service.test.js (modelos planos + sequelize.transaction invocando el
 * callback con una tx falsa) — `./loyalty-ledger.service` NO se mockea: corre real sobre los
 * mismos modelos mockeados, para no reimplementar `_aplicarMovimiento` en un doble.
 * `../cards/loyalty-card-audit.service` SÍ se mockea (es un colaborador externo, mismo criterio
 * que `registrarVentaFallida` en el test de venta-retiro).
 */

const { Transaction } = require('sequelize');

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard: { findOne: jest.fn(), findByPk: jest.fn() },
  LoyaltyPointsLedger: { findOne: jest.fn(), create: jest.fn() },
  LoyaltyAdjustmentReason: { findOne: jest.fn() },
}));
jest.mock('../../../config/database.postgres', () => ({
  sequelize: { transaction: jest.fn() },
}));
jest.mock('../cards/loyalty-card-audit.service', () => ({
  registrarCambioTarjeta: jest.fn(),
}));
jest.mock('../../../banks/shared/socket', () => ({
  emitToLoyaltyCard: jest.fn(),
  emitToAll: jest.fn(),
}));

const {
  LoyaltyCard, LoyaltyPointsLedger, LoyaltyAdjustmentReason,
} = require('../../../shared/models/postgres');
const { sequelize } = require('../../../config/database.postgres');
const { registrarCambioTarjeta } = require('../cards/loyalty-card-audit.service');
const { emitToLoyaltyCard, emitToAll } = require('../../../banks/shared/socket');
const congelamientoService = require('./loyalty-kore-congelamiento.service');

function makeCard(overrides = {}) {
  const card = {
    id: 'card-1', koreClienteId: 'KORE-CLI-001', telefono: '9511771923', saldoPuntos: '350.00', activa: true, ...overrides,
  };
  card.update = jest.fn(async (changes) => { Object.assign(card, changes); return card; });
  return card;
}

const KORE_CLIENTE_ID = 'KORE-CLI-001';
const TELEFONO = '9511771923';
const KORE_EVENTO_ID = 'KORE-EVT-001';

beforeEach(() => {
  jest.clearAllMocks();
  sequelize.transaction.mockImplementation(async (cb) => cb({}));
  LoyaltyPointsLedger.create.mockImplementation(async (data) => ({ ...data, saldoResultante: data.saldoResultante ?? '0.00' }));
  LoyaltyAdjustmentReason.findOne.mockResolvedValue({ id: 1, clave: 'congelamiento_inactividad', activo: true });
});

describe('procesarCongelamientoKore — validación', () => {
  it('400 si evento no es cancelacion_puntos ni desactivacion_tarjeta', async () => {
    await expect(congelamientoService.procesarCongelamientoKore({
      evento: 'otracosa', koreClienteId: KORE_CLIENTE_ID, koreEventoId: KORE_EVENTO_ID, telefono: TELEFONO,
    })).rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si falta koreClienteId', async () => {
    await expect(congelamientoService.procesarCongelamientoKore({
      evento: 'cancelacion_puntos', koreEventoId: KORE_EVENTO_ID, telefono: TELEFONO,
    })).rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si falta koreEventoId', async () => {
    await expect(congelamientoService.procesarCongelamientoKore({
      evento: 'cancelacion_puntos', koreClienteId: KORE_CLIENTE_ID, telefono: TELEFONO,
    })).rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si falta telefono', async () => {
    await expect(congelamientoService.procesarCongelamientoKore({
      evento: 'cancelacion_puntos', koreClienteId: KORE_CLIENTE_ID, koreEventoId: KORE_EVENTO_ID,
    })).rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si telefono tiene formato inválido', async () => {
    await expect(congelamientoService.procesarCongelamientoKore({
      evento: 'cancelacion_puntos', koreClienteId: KORE_CLIENTE_ID, koreEventoId: KORE_EVENTO_ID, telefono: '123',
    })).rejects.toMatchObject({ statusCode: 400 });
  });
});

describe('procesarCongelamientoKore — identidad (telefono obligatorio, no fallback)', () => {
  it('404 si no existe ninguna tarjeta con ese koreClienteId', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyCard.findOne.mockResolvedValue(null);
    await expect(congelamientoService.procesarCongelamientoKore({
      evento: 'cancelacion_puntos', koreClienteId: KORE_CLIENTE_ID, koreEventoId: KORE_EVENTO_ID, telefono: TELEFONO,
    })).rejects.toMatchObject({ statusCode: 404 });
  });

  it('409 si el telefono recibido no coincide con el de la tarjeta', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyCard.findOne.mockResolvedValue(makeCard({ telefono: '5559876543' }));
    await expect(congelamientoService.procesarCongelamientoKore({
      evento: 'cancelacion_puntos', koreClienteId: KORE_CLIENTE_ID, koreEventoId: KORE_EVENTO_ID, telefono: TELEFONO,
    })).rejects.toMatchObject({ statusCode: 409 });
  });
});

describe('procesarCongelamientoKore — cancelacion_puntos', () => {
  it('idempotente: responde yaCancelado:true sin resolver la tarjeta si el koreEventoId ya fue aplicado', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue({ cardId: 'card-1', origen: 'kore', referenciaExterna: `congelamiento:${KORE_EVENTO_ID}` });

    const res = await congelamientoService.procesarCongelamientoKore({
      evento: 'cancelacion_puntos', koreClienteId: KORE_CLIENTE_ID, koreEventoId: KORE_EVENTO_ID, telefono: TELEFONO,
    });

    expect(res).toEqual({
      ok: true, evento: 'cancelacion_puntos', cardId: 'card-1', yaCancelado: true,
    });
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
    expect(sequelize.transaction).not.toHaveBeenCalled();
  });

  it('resetea el saldo a 0 con un AJUSTE real cuando saldoPuntos > 0', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    const card = makeCard({ saldoPuntos: '350.00' });
    LoyaltyCard.findOne.mockResolvedValue(card);
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const res = await congelamientoService.procesarCongelamientoKore({
      evento: 'cancelacion_puntos', koreClienteId: KORE_CLIENTE_ID, koreEventoId: KORE_EVENTO_ID, telefono: TELEFONO, motivo: 'Sin ventas en 6 meses',
    });

    expect(res).toEqual({
      ok: true, evento: 'cancelacion_puntos', cardId: 'card-1', puntosCancelados: '350.00', saldoResultante: 0,
    });
    expect(LoyaltyPointsLedger.create).toHaveBeenCalledWith(expect.objectContaining({
      tipo: 'AJUSTE',
      puntos: '-350.00',
      origen: 'kore',
      referenciaExterna: `congelamiento:${KORE_EVENTO_ID}`,
      reasonId: 1,
      justificacion: expect.stringContaining('Sin ventas en 6 meses'),
    }), { transaction: {} });
    expect(card.saldoPuntos).toBe('0.00');
    expect(emitToLoyaltyCard).toHaveBeenCalledWith('card-1', 'lealtad-tarjeta:actualizada', { cardId: 'card-1', motivo: 'congelamiento-kore' });
  });

  it('no escribe nada si la tarjeta ya está en saldo 0 (yaEnCero)', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    const card = makeCard({ saldoPuntos: '0.00' });
    LoyaltyCard.findOne.mockResolvedValue(card);
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const res = await congelamientoService.procesarCongelamientoKore({
      evento: 'cancelacion_puntos', koreClienteId: KORE_CLIENTE_ID, koreEventoId: KORE_EVENTO_ID, telefono: TELEFONO,
    });

    expect(res).toEqual({
      ok: true, evento: 'cancelacion_puntos', cardId: 'card-1', puntosCancelados: 0, saldoResultante: 0, yaEnCero: true,
    });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
    expect(emitToLoyaltyCard).not.toHaveBeenCalled();
  });

  it('carrera benigna: SequelizeUniqueConstraintError al insertar responde yaCancelado:true', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    const card = makeCard({ saldoPuntos: '100.00' });
    LoyaltyCard.findOne.mockResolvedValue(card);
    LoyaltyCard.findByPk.mockResolvedValue(card);
    const err = new Error('duplicate key'); err.name = 'SequelizeUniqueConstraintError';
    LoyaltyPointsLedger.create.mockRejectedValue(err);

    const res = await congelamientoService.procesarCongelamientoKore({
      evento: 'cancelacion_puntos', koreClienteId: KORE_CLIENTE_ID, koreEventoId: KORE_EVENTO_ID, telefono: TELEFONO,
    });

    expect(res).toEqual({
      ok: true, evento: 'cancelacion_puntos', cardId: 'card-1', yaCancelado: true,
    });
  });
});

describe('procesarCongelamientoKore — desactivacion_tarjeta', () => {
  it('pone activa:false y deja rastro en LoyaltyCardAuditLog', async () => {
    const card = makeCard({ activa: true });
    LoyaltyCard.findOne.mockResolvedValue(card);

    const res = await congelamientoService.procesarCongelamientoKore({
      evento: 'desactivacion_tarjeta', koreClienteId: KORE_CLIENTE_ID, koreEventoId: KORE_EVENTO_ID, telefono: TELEFONO,
    });

    expect(res).toEqual({ ok: true, evento: 'desactivacion_tarjeta', cardId: 'card-1' });
    expect(card.activa).toBe(false);
    expect(registrarCambioTarjeta).toHaveBeenCalledWith(expect.objectContaining({
      cardId: 'card-1', accion: 'webhook_kore_desactivacion', origen: 'kore',
    }));
    expect(emitToAll).toHaveBeenCalledWith('lealtad-cliente-kore:actualizado', { cardId: 'card-1' });
  });

  it('idempotente: responde yaDesactivada:true sin tocar nada si ya estaba inactiva', async () => {
    const card = makeCard({ activa: false });
    LoyaltyCard.findOne.mockResolvedValue(card);

    const res = await congelamientoService.procesarCongelamientoKore({
      evento: 'desactivacion_tarjeta', koreClienteId: KORE_CLIENTE_ID, koreEventoId: KORE_EVENTO_ID, telefono: TELEFONO,
    });

    expect(res).toEqual({
      ok: true, evento: 'desactivacion_tarjeta', cardId: 'card-1', yaDesactivada: true,
    });
    expect(card.update).not.toHaveBeenCalled();
    expect(registrarCambioTarjeta).not.toHaveBeenCalled();
  });
});
