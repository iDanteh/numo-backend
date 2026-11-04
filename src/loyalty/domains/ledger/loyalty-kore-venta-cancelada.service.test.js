'use strict';

/**
 * loyalty-kore-venta-cancelada.service.test.js — webhook Kore→Numo de cancelación de
 * venta (Pieza 1) + bandeja de auditoría cruzada (Pieza 2). Mismo patrón de mocking que
 * loyalty-kore-venta.service.test.js (modelos planos + sequelize.transaction invocando el
 * callback con una tx falsa) — `./loyalty-ledger.service` NO se mockea: corre real sobre
 * los mismos modelos mockeados, para no reimplementar `_aplicarMovimiento` en un doble.
 */

const { Transaction } = require('sequelize');

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard: { findByPk: jest.fn() },
  LoyaltyPointsLedger: { findOne: jest.fn(), findAll: jest.fn(), create: jest.fn() },
}));
jest.mock('../../../config/database.postgres', () => ({
  sequelize: { transaction: jest.fn() },
}));
jest.mock('./loyalty-venta-fallida.service', () => ({
  registrarVentaFallida: jest.fn(),
}));
jest.mock('../../../banks/domains/collection-requests/CollectionRequest.model', () => ({
  findOne: jest.fn(),
}));
jest.mock('../../../banks/domains/collection-requests/collection-request-asignaciones', () => ({
  movimientosDe: jest.fn(),
}));

const { LoyaltyCard, LoyaltyPointsLedger } = require('../../../shared/models/postgres');
const { sequelize } = require('../../../config/database.postgres');
const { registrarVentaFallida } = require('./loyalty-venta-fallida.service');
const CollectionRequest = require('../../../banks/domains/collection-requests/CollectionRequest.model');
const { movimientosDe } = require('../../../banks/domains/collection-requests/collection-request-asignaciones');
const ventaCanceladaService = require('./loyalty-kore-venta-cancelada.service');

function makeCard(overrides = {}) {
  const card = { id: 1, saldoPuntos: '100.00', activa: true, ...overrides };
  card.update = jest.fn(async (changes) => { Object.assign(card, changes); return card; });
  return card;
}

function filaLedger(overrides = {}) {
  return { cardId: 1, tipo: 'ACUMULACION', puntos: '50.00', referenciaExterna: 'A0-260605320:ACUMULACION', origen: 'kore', ...overrides };
}

beforeEach(() => {
  jest.clearAllMocks();
  sequelize.transaction.mockImplementation(async (cb) => cb({}));
  LoyaltyPointsLedger.create.mockImplementation(async (data) => ({ ...data, saldoResultante: data.saldoResultante ?? '0.00' }));
});

describe('procesarVentaCanceladaKore — validación', () => {
  it('400 si serieFolio no tiene formato SERIE-FOLIO', async () => {
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: 'invalido' }))
      .rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si falta serieFolio', async () => {
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({}))
      .rejects.toMatchObject({ statusCode: 400 });
  });
});

describe('procesarVentaCanceladaKore — idempotencia real', () => {
  it('responde yaRevertida:true sin tocar nada si la fila :REVERSION ya existe', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(filaLedger({ tipo: 'AJUSTE', referenciaExterna: 'A0-260605320:REVERSION' }));
    const res = await ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: 'A0-260605320' });
    expect(res).toEqual({ ok: true, yaRevertida: true });
    expect(LoyaltyPointsLedger.findAll).not.toHaveBeenCalled();
    expect(sequelize.transaction).not.toHaveBeenCalled();
  });
});

describe('procesarVentaCanceladaKore — nada que revertir', () => {
  it('404 si no hay ninguna fila kore para el serieFolio', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([]);
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: 'A0-260605320' }))
      .rejects.toMatchObject({ statusCode: 404 });
  });
});

describe('procesarVentaCanceladaKore — reversión aplicada', () => {
  it('revierte el neto de ACUMULACION+CANJE con una sola fila AJUSTE', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      filaLedger({ tipo: 'ACUMULACION', puntos: '50.00', referenciaExterna: 'A0-260605320:ACUMULACION' }),
      filaLedger({ tipo: 'ACUMULACION', puntos: '30.00', referenciaExterna: 'A0-260605320:PROMOCION' }),
      filaLedger({ tipo: 'CANJE', puntos: '-20.00', referenciaExterna: 'A0-260605320:CANJE' }),
    ]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ saldoPuntos: '200.00' }));

    const res = await ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: 'A0-260605320', motivoCancelacion: 'cliente canceló' });

    // neto = 50 + 30 - 20 = 60 -> se revierte -60.
    expect(res).toEqual({ ok: true, revertido: true, puntosRevertidos: '-60.00', saldoResultante: 140 });
    expect(LoyaltyPointsLedger.create).toHaveBeenCalledWith(expect.objectContaining({
      tipo: 'AJUSTE',
      puntos: '-60.00',
      origen: 'kore',
      referenciaExterna: 'A0-260605320:REVERSION',
      justificacion: expect.stringContaining('cliente canceló'),
    }), expect.anything());
  });

  it('404 si la tarjeta ya no existe', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([filaLedger()]);
    LoyaltyCard.findByPk.mockResolvedValue(null);
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: 'A0-260605320' }))
      .rejects.toMatchObject({ statusCode: 404 });
  });
});

describe('procesarVentaCanceladaKore — saldo insuficiente', () => {
  it('no toca el saldo, registra proceso fallido y responde 200 revertido:false', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      filaLedger({ tipo: 'ACUMULACION', puntos: '50.00' }),
    ]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ saldoPuntos: '10.00' })); // 10 - 50 < 0

    const res = await ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: 'A0-260605320' });

    expect(res).toEqual({ ok: true, revertido: false, motivo: 'saldo_insuficiente' });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
    expect(registrarVentaFallida).toHaveBeenCalledWith(
      'A0-260605320',
      expect.objectContaining({ serieFolio: 'A0-260605320' }),
      expect.stringContaining('Saldo insuficiente'),
    );
  });
});

describe('procesarVentaCanceladaKore — carrera (SequelizeUniqueConstraintError)', () => {
  it('responde yaRevertida:true si otro request concurrente ya insertó la reversión', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([filaLedger()]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard());
    const err = new Error('duplicate'); err.name = 'SequelizeUniqueConstraintError';
    LoyaltyPointsLedger.create.mockRejectedValue(err);

    const res = await ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: 'A0-260605320' });
    expect(res).toEqual({ ok: true, yaRevertida: true });
  });

  it('propaga cualquier otro error inesperado', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([filaLedger()]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard());
    LoyaltyPointsLedger.create.mockRejectedValue(new Error('DB caída'));

    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: 'A0-260605320' }))
      .rejects.toThrow('DB caída');
  });
});

describe('listarVentasCanceladasConDeposito', () => {
  function filaReversion(overrides = {}) {
    return {
      referenciaExterna: 'A0-260605320:REVERSION',
      puntos: '-50.00',
      createdAt: new Date('2026-11-04T12:00:00Z'),
      card: { id: 'card-uuid-1', telefono: '5512345678', nombre: 'Cliente Uno' },
      ...overrides,
    };
  }

  it('vacío si no hay ninguna reversión', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([]);
    const res = await ventaCanceladaService.listarVentasCanceladasConDeposito();
    expect(res).toEqual([]);
    expect(CollectionRequest.findOne).not.toHaveBeenCalled();
  });

  it('excluye una reversión sin CollectionRequest vinculado', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([filaReversion()]);
    CollectionRequest.findOne.mockReturnValue({ sort: jest.fn().mockResolvedValue(null) });
    const res = await ventaCanceladaService.listarVentasCanceladasConDeposito();
    expect(res).toEqual([]);
    expect(movimientosDe).not.toHaveBeenCalled();
  });

  it('excluye una reversión cuyo CollectionRequest no tiene ningún bankMovementId real', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([filaReversion()]);
    const cr = { _id: 'cr1', cxcs: [{ serie: 'A0', folioExterno: '260605320' }] };
    CollectionRequest.findOne.mockReturnValue({ sort: jest.fn().mockResolvedValue(cr) });
    movimientosDe.mockReturnValue([]);
    const res = await ventaCanceladaService.listarVentasCanceladasConDeposito();
    expect(res).toEqual([]);
  });

  it('incluye una reversión con CollectionRequest + bankMovementId real', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([filaReversion()]);
    const cr = { _id: 'cr1', cxcs: [{ serie: 'A0', folioExterno: '260605320' }] };
    CollectionRequest.findOne.mockReturnValue({ sort: jest.fn().mockResolvedValue(cr) });
    movimientosDe.mockReturnValue(['mov1']);

    const res = await ventaCanceladaService.listarVentasCanceladasConDeposito();

    expect(CollectionRequest.findOne).toHaveBeenCalledWith({ 'cxcs.serie': 'A0', 'cxcs.folioExterno': '260605320' });
    expect(res).toEqual([{
      cardId: 'card-uuid-1',
      collectionRequestId: 'cr1',
      serieFolio: 'A0-260605320',
      bankMovementId: 'mov1',
      telefono: '5512345678',
      nombre: 'Cliente Uno',
      puntosRevertidos: '-50.00',
      fecha: filaReversion().createdAt,
    }]);
  });
});
