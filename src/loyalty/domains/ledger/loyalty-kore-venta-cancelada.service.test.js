'use strict';

/**
 * loyalty-kore-venta-cancelada.service.test.js — webhook Kore→Numo de cancelación de
 * venta, total Y parcial (Pieza 1) + bandeja de auditoría cruzada (Pieza 2). Mismo patrón
 * de mocking que loyalty-kore-venta.service.test.js (modelos planos + sequelize.transaction
 * invocando el callback con una tx falsa) — `./loyalty-ledger.service` NO se mockea: corre
 * real sobre los mismos modelos mockeados, para no reimplementar `_aplicarMovimiento` en un
 * doble.
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
// Notificación en vivo (2026-11-10) — ver mismo criterio en loyalty-kore-venta.service.test.js.
jest.mock('../../../banks/shared/socket', () => ({ emitToLoyaltyCard: jest.fn() }));

const { LoyaltyCard, LoyaltyPointsLedger } = require('../../../shared/models/postgres');
const { sequelize } = require('../../../config/database.postgres');
const { registrarVentaFallida } = require('./loyalty-venta-fallida.service');
const CollectionRequest = require('../../../banks/domains/collection-requests/CollectionRequest.model');
const { movimientosDe } = require('../../../banks/domains/collection-requests/collection-request-asignaciones');
const { emitToLoyaltyCard } = require('../../../banks/shared/socket');
const ventaCanceladaService = require('./loyalty-kore-venta-cancelada.service');

function makeCard(overrides = {}) {
  const card = { id: 1, saldoPuntos: '100.00', activa: true, ...overrides };
  card.update = jest.fn(async (changes) => { Object.assign(card, changes); return card; });
  return card;
}

function filaLedger(overrides = {}) {
  return {
    cardId: 1, tipo: 'ACUMULACION', puntos: '50.00', referenciaExterna: 'A0-260605320:ACUMULACION', origen: 'kore', ...overrides,
  };
}

const SF = 'A0-260605320';
const SFC1 = 'A0-900000001';
const SFC2 = 'A0-900000002';

beforeEach(() => {
  jest.clearAllMocks();
  sequelize.transaction.mockImplementation(async (cb) => cb({}));
  LoyaltyPointsLedger.create.mockImplementation(async (data) => ({ ...data, saldoResultante: data.saldoResultante ?? '0.00' }));
});

describe('procesarVentaCanceladaKore — validación', () => {
  it('400 si falta o serieFolio tiene formato inválido', async () => {
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolioCancelacion: SFC1, tipoCancelacion: 'total' }))
      .rejects.toMatchObject({ statusCode: 400 });
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: 'invalido', serieFolioCancelacion: SFC1, tipoCancelacion: 'total' }))
      .rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si falta o serieFolioCancelacion tiene formato inválido', async () => {
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, tipoCancelacion: 'total' }))
      .rejects.toMatchObject({ statusCode: 400 });
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: 'invalido', tipoCancelacion: 'total' }))
      .rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si tipoCancelacion no es "total" ni "parcial"', async () => {
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'otracosa' }))
      .rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si tipoCancelacion es "parcial" sin importeCancelado o con importeCancelado <= 0', async () => {
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'parcial' }))
      .rejects.toMatchObject({ statusCode: 400 });
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({
      serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'parcial', importeCancelado: 0,
    })).rejects.toMatchObject({ statusCode: 400 });
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({
      serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'parcial', importeCancelado: -5,
    })).rejects.toMatchObject({ statusCode: 400 });
  });
});

describe('procesarVentaCanceladaKore — idempotencia real', () => {
  it('responde yaRevertida:true sin tocar nada si la fila :REVERSION:<serieFolioCancelacion> ya existe', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(filaLedger({ tipo: 'AJUSTE', referenciaExterna: `${SF}:REVERSION:${SFC1}` }));
    const res = await ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'total' });
    expect(res).toEqual({ ok: true, yaRevertida: true });
    expect(LoyaltyPointsLedger.findOne).toHaveBeenCalledWith({ where: { origen: 'kore', referenciaExterna: `${SF}:REVERSION:${SFC1}` } });
    expect(LoyaltyPointsLedger.findAll).not.toHaveBeenCalled();
    expect(sequelize.transaction).not.toHaveBeenCalled();
  });
});

describe('procesarVentaCanceladaKore — nada que revertir', () => {
  it('404 si no hay ninguna fila original kore para el serieFolio', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([]);
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'total' }))
      .rejects.toMatchObject({ statusCode: 404 });
  });
});

describe('procesarVentaCanceladaKore — total sin parciales previas', () => {
  it('revierte el neto de ACUMULACION+PROMOCION+CANJE con una sola fila AJUSTE', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      filaLedger({ tipo: 'ACUMULACION', puntos: '50.00', referenciaExterna: `${SF}:ACUMULACION`, montoVenta: '1000.00' }),
      filaLedger({ tipo: 'ACUMULACION', puntos: '30.00', referenciaExterna: `${SF}:PROMOCION` }),
      filaLedger({ tipo: 'CANJE', puntos: '-20.00', referenciaExterna: `${SF}:CANJE` }),
    ]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ saldoPuntos: '200.00' }));

    const res = await ventaCanceladaService.procesarVentaCanceladaKore({
      serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'total', motivoCancelacion: 'cliente canceló',
    });

    // neto = 50 + 30 - 20 = 60 -> se revierte -60.
    expect(res).toEqual({
      ok: true, revertido: true, tipoCancelacion: 'total', puntosRevertidos: '-60.00', saldoResultante: 140,
    });
    expect(LoyaltyPointsLedger.create).toHaveBeenCalledWith(expect.objectContaining({
      tipo: 'AJUSTE',
      puntos: '-60.00',
      origen: 'kore',
      referenciaExterna: `${SF}:REVERSION:${SFC1}`,
      justificacion: expect.stringContaining('cliente canceló'),
    }), expect.anything());
  });

  it('404 si la tarjeta ya no existe', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([filaLedger()]);
    LoyaltyCard.findByPk.mockResolvedValue(null);
    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'total' }))
      .rejects.toMatchObject({ statusCode: 404 });
  });
});

describe('procesarVentaCanceladaKore — parcial', () => {
  it('calcula la proporción usando el montoVenta de la fila ACUMULACION guardada ($1000 venta, 50 pts, cancela $200 -> 10 pts)', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      filaLedger({ tipo: 'ACUMULACION', puntos: '50.00', referenciaExterna: `${SF}:ACUMULACION`, montoVenta: '1000.00' }),
    ]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ saldoPuntos: '200.00' }));

    const res = await ventaCanceladaService.procesarVentaCanceladaKore({
      serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'parcial', importeCancelado: 200,
    });

    expect(res).toEqual({
      ok: true, revertido: true, tipoCancelacion: 'parcial', puntosRevertidos: '-10.00', saldoResultante: 190,
    });
    expect(LoyaltyPointsLedger.create).toHaveBeenCalledWith(expect.objectContaining({
      puntos: '-10.00',
      referenciaExterna: `${SF}:REVERSION:${SFC1}`,
    }), expect.anything());
  });

  it('usa el montoVenta del body cuando no hay fila ACUMULACION guardada (venta 100% CANJE)', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      filaLedger({ tipo: 'CANJE', puntos: '-40.00', referenciaExterna: `${SF}:CANJE` }),
    ]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ saldoPuntos: '10.00' }));

    // venta de $400, neto = -40, cancela $200 (mitad) -> revierte +20.
    const res = await ventaCanceladaService.procesarVentaCanceladaKore({
      serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'parcial', importeCancelado: 200, montoVenta: 400,
    });

    expect(res).toEqual({
      ok: true, revertido: true, tipoCancelacion: 'parcial', puntosRevertidos: '20.00', saldoResultante: 30,
    });
  });

  it('409 si el montoVenta del body no coincide (fuera de tolerancia) con el guardado', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      filaLedger({ tipo: 'ACUMULACION', puntos: '50.00', referenciaExterna: `${SF}:ACUMULACION`, montoVenta: '1000.00' }),
    ]);

    await expect(ventaCanceladaService.procesarVentaCanceladaKore({
      serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'parcial', importeCancelado: 200, montoVenta: 500,
    })).rejects.toMatchObject({ statusCode: 409 });
    expect(LoyaltyCard.findByPk).not.toHaveBeenCalled();
  });

  it('400 si no se puede resolver montoVentaOriginal de ninguna fuente', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      filaLedger({ tipo: 'CANJE', puntos: '-40.00', referenciaExterna: `${SF}:CANJE` }),
    ]);

    await expect(ventaCanceladaService.procesarVentaCanceladaKore({
      serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'parcial', importeCancelado: 200,
    })).rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si la suma de cancelaciones (previas + esta) excede el montoVenta original', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      filaLedger({ tipo: 'ACUMULACION', puntos: '50.00', referenciaExterna: `${SF}:ACUMULACION`, montoVenta: '1000.00' }),
      // Reversión previa que ya canceló $900 (90% de 50 = 45 pts revertidos, signo opuesto -45).
      filaLedger({ tipo: 'AJUSTE', puntos: '-45.00', referenciaExterna: `${SF}:REVERSION:${SFC1}` }),
    ]);

    await expect(ventaCanceladaService.procesarVentaCanceladaKore({
      serieFolio: SF, serieFolioCancelacion: SFC2, tipoCancelacion: 'parcial', importeCancelado: 200,
    })).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyCard.findByPk).not.toHaveBeenCalled();
  });

  it('dos cancelaciones parciales consecutivas: la segunda se calcula correctamente sin pisar la primera', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    // Venta de $1000 -> 50 pts. Primera parcial ya canceló $200 -> revirtió -10.
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      filaLedger({ tipo: 'ACUMULACION', puntos: '50.00', referenciaExterna: `${SF}:ACUMULACION`, montoVenta: '1000.00' }),
      filaLedger({ tipo: 'AJUSTE', puntos: '-10.00', referenciaExterna: `${SF}:REVERSION:${SFC1}` }),
    ]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ saldoPuntos: '40.00' }));

    // Segunda parcial: cancela otros $300 (30% de 1000) -> revierte 15 pts más.
    const res = await ventaCanceladaService.procesarVentaCanceladaKore({
      serieFolio: SF, serieFolioCancelacion: SFC2, tipoCancelacion: 'parcial', importeCancelado: 300,
    });

    expect(res).toEqual({
      ok: true, revertido: true, tipoCancelacion: 'parcial', puntosRevertidos: '-15.00', saldoResultante: 25,
    });
    expect(LoyaltyPointsLedger.create).toHaveBeenCalledWith(expect.objectContaining({
      puntos: '-15.00',
      referenciaExterna: `${SF}:REVERSION:${SFC2}`,
    }), expect.anything());
  });

  it('cancelación total después de una parcial previa revierte solo el remanente', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    // neto original = 60 (ACUMULACION 50 + CANJE -(-10)... usamos un caso simple: ACUMULACION 60).
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      filaLedger({ tipo: 'ACUMULACION', puntos: '60.00', referenciaExterna: `${SF}:ACUMULACION`, montoVenta: '1000.00' }),
      filaLedger({ tipo: 'AJUSTE', puntos: '-10.00', referenciaExterna: `${SF}:REVERSION:${SFC1}` }),
    ]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ saldoPuntos: '50.00' }));

    const res = await ventaCanceladaService.procesarVentaCanceladaKore({
      serieFolio: SF, serieFolioCancelacion: SFC2, tipoCancelacion: 'total',
    });

    // remanente = 60 - 10 = 50 -> revierte -50 (no vuelve a tocar los 10 ya revertidos).
    expect(res).toEqual({
      ok: true, revertido: true, tipoCancelacion: 'total', puntosRevertidos: '-50.00', saldoResultante: 0,
    });
  });
});

describe('procesarVentaCanceladaKore — saldo insuficiente', () => {
  it('no toca el saldo, registra proceso fallido y responde 200 revertido:false', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      filaLedger({ tipo: 'ACUMULACION', puntos: '50.00' }),
    ]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ saldoPuntos: '10.00' })); // 10 - 50 < 0

    const res = await ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'total' });

    expect(res).toEqual({ ok: true, revertido: false, motivo: 'saldo_insuficiente' });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
    expect(registrarVentaFallida).toHaveBeenCalledWith(
      SF,
      expect.objectContaining({ serieFolio: SF, serieFolioCancelacion: SFC1 }),
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

    const res = await ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'total' });
    expect(res).toEqual({ ok: true, yaRevertida: true });
  });

  it('propaga cualquier otro error inesperado', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([filaLedger()]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard());
    LoyaltyPointsLedger.create.mockRejectedValue(new Error('DB caída'));

    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'total' }))
      .rejects.toThrow('DB caída');
  });
});

// Notificación en vivo (2026-11-10, pedido explícito del usuario): a la sala de la tarjeta
// puntual, nunca emitToAll (ver docstring del archivo). Se emite incluso en los caminos
// idempotentes/carrera (inofensivo), pero NUNCA si no se escribió ningún movimiento real.
describe('procesarVentaCanceladaKore — notificación en vivo (emitToLoyaltyCard)', () => {
  it('ya revertida (idempotente): emite con el cardId de la fila ya existente', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(filaLedger({
      cardId: 5, tipo: 'AJUSTE', referenciaExterna: `${SF}:REVERSION:${SFC1}`,
    }));

    await ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'total' });

    expect(emitToLoyaltyCard).toHaveBeenCalledWith(5, 'lealtad-tarjeta:actualizada', { cardId: 5, motivo: 'cancelacion-kore' });
  });

  it('cancelación total aplicada con éxito: emite con el cardId de la venta original', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([filaLedger({ cardId: 9, tipo: 'ACUMULACION', puntos: '50.00' })]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ id: 9, saldoPuntos: '200.00' }));

    await ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'total' });

    expect(emitToLoyaltyCard).toHaveBeenCalledWith(9, 'lealtad-tarjeta:actualizada', { cardId: 9, motivo: 'cancelacion-kore' });
  });

  it('saldo insuficiente: NO emite (no se escribió ningún movimiento real)', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([filaLedger({ tipo: 'ACUMULACION', puntos: '50.00' })]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ saldoPuntos: '10.00' }));

    await ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'total' });

    expect(emitToLoyaltyCard).not.toHaveBeenCalled();
  });

  it('carrera (SequelizeUniqueConstraintError): emite igual, es inofensivo', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([filaLedger({ cardId: 3 })]);
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ id: 3 }));
    const err = new Error('duplicate'); err.name = 'SequelizeUniqueConstraintError';
    LoyaltyPointsLedger.create.mockRejectedValue(err);

    await ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'total' });

    expect(emitToLoyaltyCard).toHaveBeenCalledWith(3, 'lealtad-tarjeta:actualizada', { cardId: 3, motivo: 'cancelacion-kore' });
  });

  it('404 (no hay venta original que revertir): nunca emite', async () => {
    LoyaltyPointsLedger.findOne.mockResolvedValue(null);
    LoyaltyPointsLedger.findAll.mockResolvedValue([]);

    await expect(ventaCanceladaService.procesarVentaCanceladaKore({ serieFolio: SF, serieFolioCancelacion: SFC1, tipoCancelacion: 'total' }))
      .rejects.toMatchObject({ statusCode: 404 });
    expect(emitToLoyaltyCard).not.toHaveBeenCalled();
  });
});

describe('listarVentasCanceladasConDeposito', () => {
  function filaReversion(overrides = {}) {
    return {
      referenciaExterna: `${SF}:REVERSION:${SFC1}`,
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

  it('incluye una reversión con CollectionRequest + bankMovementId real, con serieFolioCancelacion', async () => {
    LoyaltyPointsLedger.findAll.mockResolvedValue([filaReversion()]);
    const cr = { _id: 'cr1', cxcs: [{ serie: 'A0', folioExterno: '260605320' }] };
    CollectionRequest.findOne.mockReturnValue({ sort: jest.fn().mockResolvedValue(cr) });
    movimientosDe.mockReturnValue(['mov1']);

    const res = await ventaCanceladaService.listarVentasCanceladasConDeposito();

    expect(LoyaltyPointsLedger.findAll).toHaveBeenCalledWith(expect.objectContaining({
      where: expect.objectContaining({ referenciaExterna: { [require('sequelize').Op.like]: '%:REVERSION:%' } }),
    }));
    expect(CollectionRequest.findOne).toHaveBeenCalledWith({ 'cxcs.serie': 'A0', 'cxcs.folioExterno': '260605320' });
    expect(res).toEqual([{
      cardId: 'card-uuid-1',
      collectionRequestId: 'cr1',
      serieFolio: SF,
      serieFolioCancelacion: SFC1,
      bankMovementId: 'mov1',
      telefono: '5512345678',
      nombre: 'Cliente Uno',
      puntosRevertidos: '-50.00',
      fecha: filaReversion().createdAt,
    }]);
  });
});
