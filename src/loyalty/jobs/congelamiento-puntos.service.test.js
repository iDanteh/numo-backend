'use strict';

/**
 * jobs/congelamiento-puntos.service.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — "Congelamiento de puntos" por inactividad (2026-11-03).
 * Mismo patrón de mocking que loyalty-ledger.service.test.js: modelos y
 * servicios de dominio mockeados completos, sin Postgres real.
 */

jest.mock('../../shared/models/postgres', () => ({
  LoyaltyCard:             { findAll: jest.fn() },
  LoyaltyPointsLedger:     { findAll: jest.fn() },
  LoyaltyAdjustmentReason: { findOne: jest.fn() },
}));
jest.mock('../domains/config/loyalty-config.service', () => ({
  obtenerCongelamientoMeses: jest.fn(),
}));
jest.mock('../domains/ledger/loyalty-ledger.service', () => ({
  registrarAjuste: jest.fn(),
}));

const { LoyaltyCard, LoyaltyPointsLedger, LoyaltyAdjustmentReason } = require('../../shared/models/postgres');
const configService = require('../domains/config/loyalty-config.service');
const ledgerService = require('../domains/ledger/loyalty-ledger.service');
const { congelarTarjetasInactivas } = require('./congelamiento-puntos.service');

const REASON = { id: 99, clave: 'congelamiento_inactividad', activo: true };

function haceMeses(meses) {
  const d = new Date();
  d.setMonth(d.getMonth() - meses);
  return d;
}

beforeEach(() => {
  jest.clearAllMocks();
  LoyaltyAdjustmentReason.findOne.mockResolvedValue(REASON);
  ledgerService.registrarAjuste.mockResolvedValue({ movimiento: {}, saldoPuntos: '0.00' });
});

describe('congelarTarjetasInactivas', () => {
  test('config deshabilitada/no seteada (null) → no hace nada', async () => {
    configService.obtenerCongelamientoMeses.mockResolvedValue(null);

    const resultado = await congelarTarjetasInactivas();

    expect(resultado).toMatchObject({ deshabilitado: true, congeladas: 0 });
    expect(LoyaltyCard.findAll).not.toHaveBeenCalled();
    expect(ledgerService.registrarAjuste).not.toHaveBeenCalled();
  });

  test('tarjeta sin ninguna ACUMULACION registrada nunca → no se toca', async () => {
    configService.obtenerCongelamientoMeses.mockResolvedValue(3);
    LoyaltyCard.findAll.mockResolvedValue([{ id: 1, saldoPuntos: '150.00' }]);
    LoyaltyPointsLedger.findAll.mockResolvedValue([]); // sin fila agregada para esa tarjeta

    const resultado = await congelarTarjetasInactivas();

    expect(ledgerService.registrarAjuste).not.toHaveBeenCalled();
    expect(resultado.congeladas).toBe(0);
  });

  test('tarjeta con acumulación reciente (dentro del umbral) → no se toca', async () => {
    configService.obtenerCongelamientoMeses.mockResolvedValue(3);
    LoyaltyCard.findAll.mockResolvedValue([{ id: 2, saldoPuntos: '200.00' }]);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      { cardId: 2, ultimaAcumulacion: haceMeses(1) }, // 1 mes, dentro del umbral de 3
    ]);

    const resultado = await congelarTarjetasInactivas();

    expect(ledgerService.registrarAjuste).not.toHaveBeenCalled();
    expect(resultado.congeladas).toBe(0);
  });

  test('tarjeta con acumulación vieja (fuera del umbral) y saldo>0 → se resetea a 0 con el motivo correcto', async () => {
    configService.obtenerCongelamientoMeses.mockResolvedValue(3);
    LoyaltyCard.findAll.mockResolvedValue([{ id: 3, saldoPuntos: '350.00' }]);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      { cardId: 3, ultimaAcumulacion: haceMeses(4) }, // 4 meses, fuera del umbral de 3
    ]);

    const resultado = await congelarTarjetasInactivas();

    expect(ledgerService.registrarAjuste).toHaveBeenCalledWith(
      {
        cardId:        3,
        puntos:        -350,
        reasonId:      REASON.id,
        justificacion: expect.stringContaining('3 meses'),
      },
      expect.objectContaining({ dbId: null, nombre: expect.stringContaining('Sistema') }),
    );
    expect(resultado.congeladas).toBe(1);
  });

  test('tarjeta ya en 0 → no genera otro ajuste (idempotente)', async () => {
    configService.obtenerCongelamientoMeses.mockResolvedValue(3);
    LoyaltyCard.findAll.mockResolvedValue([{ id: 4, saldoPuntos: '0.00' }]);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      { cardId: 4, ultimaAcumulacion: haceMeses(10) },
    ]);

    const resultado = await congelarTarjetasInactivas();

    expect(ledgerService.registrarAjuste).not.toHaveBeenCalled();
    expect(resultado.congeladas).toBe(0);
  });

  test('una tarjeta falla (registrarAjuste tira) → no frena las demás, se cuenta en errores', async () => {
    configService.obtenerCongelamientoMeses.mockResolvedValue(3);
    LoyaltyCard.findAll.mockResolvedValue([
      { id: 5, saldoPuntos: '100.00' },
      { id: 6, saldoPuntos: '200.00' },
    ]);
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      { cardId: 5, ultimaAcumulacion: haceMeses(5) },
      { cardId: 6, ultimaAcumulacion: haceMeses(5) },
    ]);
    ledgerService.registrarAjuste
      .mockRejectedValueOnce(new Error('falla simulada'))
      .mockResolvedValueOnce({ movimiento: {}, saldoPuntos: '0.00' });

    const resultado = await congelarTarjetasInactivas();

    expect(ledgerService.registrarAjuste).toHaveBeenCalledTimes(2);
    expect(resultado.errores).toBe(1);
    expect(resultado.congeladas).toBe(1);
  });
});
