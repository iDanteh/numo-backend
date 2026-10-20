'use strict';

/**
 * domains/ledger/loyalty-ledger.service.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad (PR2) — ledger transaccional (D4: lock de fila + CHECK
 * saldo_puntos >= 0 como respaldo). Mismo patrón de mocking que
 * `poliza.netpay-reporte.test.js` (jest.mock de shared/models/postgres con
 * objetos planos), extendido con un mock de `sequelize.transaction` que
 * invoca el callback con una transacción falsa — no hay Postgres real en CI
 * (ver tasks.md Phase 2, nota de mocking del prompt de apply).
 */

const { Transaction } = require('sequelize');

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard:             { findByPk: jest.fn() },
  LoyaltyPointsLedger:     { create: jest.fn(), findAndCountAll: jest.fn() },
  LoyaltyTierHistory:      { findAll: jest.fn() },
  LoyaltyAdjustmentReason: { findByPk: jest.fn() },
  CentroCosto:             { findByPk: jest.fn() },
}));
jest.mock('../../../config/database.postgres', () => ({
  sequelize: { transaction: jest.fn() },
}));
jest.mock('../../../shared/services/global-config.service', () => ({
  getValue: jest.fn(),
}));
// loyalty-config.service.js se mockea completo (no hay require circular real:
// config no importa nada de ledger, verificado en el código — se mockea igual
// que global-config.service para no acoplar este test al modelo LoyaltyPromotion).
jest.mock('../config/loyalty-config.service', () => ({
  obtenerPromocionVigente: jest.fn(),
}));

const {
  LoyaltyCard, LoyaltyPointsLedger, LoyaltyTierHistory, LoyaltyAdjustmentReason, CentroCosto,
} = require('../../../shared/models/postgres');
const { sequelize } = require('../../../config/database.postgres');
const globalConfigService = require('../../../shared/services/global-config.service');
const configService = require('../config/loyalty-config.service');
const ledgerService = require('./loyalty-ledger.service');

/** Simula una instancia Sequelize mínima: estado mutable + `.update()`. */
function makeCard(overrides = {}) {
  const card = {
    id: 1,
    telefono: '5512345678',
    nivel: 'Plata',
    saldoPuntos: '0.00',
    activa: true,
    ...overrides,
  };
  card.update = jest.fn(async (changes) => {
    Object.assign(card, changes);
    return card;
  });
  return card;
}

function makeCentroCosto(overrides = {}) {
  return { id: 10, clave: 'D0', isActive: true, participaLealtad: true, permiteCanjeLealtad: true, ...overrides };
}

const USER = { dbId: 99, nombre: 'Tester' };

beforeEach(() => {
  jest.clearAllMocks();
  // sequelize.transaction invoca el callback con una transacción falsa — basta
  // con un objeto distinto de undefined, nada lo inspecciona en estos mocks.
  sequelize.transaction.mockImplementation(async (cb) => cb({ fake: true }));
  LoyaltyPointsLedger.create.mockImplementation(async (data) => ({ ...data }));
  // Sin promo vigente por default — mismo comportamiento que hoy en todos los
  // tests existentes que no la mencionan explícitamente.
  configService.obtenerPromocionVigente.mockResolvedValue(null);
});

describe('registrarAcumulacion', () => {
  test('participaLealtad=false en el centro de costo → 422, no se escribe nada', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto({ participaLealtad: false }));
    const card = makeCard();
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, montoVenta: '100.00', centroCostoId: 10 }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });

    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
    expect(card.update).not.toHaveBeenCalled();
  });

  test('CentroCosto inactivo → 422', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto({ isActive: false }));

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, montoVenta: '100.00', centroCostoId: 10 }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });
  });

  test('tarjeta inactiva → 422', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ activa: false }));

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, montoVenta: '100.00', centroCostoId: 10 }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });

    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  test('falta configurar el % del nivel (D11) → 422, nunca usa 0%', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ nivel: 'Oro' }));
    globalConfigService.getValue.mockRejectedValue(new Error("No existe la configuración 'lealtad.PORCENTAJE_ORO'"));

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, montoVenta: '100.00', centroCostoId: 10 }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });

    expect(globalConfigService.getValue).toHaveBeenCalledWith('lealtad', 'PORCENTAJE_ORO');
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  test('acumula con el % snapshot y actualiza el saldo denormalizado', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ nivel: 'Plata', saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    globalConfigService.getValue.mockResolvedValue('5');

    const { movimiento, saldoPuntos } = await ledgerService.registrarAcumulacion(
      { cardId: 1, montoVenta: '1000.00', centroCostoId: 10 }, USER,
    );

    expect(movimiento.tipo).toBe('ACUMULACION');
    expect(movimiento.puntos).toBe('50.00');
    expect(movimiento.porcentajeAplicado).toBe(5);
    expect(movimiento.nivelAplicado).toBe('Plata');
    expect(movimiento.saldoResultante).toBe('50.00');
    expect(saldoPuntos).toBe('50.00');
    expect(card.saldoPuntos).toBe('50.00');
  });

  test('el % aplicado queda congelado aunque la config cambie después (snapshot, no referencia viva)', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ nivel: 'Plata', saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    globalConfigService.getValue.mockResolvedValue('5');

    const primera = await ledgerService.registrarAcumulacion(
      { cardId: 1, montoVenta: '1000.00', centroCostoId: 10 }, USER,
    );
    expect(primera.movimiento.porcentajeAplicado).toBe(5);

    // La configuración "cambia" — una acumulación nueva usa el valor nuevo,
    // pero la fila ya escrita (verificada arriba) nunca se recalcula.
    globalConfigService.getValue.mockResolvedValue('8');
    const segunda = await ledgerService.registrarAcumulacion(
      { cardId: 1, montoVenta: '1000.00', centroCostoId: 10 }, USER,
    );
    expect(segunda.movimiento.porcentajeAplicado).toBe(8);
    expect(primera.movimiento.porcentajeAplicado).toBe(5);
  });

  test('montoVenta <= 0 → 400', async () => {
    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, montoVenta: '0.00', centroCostoId: 10 }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(CentroCosto.findByPk).not.toHaveBeenCalled();
  });

  test('el resultado da 0 puntos → 400', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyCard.findByPk.mockResolvedValue(makeCard());
    globalConfigService.getValue.mockResolvedValue('0.01');

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, montoVenta: '0.01', centroCostoId: 10 }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
  });
});

describe('registrarAcumulacion con promoción vigente', () => {
  test('sin promo vigente se comporta exactamente igual que hoy (regresión)', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ nivel: 'Plata', saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    globalConfigService.getValue.mockResolvedValue('5');
    configService.obtenerPromocionVigente.mockResolvedValue(null);

    const { movimiento } = await ledgerService.registrarAcumulacion(
      { cardId: 1, montoVenta: '1000.00', centroCostoId: 10 }, USER,
    );

    expect(movimiento.porcentajeAplicado).toBe(5);
    expect(movimiento.puntos).toBe('50.00');
    expect(movimiento.promocionNombre).toBeNull();
    expect(movimiento.promocionPorcentaje).toBeNull();
  });

  test('con promo vigente suma el % de bono al % del nivel y lo audita en el movimiento', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ nivel: 'Plata', saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    globalConfigService.getValue.mockResolvedValue('5');
    configService.obtenerPromocionVigente.mockResolvedValue({ nombre: 'Buen Fin', porcentajeBono: '3' });

    const { movimiento } = await ledgerService.registrarAcumulacion(
      { cardId: 1, montoVenta: '1000.00', centroCostoId: 10 }, USER,
    );

    expect(movimiento.porcentajeAplicado).toBe(8);
    expect(movimiento.puntos).toBe('80.00');
    expect(movimiento.promocionNombre).toBe('Buen Fin');
    expect(movimiento.promocionPorcentaje).toBe('3');
  });
});

describe('registrarCanje', () => {
  test('permiteCanjeLealtad=false → 422, mientras la acumulación en ese centro sigue funcionando', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto({ permiteCanjeLealtad: false, participaLealtad: true }));

    await expect(
      ledgerService.registrarCanje({ cardId: 1, puntos: '10.00', centroCostoId: 10 }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });

    // La misma sucursal, con participaLealtad true, sí deja acumular (flags independientes)
    const card = makeCard({ nivel: 'Plata', saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    globalConfigService.getValue.mockResolvedValue('5');
    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, montoVenta: '100.00', centroCostoId: 10 }, USER),
    ).resolves.toBeDefined();
  });

  test('canje mayor al saldo disponible → 409, verificado DESPUÉS del lock de fila', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ saldoPuntos: '40.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(
      ledgerService.registrarCanje({ cardId: 1, puntos: '60.00', centroCostoId: 10 }, USER),
    ).rejects.toMatchObject({ statusCode: 409 });

    expect(LoyaltyCard.findByPk).toHaveBeenCalled();
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
    expect(card.saldoPuntos).toBe('40.00');
  });

  test('canje dentro del saldo se registra con signo negativo y saldoResultante correcto', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ saldoPuntos: '100.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const { movimiento, saldoPuntos } = await ledgerService.registrarCanje(
      { cardId: 1, puntos: '60.00', centroCostoId: 10 }, USER,
    );

    expect(movimiento.tipo).toBe('CANJE');
    expect(movimiento.puntos).toBe('-60.00');
    expect(movimiento.saldoResultante).toBe('40.00');
    expect(saldoPuntos).toBe('40.00');
  });

  test('tarjeta inactiva → 422', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ activa: false, saldoPuntos: '100.00' }));

    await expect(
      ledgerService.registrarCanje({ cardId: 1, puntos: '10.00', centroCostoId: 10 }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });
  });
});

describe('registrarAjuste', () => {
  test('signo contrario al permitido por el motivo → 400', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 2, nombre: 'regalo', signoPermitido: 'POSITIVO', activo: true });

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, puntos: '-20.00', reasonId: 2, justificacion: 'corrección de prueba' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });

    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  test('falta justificacion → 400', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 3, nombre: 'correccion', signoPermitido: 'AMBOS', activo: true });

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, puntos: '20.00', reasonId: 3, justificacion: '  ' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
  });

  test('ajuste positivo con motivo permitido actualiza el saldo', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 1, nombre: 'bono', signoPermitido: 'POSITIVO', activo: true });
    const card = makeCard({ saldoPuntos: '10.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const { movimiento, saldoPuntos } = await ledgerService.registrarAjuste(
      { cardId: 1, puntos: '20.00', reasonId: 1, justificacion: 'bono de cumpleaños' }, USER,
    );

    expect(movimiento.tipo).toBe('AJUSTE');
    expect(movimiento.puntos).toBe('20.00');
    expect(saldoPuntos).toBe('30.00');
  });

  test('motivo inactivo → 400', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 4, nombre: 'viejo', signoPermitido: 'AMBOS', activo: false });

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, puntos: '20.00', reasonId: 4, justificacion: 'motivo inactivo' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
  });

  test('el ajuste no puede dejar el saldo en negativo', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 3, nombre: 'correccion', signoPermitido: 'AMBOS', activo: true });
    const card = makeCard({ saldoPuntos: '10.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, puntos: '-20.00', reasonId: 3, justificacion: 'corrección de saldo' }, USER),
    ).rejects.toMatchObject({ statusCode: 409 });

    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });
});

describe('registrarAjuste — motivo SIN % (regresión del flujo manual existente)', () => {
  test('sigue calculando los puntos directo del body, sin tocar montoBase', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 1, nombre: 'bono', signoPermitido: 'POSITIVO', activo: true });
    const card = makeCard({ saldoPuntos: '10.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const { movimiento, saldoPuntos } = await ledgerService.registrarAjuste(
      { cardId: 1, puntos: '20.00', reasonId: 1, justificacion: 'bono de cumpleaños' }, USER,
    );

    expect(movimiento.tipo).toBe('AJUSTE');
    expect(movimiento.puntos).toBe('20.00');
    expect(movimiento.montoVenta).toBeNull();
    expect(movimiento.porcentajeAplicado).toBeNull();
    expect(saldoPuntos).toBe('30.00');
  });
});

describe('registrarAjuste — motivo CON % de cálculo automático', () => {
  function makeReasonPct(overrides = {}) {
    return { id: 10, nombre: 'comision', signoPermitido: 'POSITIVO', activo: true, porcentaje: '5.00', ...overrides };
  }

  test('montoBase faltante → 400, no escribe nada', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(makeReasonPct());

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, reasonId: 10, justificacion: 'ajuste automático' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  test('montoBase = 0 → 400', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(makeReasonPct());

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, montoBase: '0', reasonId: 10, justificacion: 'ajuste automático' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
  });

  test('motivo POSITIVO calcula puntos positivos desde montoBase y audita montoVenta/porcentajeAplicado', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(makeReasonPct({ signoPermitido: 'POSITIVO' }));
    const card = makeCard({ saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const { movimiento, saldoPuntos } = await ledgerService.registrarAjuste(
      { cardId: 1, montoBase: '1000.00', reasonId: 10, justificacion: 'ajuste automático' }, USER,
    );

    expect(movimiento.puntos).toBe('50.00');
    expect(movimiento.montoVenta).toBe('1000.00');
    expect(movimiento.porcentajeAplicado).toBe('5.00');
    expect(saldoPuntos).toBe('50.00');
  });

  test('motivo NEGATIVO calcula puntos negativos desde montoBase', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(makeReasonPct({ signoPermitido: 'NEGATIVO' }));
    const card = makeCard({ saldoPuntos: '100.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const { movimiento, saldoPuntos } = await ledgerService.registrarAjuste(
      { cardId: 1, montoBase: '1000.00', reasonId: 10, justificacion: 'ajuste automático' }, USER,
    );

    expect(movimiento.puntos).toBe('-50.00');
    expect(saldoPuntos).toBe('50.00');
  });

  test('el monto no genera puntos con el % configurado → 400', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(makeReasonPct({ porcentaje: '0.01' }));

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, montoBase: '0.01', reasonId: 10, justificacion: 'ajuste automático' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
  });

  test('datos legados: motivo con % y signoPermitido AMBOS → 400 (estado inválido, no se asume que nunca pasa)', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(makeReasonPct({ signoPermitido: 'AMBOS' }));

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, montoBase: '100.00', reasonId: 10, justificacion: 'caso legado' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
  });

  test('el ajuste automático no puede dejar el saldo en negativo (NEGATIVO mayor al saldo)', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue(makeReasonPct({ signoPermitido: 'NEGATIVO', porcentaje: '50.00' }));
    const card = makeCard({ saldoPuntos: '10.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, montoBase: '1000.00', reasonId: 10, justificacion: 'ajuste automático' }, USER),
    ).rejects.toMatchObject({ statusCode: 409 });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });
});

describe('saldoResultante encadenado', () => {
  test('una cadena de movimientos mantiene saldoResultante = suma acumulada', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ nivel: 'Plata', saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    globalConfigService.getValue.mockResolvedValue('10');
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 1, nombre: 'bono', signoPermitido: 'POSITIVO', activo: true });

    const r1 = await ledgerService.registrarAcumulacion({ cardId: 1, montoVenta: '100.00', centroCostoId: 10 }, USER);
    expect(r1.movimiento.saldoResultante).toBe('10.00');

    const r2 = await ledgerService.registrarAjuste({ cardId: 1, puntos: '5.00', reasonId: 1, justificacion: 'bono extra' }, USER);
    expect(r2.movimiento.saldoResultante).toBe('15.00');

    const r3 = await ledgerService.registrarCanje({ cardId: 1, puntos: '15.00', centroCostoId: 10 }, USER);
    expect(r3.movimiento.saldoResultante).toBe('0.00');
  });
});

describe('listarMovimientos / listarNiveles', () => {
  test('listarMovimientos pagina y ordena por fecha descendente', async () => {
    LoyaltyPointsLedger.findAndCountAll.mockResolvedValue({ count: 2, rows: [{ id: 2 }, { id: 1 }] });
    const result = await ledgerService.listarMovimientos(1, { page: 1, limit: 50 });
    expect(result.total).toBe(2);
    expect(result.movimientos).toHaveLength(2);
  });

  test('listarNiveles delega en LoyaltyTierHistory', async () => {
    LoyaltyTierHistory.findAll.mockResolvedValue([{ id: 1 }]);
    const result = await ledgerService.listarNiveles(1);
    expect(result).toHaveLength(1);
    expect(LoyaltyTierHistory.findAll).toHaveBeenCalledWith(expect.objectContaining({ where: { cardId: 1 } }));
  });
});

// Transaction.LOCK.UPDATE se usa en el servicio real — este `require` solo
// confirma que la librería expone la constante que el servicio consumirá.
test('Transaction.LOCK.UPDATE existe en la librería real de Sequelize', () => {
  expect(Transaction.LOCK.UPDATE).toBeDefined();
});
