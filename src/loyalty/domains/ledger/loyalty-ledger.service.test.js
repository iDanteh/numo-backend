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

const { Transaction, Op } = require('sequelize');

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard:             { findByPk: jest.fn() },
  LoyaltyPointsLedger:     { create: jest.fn(), findAndCountAll: jest.fn(), findOne: jest.fn() },
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
// Serie-Folio (2026-10-21): registrarAcumulacion/registrarCanje ya no reciben
// centroCostoId directo — resuelven la serie contra este mapa (mismo que usa
// el generador de pólizas, reutilizado vía require, nunca copiado).
jest.mock('../../../banks/domains/centros-costo/centros-costo.service', () => ({
  resolveBySerieMap: jest.fn(),
}));
// Consulta real a Kore (2026-10-21): registrarAcumulacion ya no recibe
// montoVenta del body — lo resuelve consultando la CxC real por Serie-Folio.
jest.mock('../../../banks/domains/erp/erp-sync.service', () => ({
  sincronizarCuentasPendientes: jest.fn(),
}));

const {
  LoyaltyCard, LoyaltyPointsLedger, LoyaltyTierHistory, LoyaltyAdjustmentReason, CentroCosto,
} = require('../../../shared/models/postgres');
const { sequelize } = require('../../../config/database.postgres');
const globalConfigService = require('../../../shared/services/global-config.service');
const configService = require('../config/loyalty-config.service');
const centrosCostoService = require('../../../banks/domains/centros-costo/centros-costo.service');
const erpSyncService = require('../../../banks/domains/erp/erp-sync.service');
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

/** Respuesta cruda de Kore (/cuentas-pendientes) para UNA cuenta — ver el
 *  Mensaje/Data/cuentas que pegó el usuario; solo se usan subtotal/total. */
function makeCuentaKore(overrides = {}) {
  return {
    serieExterna: 'A0', folioExterno: '260605320', subtotal: 1000, impuesto: 160, total: 1160,
    ...overrides,
  };
}

const USER = { dbId: 99, nombre: 'Tester' };

beforeEach(() => {
  jest.clearAllMocks();
  // sequelize.transaction invoca el callback con una transacción falsa — basta
  // con un objeto distinto de undefined, nada lo inspecciona en estos mocks.
  sequelize.transaction.mockImplementation(async (cb) => cb({ fake: true }));
  LoyaltyPointsLedger.create.mockImplementation(async (data) => ({ ...data }));
  // Sin acreditación previa de Kore por default — mismo comportamiento que hoy
  // en todos los tests existentes que no mencionan el blindaje cruzado manual/kore.
  LoyaltyPointsLedger.findOne.mockResolvedValue(null);
  // Sin promo vigente por default — mismo comportamiento que hoy en todos los
  // tests existentes que no la mencionan explícitamente.
  configService.obtenerPromocionVigente.mockResolvedValue(null);
  // Serie "A0" → mismo centro de costo (id 10) que CentroCosto.findByPk
  // devuelve por default (makeCentroCosto) — todos los tests existentes de
  // registrarAcumulacion/registrarCanje siguen usando ese centro, ahora
  // resuelto desde una serie en vez de mandar centroCostoId directo.
  centrosCostoService.resolveBySerieMap.mockResolvedValue({ A0: { id: 10, clave: 'D0', sucursal: 'Matriz' } });
  // Kore encuentra la venta por default con subtotal 1000 (igual a los
  // montoVenta de siempre en estos tests) — los tests que necesitan un
  // subtotal distinto lo sobreescriben explícitamente.
  erpSyncService.sincronizarCuentasPendientes.mockResolvedValue({ raw: [makeCuentaKore()] });
});

describe('registrarAcumulacion', () => {
  test('participaLealtad=false en el centro de costo → 422, no se escribe nada (nunca llega a consultar Kore)', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto({ participaLealtad: false }));
    const card = makeCard();
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });

    expect(erpSyncService.sincronizarCuentasPendientes).not.toHaveBeenCalled();
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
    expect(card.update).not.toHaveBeenCalled();
  });

  test('CentroCosto inactivo → 422', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto({ isActive: false }));

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });
  });

  test('tarjeta inactiva → 422', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ activa: false }));

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });

    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  test('falta configurar el % del nivel (D11) → 422, nunca usa 0%', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ nivel: 'Oro' }));
    globalConfigService.getValue.mockRejectedValue(new Error("No existe la configuración 'lealtad.PORCENTAJE_ORO'"));

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });

    expect(globalConfigService.getValue).toHaveBeenCalledWith('lealtad', 'PORCENTAJE_ORO');
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  test('acumula sobre el SUBTOTAL de Kore (no el total) y actualiza el saldo denormalizado', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ nivel: 'Plata', saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    globalConfigService.getValue.mockResolvedValue('5');
    erpSyncService.sincronizarCuentasPendientes.mockResolvedValue({
      raw: [makeCuentaKore({ subtotal: 1000, total: 1160 })],
    });

    const { movimiento, saldoPuntos } = await ledgerService.registrarAcumulacion(
      { cardId: 1, serieFolio: 'A0-260605320' }, USER,
    );

    expect(movimiento.tipo).toBe('ACUMULACION');
    expect(movimiento.puntos).toBe('50.00');
    expect(movimiento.porcentajeAplicado).toBe(5);
    expect(movimiento.nivelAplicado).toBe('Plata');
    expect(movimiento.saldoResultante).toBe('50.00');
    expect(saldoPuntos).toBe('50.00');
    expect(card.saldoPuntos).toBe('50.00');
    // El monto de venta auditado es el SUBTOTAL (base real del cálculo).
    expect(movimiento.montoVenta).toBe(1000);
    // El total (con impuesto) se guarda aparte, solo para auditoría.
    expect(movimiento.montoVentaTotal).toBe(1160);
    // montoBaseCalculo (2026-10-27): en el flujo manual nunca hay prorrateo, siempre coincide con montoVenta.
    expect(movimiento.montoBaseCalculo).toBe(1000);
  });

  test('el % aplicado queda congelado aunque la config cambie después (snapshot, no referencia viva)', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ nivel: 'Plata', saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    globalConfigService.getValue.mockResolvedValue('5');

    const primera = await ledgerService.registrarAcumulacion(
      { cardId: 1, serieFolio: 'A0-260605320' }, USER,
    );
    expect(primera.movimiento.porcentajeAplicado).toBe(5);

    // La configuración "cambia" — una acumulación nueva usa el valor nuevo,
    // pero la fila ya escrita (verificada arriba) nunca se recalcula.
    globalConfigService.getValue.mockResolvedValue('8');
    const segunda = await ledgerService.registrarAcumulacion(
      { cardId: 1, serieFolio: 'A0-260605320' }, USER,
    );
    expect(segunda.movimiento.porcentajeAplicado).toBe(8);
    expect(primera.movimiento.porcentajeAplicado).toBe(5);
  });

  test('Serie-Folio con formato inválido → 400, nunca resuelve centro de costo ni consulta Kore', async () => {
    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'folio-sin-serie' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(centrosCostoService.resolveBySerieMap).not.toHaveBeenCalled();
    expect(erpSyncService.sincronizarCuentasPendientes).not.toHaveBeenCalled();
    expect(CentroCosto.findByPk).not.toHaveBeenCalled();
  });

  test('serie del Serie-Folio no existe en ningún centro de costo → 400, nunca consulta Kore', async () => {
    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'Z9-123' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(CentroCosto.findByPk).not.toHaveBeenCalled();
    expect(erpSyncService.sincronizarCuentasPendientes).not.toHaveBeenCalled();
  });

  test('la serie se resuelve sin distinguir mayúsculas/minúsculas', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyCard.findByPk.mockResolvedValue(makeCard());
    globalConfigService.getValue.mockResolvedValue('5');

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'a0-260605320' }, USER),
    ).resolves.toBeDefined();
    expect(CentroCosto.findByPk).toHaveBeenCalledWith(10);
  });

  test('consulta Kore con serieExterna/folioExterno y el rango de fecha derivado del folio (YYMM)', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyCard.findByPk.mockResolvedValue(makeCard());
    globalConfigService.getValue.mockResolvedValue('5');

    await ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER);

    // Folio "260605320" → año 2026, mes 06 (ver _rangoDesdeFollo).
    expect(erpSyncService.sincronizarCuentasPendientes).toHaveBeenCalledWith({
      serieExterna: 'A0',
      folioExterno: '260605320',
      fechaDesde: '2026-06-01T00:00:00Z',
      fechaHasta: '2026-06-30T23:59:59Z',
    });
  });

  test('si el rango normal no encuentra nada, reintenta con la ventana de spillover de fin de mes', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyCard.findByPk.mockResolvedValue(makeCard());
    globalConfigService.getValue.mockResolvedValue('5');
    erpSyncService.sincronizarCuentasPendientes
      .mockResolvedValueOnce({ raw: [] })
      .mockResolvedValueOnce({ raw: [makeCuentaKore()] });

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).resolves.toBeDefined();
    expect(erpSyncService.sincronizarCuentasPendientes).toHaveBeenCalledTimes(2);
  });

  test('Kore no encuentra la venta (ni en el mes normal ni en el spillover) → 400', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    erpSyncService.sincronizarCuentasPendientes.mockResolvedValue({ raw: [] });

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  test('Kore responde pero ninguna cuenta calza EXACTO con la serie/folio pedidos → 400 (nunca usa raw[0] a ciegas)', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    erpSyncService.sincronizarCuentasPendientes.mockResolvedValue({
      raw: [makeCuentaKore({ serieExterna: 'B0', folioExterno: '999999999' })],
    });

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
  });

  test('Kore falla (red/timeout) → 422, mensaje claro, nunca escribe nada', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    erpSyncService.sincronizarCuentasPendientes.mockRejectedValue(new Error('timeout'));

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  test('la cuenta encontrada en Kore no tiene subtotal válido (<=0) → 422', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    erpSyncService.sincronizarCuentasPendientes.mockResolvedValue({
      raw: [makeCuentaKore({ subtotal: 0 })],
    });

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  test('idempotencia: el mismo folio no puede generar puntos dos veces → 409, sin recalcular', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyCard.findByPk.mockResolvedValue(makeCard());
    globalConfigService.getValue.mockResolvedValue('5');
    const dupErr = new Error('duplicate');
    dupErr.name = 'SequelizeUniqueConstraintError';
    LoyaltyPointsLedger.create.mockRejectedValueOnce(dupErr);

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).rejects.toMatchObject({ statusCode: 409 });
  });

  test('blindaje cruzado: el folio ya fue acreditado por el webhook de Kore → 409, nunca consulta a Kore ni escribe', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyPointsLedger.findOne.mockResolvedValue({ id: 5, origen: 'kore', referenciaExterna: 'A0-260605320:ACUMULACION' });

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).rejects.toMatchObject({ statusCode: 409 });

    expect(LoyaltyPointsLedger.findOne).toHaveBeenCalledWith({
      where: { origen: 'kore', referenciaExterna: { [Op.like]: 'A0-260605320%' } },
    });
    expect(erpSyncService.sincronizarCuentasPendientes).not.toHaveBeenCalled();
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  test('el resultado da 0 puntos → 400', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyCard.findByPk.mockResolvedValue(makeCard());
    globalConfigService.getValue.mockResolvedValue('0.01');
    erpSyncService.sincronizarCuentasPendientes.mockResolvedValue({
      raw: [makeCuentaKore({ subtotal: 0.01, total: 0.01 })],
    });

    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
  });
});

describe('registrarAcumulacion con promoción vigente (2026-10-27: filas separadas nivel/promoción)', () => {
  test('sin promo vigente se comporta exactamente igual que hoy (regresión) — sin fila de promoción', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ nivel: 'Plata', saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    globalConfigService.getValue.mockResolvedValue('5');
    configService.obtenerPromocionVigente.mockResolvedValue(null);

    const { movimiento, movimientoPromocion } = await ledgerService.registrarAcumulacion(
      { cardId: 1, serieFolio: 'A0-260605320' }, USER,
    );

    expect(movimiento.porcentajeAplicado).toBe(5);
    expect(movimiento.puntos).toBe('50.00');
    expect(movimiento.promocionNombre).toBeNull();
    expect(movimiento.promocionPorcentaje).toBeNull();
    expect(movimientoPromocion).toBeNull();
  });

  test('con promo vigente genera DOS filas separadas en el ledger: una de nivel y otra de promoción', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ nivel: 'Plata', saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    globalConfigService.getValue.mockResolvedValue('5');
    configService.obtenerPromocionVigente.mockResolvedValue({ nombre: 'Buen Fin', porcentajeBono: '3' });

    const { movimiento, movimientoPromocion, saldoPuntos } = await ledgerService.registrarAcumulacion(
      { cardId: 1, serieFolio: 'A0-260605320' }, USER,
    );

    // Fila del nivel: % propio (NO combinado), sin datos de promoción.
    expect(movimiento.porcentajeAplicado).toBe(5);
    expect(movimiento.puntos).toBe('50.00');
    expect(movimiento.promocionNombre).toBeNull();
    expect(movimiento.referenciaExterna).toBe('A0-260605320');

    // Fila de la promoción: referenciaExterna con sufijo propio (no choca con la de nivel).
    expect(movimientoPromocion.porcentajeAplicado).toBe(3);
    expect(movimientoPromocion.puntos).toBe('30.00');
    expect(movimientoPromocion.promocionNombre).toBe('Buen Fin');
    expect(movimientoPromocion.promocionPorcentaje).toBe('3');
    expect(movimientoPromocion.nivelAplicado).toBeNull();
    expect(movimientoPromocion.referenciaExterna).toBe('A0-260605320:PROMOCION');

    // El saldo final ya refleja AMBOS movimientos (50 + 30).
    expect(saldoPuntos).toBe('80.00');
    expect(card.saldoPuntos).toBe('80.00');
  });

  test('el bono de promoción redondea a 0 centavos → no escribe fila de promoción', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ nivel: 'Plata', saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    globalConfigService.getValue.mockResolvedValue('5');
    erpSyncService.sincronizarCuentasPendientes.mockResolvedValue({
      raw: [makeCuentaKore({ subtotal: 1, total: 1 })],
    });
    configService.obtenerPromocionVigente.mockResolvedValue({ nombre: 'Micro Promo', porcentajeBono: '0.01' });

    const { movimiento, movimientoPromocion, saldoPuntos } = await ledgerService.registrarAcumulacion(
      { cardId: 1, serieFolio: 'A0-260605320' }, USER,
    );

    expect(movimiento.puntos).toBe('0.05');
    expect(movimientoPromocion).toBeNull();
    expect(saldoPuntos).toBe('0.05');
  });
});

describe('registrarCanje', () => {
  test('permiteCanjeLealtad=false → 422, mientras la acumulación en esa misma serie sigue funcionando', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto({ permiteCanjeLealtad: false, participaLealtad: true }));

    await expect(
      ledgerService.registrarCanje({ cardId: 1, puntos: '10.00', serie: 'A0' }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });

    // La misma sucursal, con participaLealtad true, sí deja acumular (flags independientes)
    const card = makeCard({ nivel: 'Plata', saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);
    globalConfigService.getValue.mockResolvedValue('5');
    await expect(
      ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER),
    ).resolves.toBeDefined();
  });

  test('serie no existe en ningún centro de costo → 400', async () => {
    await expect(
      ledgerService.registrarCanje({ cardId: 1, puntos: '10.00', serie: 'Z9' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(CentroCosto.findByPk).not.toHaveBeenCalled();
  });

  test('canje mayor al saldo disponible → 409, verificado DESPUÉS del lock de fila', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ saldoPuntos: '40.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(
      ledgerService.registrarCanje({ cardId: 1, puntos: '60.00', serie: 'A0' }, USER),
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
      { cardId: 1, puntos: '60.00', serie: 'A0' }, USER,
    );

    expect(movimiento.tipo).toBe('CANJE');
    expect(movimiento.puntos).toBe('-60.00');
    expect(movimiento.saldoResultante).toBe('40.00');
    expect(saldoPuntos).toBe('40.00');
    expect(movimiento.centroCostoId).toBe(10);
  });

  test('tarjeta inactiva → 422', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    LoyaltyCard.findByPk.mockResolvedValue(makeCard({ activa: false, saldoPuntos: '100.00' }));

    await expect(
      ledgerService.registrarCanje({ cardId: 1, puntos: '10.00', serie: 'A0' }, USER),
    ).rejects.toMatchObject({ statusCode: 422 });
  });

  test('la serie se resuelve sin distinguir mayúsculas/minúsculas', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
    const card = makeCard({ saldoPuntos: '100.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(
      ledgerService.registrarCanje({ cardId: 1, puntos: '10.00', serie: 'a0' }, USER),
    ).resolves.toBeDefined();
    expect(CentroCosto.findByPk).toHaveBeenCalledWith(10);
  });
});

describe('registrarAjuste — motivo manual (puntosFijos: null)', () => {
  test('falta justificacion → 400', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 3, nombre: 'correccion', activo: true, puntosFijos: null });

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, puntos: '20.00', reasonId: 3, justificacion: '  ' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
  });

  test('puntos del body en 0 → 400, no escribe nada', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 3, nombre: 'correccion', activo: true, puntosFijos: null });

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, puntos: '0', reasonId: 3, justificacion: 'ajuste sin monto' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  test('ajuste positivo manual actualiza el saldo', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 1, nombre: 'bono', activo: true, puntosFijos: null });
    const card = makeCard({ saldoPuntos: '10.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const { movimiento, saldoPuntos } = await ledgerService.registrarAjuste(
      { cardId: 1, puntos: '20.00', reasonId: 1, justificacion: 'bono de cumpleaños' }, USER,
    );

    expect(movimiento.tipo).toBe('AJUSTE');
    expect(movimiento.puntos).toBe('20.00');
    expect(saldoPuntos).toBe('30.00');
  });

  test('ajuste negativo manual (ya no hay signoPermitido que lo restrinja)', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 3, nombre: 'correccion', activo: true, puntosFijos: null });
    const card = makeCard({ saldoPuntos: '30.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const { movimiento, saldoPuntos } = await ledgerService.registrarAjuste(
      { cardId: 1, puntos: '-20.00', reasonId: 3, justificacion: 'corrección de prueba' }, USER,
    );

    expect(movimiento.puntos).toBe('-20.00');
    expect(saldoPuntos).toBe('10.00');
  });

  test('motivo inactivo → 400', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 4, nombre: 'viejo', activo: false, puntosFijos: null });

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, puntos: '20.00', reasonId: 4, justificacion: 'motivo inactivo' }, USER),
    ).rejects.toMatchObject({ statusCode: 400 });
  });

  test('el ajuste no puede dejar el saldo en negativo', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 3, nombre: 'correccion', activo: true, puntosFijos: null });
    const card = makeCard({ saldoPuntos: '10.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, puntos: '-20.00', reasonId: 3, justificacion: 'corrección de saldo' }, USER),
    ).rejects.toMatchObject({ statusCode: 409 });

    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });
});

describe('registrarAjuste — motivo con puntosFijos (rediseño 2026-10-23)', () => {
  test('ignora "puntos" del body y aplica EXACTAMENTE el valor fijo positivo (ej. "Bono" = 100)', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 10, nombre: 'Bono', activo: true, puntosFijos: 100 });
    const card = makeCard({ saldoPuntos: '0.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const { movimiento, saldoPuntos } = await ledgerService.registrarAjuste(
      { cardId: 1, puntos: '999.00', reasonId: 10, justificacion: 'bono de bienvenida' }, USER,
    );

    expect(movimiento.puntos).toBe('100.00');
    expect(saldoPuntos).toBe('100.00');
  });

  test('aplica EXACTAMENTE el valor fijo negativo (ej. "Comisión por mora" = -200)', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 11, nombre: 'Comisión por mora', activo: true, puntosFijos: -200 });
    const card = makeCard({ saldoPuntos: '500.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    const { movimiento, saldoPuntos } = await ledgerService.registrarAjuste(
      { cardId: 1, reasonId: 11, justificacion: 'mora de octubre' }, USER,
    );

    expect(movimiento.puntos).toBe('-200.00');
    expect(saldoPuntos).toBe('300.00');
  });

  test('un puntosFijos negativo que deja el saldo en negativo → 409, no escribe nada', async () => {
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 11, nombre: 'Comisión por mora', activo: true, puntosFijos: -200 });
    const card = makeCard({ saldoPuntos: '10.00' });
    LoyaltyCard.findByPk.mockResolvedValue(card);

    await expect(
      ledgerService.registrarAjuste({ cardId: 1, reasonId: 11, justificacion: 'mora de octubre' }, USER),
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
    LoyaltyAdjustmentReason.findByPk.mockResolvedValue({ id: 1, nombre: 'bono', activo: true, puntosFijos: null });
    // Subtotal 100 × 10% = 10 puntos, para que la cadena coincida con los
    // valores de siempre (antes montoVenta='100.00' venía directo del body).
    erpSyncService.sincronizarCuentasPendientes.mockResolvedValue({ raw: [makeCuentaKore({ subtotal: 100, total: 116 })] });

    const r1 = await ledgerService.registrarAcumulacion({ cardId: 1, serieFolio: 'A0-260605320' }, USER);
    expect(r1.movimiento.saldoResultante).toBe('10.00');

    const r2 = await ledgerService.registrarAjuste({ cardId: 1, puntos: '5.00', reasonId: 1, justificacion: 'bono extra' }, USER);
    expect(r2.movimiento.saldoResultante).toBe('15.00');

    const r3 = await ledgerService.registrarCanje({ cardId: 1, puntos: '15.00', serie: 'A0' }, USER);
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
