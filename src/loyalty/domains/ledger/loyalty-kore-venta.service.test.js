'use strict';

/**
 * loyalty-kore-venta.service.test.js — webhook Kore→Numo de venta (acumulación +
 * devengación combinadas). Mismo patrón de mocking que loyalty-ledger.service.test.js
 * (modelos planos + sequelize.transaction invocando el callback con una tx falsa) — la
 * diferencia es que acá `./loyalty-ledger.service` NO se mockea: corre real sobre los
 * mismos modelos mockeados, para no reimplementar _aplicarMovimiento/_resolverCentroCosto*
 * en un doble.
 */

const { Transaction } = require('sequelize');

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard: { findOne: jest.fn(), findByPk: jest.fn() },
  LoyaltyPointsLedger: { create: jest.fn(), findAll: jest.fn() },
  LoyaltyKoreConcepto: { findOne: jest.fn() },
  LoyaltyKoreFormaPago: { findOne: jest.fn() },
  CentroCosto: { findByPk: jest.fn() },
}));
jest.mock('../../../config/database.postgres', () => ({
  sequelize: { transaction: jest.fn() },
}));
jest.mock('../../../banks/domains/centros-costo/centros-costo.service', () => ({
  resolveBySerieMap: jest.fn(),
}));
jest.mock('../../../shared/services/global-config.service', () => ({
  getValue: jest.fn(),
}));
jest.mock('../config/loyalty-config.service', () => ({
  obtenerPromocionVigente: jest.fn(),
  sincronizarConceptosKore: jest.fn(),
  sincronizarFormasPagoKore: jest.fn(),
}));
jest.mock('./loyalty-venta-fallida.service', () => ({
  registrarVentaFallida: jest.fn(),
}));

const {
  LoyaltyCard, LoyaltyPointsLedger, LoyaltyKoreConcepto, LoyaltyKoreFormaPago, CentroCosto,
} = require('../../../shared/models/postgres');
const { sequelize } = require('../../../config/database.postgres');
const centrosCostoService = require('../../../banks/domains/centros-costo/centros-costo.service');
const globalConfigService = require('../../../shared/services/global-config.service');
const configService = require('../config/loyalty-config.service');
const { registrarVentaFallida } = require('./loyalty-venta-fallida.service');
const ventaService = require('./loyalty-kore-venta.service');

function makeCard(overrides = {}) {
  const card = {
    id: 1, telefono: '5512345678', koreClienteId: 'KORE-1', nivel: 'Plata',
    saldoPuntos: '100.00', activa: true, ...overrides,
  };
  card.update = jest.fn(async (changes) => { Object.assign(card, changes); return card; });
  return card;
}

function makeCentroCosto(overrides = {}) {
  return { id: 10, clave: 'A0', isActive: true, participaLealtad: true, ...overrides };
}

/** `_resolverTarjeta` usa `findOne` (fuera de transacción); una vez dentro de la transacción, el
 *  servicio vuelve a leer la MISMA fila con `findByPk` bajo candado de fila — ambos deben resolver
 *  a la misma instancia para que las mutaciones de `_aplicarMovimiento` se reflejen en el resultado. */
function mockCard(card) {
  LoyaltyCard.findOne.mockResolvedValue(card);
  LoyaltyCard.findByPk.mockResolvedValue(card);
  return card;
}

const PAYLOAD_BASE = {
  telefono: '5512345678',
  serieFolio: 'A0-260605320',
  conceptoId: 'CONCEPTO-1',
  metodoPago: 'PUE',
  formasPago: [{ formaPagoId: 'FP-EFECTIVO', importe: 1160 }],
  subtotal: 1000,
  total: 1160,
  usuarioAplicaId: 'U1',
  usuarioAplicaNombre: 'Cajero Uno',
  fecha: '2026-10-26T12:00:00Z',
};

beforeEach(() => {
  jest.clearAllMocks();
  sequelize.transaction.mockImplementation(async (cb) => cb({}));
  centrosCostoService.resolveBySerieMap.mockResolvedValue({ A0: makeCentroCosto() });
  CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
  LoyaltyPointsLedger.findAll.mockResolvedValue([]);
  LoyaltyPointsLedger.create.mockImplementation(async (data) => ({ ...data, saldoResultante: data.saldoResultante }));
  globalConfigService.getValue.mockResolvedValue('5.00');
  configService.obtenerPromocionVigente.mockResolvedValue(null);
  LoyaltyKoreConcepto.findOne.mockResolvedValue({ koreId: 'CONCEPTO-1', acumulaPuntos: true });
  LoyaltyKoreFormaPago.findOne.mockImplementation(({ where }) => {
    if (where.koreId === 'FP-EFECTIVO') return Promise.resolve({ koreId: 'FP-EFECTIVO', acumulaPuntos: true, esPuntos: false });
    if (where.koreId === 'FP-PUNTOS') return Promise.resolve({ koreId: 'FP-PUNTOS', acumulaPuntos: false, esPuntos: true });
    return Promise.resolve(null);
  });
});

describe('consultarSaldoKore', () => {
  it('matchea por koreClienteId primero', async () => {
    LoyaltyCard.findOne.mockResolvedValueOnce(makeCard({ saldoPuntos: '250.00' }));
    const res = await ventaService.consultarSaldoKore({ koreClienteId: 'KORE-1' });
    expect(res).toEqual({ tieneTarjeta: true, saldoDisponible: 250, nivel: 'Plata' });
    expect(LoyaltyCard.findOne).toHaveBeenCalledWith({ where: { koreClienteId: 'KORE-1' } });
  });

  it('cae a teléfono si no matchea por koreClienteId', async () => {
    LoyaltyCard.findOne.mockResolvedValueOnce(null).mockResolvedValueOnce(makeCard());
    const res = await ventaService.consultarSaldoKore({ telefono: '5512345678', koreClienteId: 'OTRO' });
    expect(res.tieneTarjeta).toBe(true);
  });

  it('404 si no matchea nada', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);
    await expect(ventaService.consultarSaldoKore({ telefono: '5512345678' })).rejects.toMatchObject({ statusCode: 404 });
  });
});

describe('procesarVentaKore — validación', () => {
  it('400 si faltan campos requeridos', async () => {
    await expect(ventaService.procesarVentaKore({ telefono: '5512345678' })).rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si la suma de formasPago no coincide con total', async () => {
    const payload = { ...PAYLOAD_BASE, formasPago: [{ formaPagoId: 'FP-EFECTIVO', importe: 500 }] };
    await expect(ventaService.procesarVentaKore(payload)).rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si serieFolio no tiene formato SERIE-FOLIO', async () => {
    const payload = { ...PAYLOAD_BASE, serieFolio: 'invalido' };
    await expect(ventaService.procesarVentaKore(payload)).rejects.toMatchObject({ statusCode: 400 });
  });

  it('404 si la tarjeta no existe', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);
    await expect(ventaService.procesarVentaKore(PAYLOAD_BASE)).rejects.toMatchObject({ statusCode: 404 });
  });
});

describe('procesarVentaKore — acumulación pura', () => {
  it('acumula proporcional al subtotal cuando todo califica', async () => {
    mockCard(makeCard());
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res.status).toBe('aplicado');
    expect(res.puntosDescontados).toBe(0);
    // subtotal 1000 * 5% = 50
    expect(res.puntosAcumulados).toBe(50);
    expect(res.saldoResultante).toBe(150);
  });

  it('no_califica si metodoPago es PPD', async () => {
    mockCard(makeCard());
    const res = await ventaService.procesarVentaKore({ ...PAYLOAD_BASE, metodoPago: 'PPD' });
    expect(res.status).toBe('no_califica');
    expect(res.puntosAcumulados).toBe(0);
  });

  it('no_califica si la sucursal no participa en Lealtad', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto({ participaLealtad: false }));
    mockCard(makeCard());
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res.status).toBe('no_califica');
  });

  it('no_califica si el concepto no acumula puntos', async () => {
    LoyaltyKoreConcepto.findOne.mockResolvedValue({ koreId: 'CONCEPTO-1', acumulaPuntos: false });
    mockCard(makeCard());
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res.status).toBe('no_califica');
  });
});

describe('procesarVentaKore — formas de pago mixtas (proporcional)', () => {
  it('calcula puntos solo sobre la porción calificante', async () => {
    mockCard(makeCard());
    LoyaltyKoreFormaPago.findOne.mockImplementation(({ where }) => {
      if (where.koreId === 'FP-EFECTIVO') return Promise.resolve({ koreId: 'FP-EFECTIVO', acumulaPuntos: true, esPuntos: false });
      if (where.koreId === 'FP-TRANSFERENCIA') return Promise.resolve({ koreId: 'FP-TRANSFERENCIA', acumulaPuntos: false, esPuntos: false });
      return Promise.resolve(null);
    });
    const payload = {
      ...PAYLOAD_BASE,
      formasPago: [
        { formaPagoId: 'FP-EFECTIVO', importe: 580 },
        { formaPagoId: 'FP-TRANSFERENCIA', importe: 580 },
      ],
      subtotal: 1000, total: 1160,
    };
    const res = await ventaService.procesarVentaKore(payload);
    // montoCalificable = 580 (solo efectivo). Prorrateado: 580/1160*1000 = 500. 5% de 500 = 25.
    expect(res.puntosAcumulados).toBe(25);

    // montoBaseCalculo (2026-10-27): debe guardar el monto REAL usado en el cálculo (500, el
    // prorrateado), NUNCA el subtotal completo (1000) — es justo el caso que motivó el campo.
    const filaNivel = LoyaltyPointsLedger.create.mock.calls.map(([d]) => d).find(d => d.tipo === 'ACUMULACION');
    expect(filaNivel.montoVenta).toBe(1000);
    expect(filaNivel.montoBaseCalculo).toBe('500.00');
  });

  it('sin mezcla (una sola forma que acumula): montoBaseCalculo coincide con montoVenta', async () => {
    mockCard(makeCard());
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res.puntosAcumulados).toBe(50);

    const filaNivel = LoyaltyPointsLedger.create.mock.calls.map(([d]) => d).find(d => d.tipo === 'ACUMULACION');
    expect(filaNivel.montoBaseCalculo).toBe('1000.00');
    expect(Number(filaNivel.montoBaseCalculo)).toBe(filaNivel.montoVenta);
  });
});

describe('procesarVentaKore — devengación (canje)', () => {
  const payloadConPuntos = {
    ...PAYLOAD_BASE,
    formasPago: [
      { formaPagoId: 'FP-EFECTIVO', importe: 660 },
      { formaPagoId: 'FP-PUNTOS', importe: 500 },
    ],
    total: 1160,
  };

  it('descuenta puntos independientemente de si la venta acumula', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto({ participaLealtad: false })); // no acumula
    mockCard(makeCard({ saldoPuntos: '600.00' }));
    const res = await ventaService.procesarVentaKore(payloadConPuntos);
    expect(res.status).toBe('aplicado');
    expect(res.puntosDescontados).toBe(500);
    expect(res.puntosAcumulados).toBe(0);
    expect(res.saldoResultante).toBe(100);
  });

  it('400 si el saldo no alcanza', async () => {
    mockCard(makeCard({ saldoPuntos: '100.00' }));
    await expect(ventaService.procesarVentaKore(payloadConPuntos)).rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si la tarjeta está inactiva', async () => {
    mockCard(makeCard({ activa: false, saldoPuntos: '600.00' }));
    await expect(ventaService.procesarVentaKore(payloadConPuntos)).rejects.toMatchObject({ statusCode: 400 });
  });
});

describe('procesarVentaKore — idempotencia', () => {
  it('devuelve el resultado ya aplicado sin volver a escribir si el serieFolio ya se procesó', async () => {
    mockCard(makeCard({ saldoPuntos: '150.00' }));
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      { tipo: 'ACUMULACION', puntos: '50.00', montoVenta: '1000.00', referenciaExterna: 'A0-260605320:ACUMULACION' },
    ]);
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res).toEqual({
      serieFolio: 'A0-260605320', status: 'aplicado', puntosDescontados: 0, puntosAcumulados: 50, saldoResultante: 150,
    });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  it('con nivel + promoción ya aplicados, suma AMBAS filas al reconstruir la respuesta idempotente', async () => {
    mockCard(makeCard({ saldoPuntos: '180.00' }));
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      { tipo: 'ACUMULACION', puntos: '50.00', montoVenta: '1000.00', referenciaExterna: 'A0-260605320:ACUMULACION' },
      { tipo: 'ACUMULACION', puntos: '30.00', montoVenta: '1000.00', referenciaExterna: 'A0-260605320:PROMOCION' },
    ]);
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res).toEqual({
      serieFolio: 'A0-260605320', status: 'aplicado', puntosDescontados: 0, puntosAcumulados: 80, saldoResultante: 180,
    });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  it('blindaje cruzado: si la venta ya se acreditó a mano (origen manual, sin sufijo), responde "aplicado" sin duplicar', async () => {
    mockCard(makeCard({ saldoPuntos: '50.00' }));
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      { tipo: 'ACUMULACION', puntos: '50.00', montoVenta: '1000.00', referenciaExterna: 'A0-260605320', origen: 'manual' },
    ]);
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res).toEqual({
      serieFolio: 'A0-260605320', status: 'aplicado', puntosDescontados: 0, puntosAcumulados: 50, saldoResultante: 50,
    });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  it('409 si el mismo serieFolio ya se reportó con un subtotal distinto', async () => {
    mockCard(makeCard());
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      { tipo: 'ACUMULACION', puntos: '50.00', montoVenta: '1000.00', referenciaExterna: 'A0-260605320:ACUMULACION' },
    ]);
    const payload = { ...PAYLOAD_BASE, subtotal: 2000 };
    await expect(ventaService.procesarVentaKore(payload)).rejects.toMatchObject({ statusCode: 409 });
  });
});

describe('procesarVentaKore — acumulación con promoción vigente (2026-10-27: filas separadas)', () => {
  it('genera DOS filas ACUMULACION (nivel y promoción) con referenciaExterna distintos', async () => {
    mockCard(makeCard());
    configService.obtenerPromocionVigente.mockResolvedValue({ nombre: 'Buen Fin', porcentajeBono: '3' });

    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);

    // subtotal 1000 * 5% (nivel) = 50; subtotal 1000 * 3% (promo) = 30; total 80.
    expect(res.status).toBe('aplicado');
    expect(res.puntosAcumulados).toBe(80);
    expect(res.saldoResultante).toBe(180);

    const llamadas = LoyaltyPointsLedger.create.mock.calls.map(([data]) => data);
    const filaNivel = llamadas.find(d => d.referenciaExterna === 'A0-260605320:ACUMULACION');
    const filaPromo = llamadas.find(d => d.referenciaExterna === 'A0-260605320:PROMOCION');
    expect(filaNivel).toMatchObject({ tipo: 'ACUMULACION', puntos: '50.00', nivelAplicado: 'Plata', promocionNombre: null });
    expect(filaPromo).toMatchObject({ tipo: 'ACUMULACION', puntos: '30.00', promocionNombre: 'Buen Fin', promocionPorcentaje: '3' });
  });

  it('idempotencia: la fila de promoción también entra en el chequeo de referencias ya procesadas', async () => {
    mockCard(makeCard({ saldoPuntos: '180.00' }));
    configService.obtenerPromocionVigente.mockResolvedValue({ nombre: 'Buen Fin', porcentajeBono: '3' });
    LoyaltyPointsLedger.findAll.mockResolvedValue([
      { tipo: 'ACUMULACION', puntos: '50.00', montoVenta: '1000.00', referenciaExterna: 'A0-260605320:ACUMULACION' },
      { tipo: 'ACUMULACION', puntos: '30.00', montoVenta: '1000.00', referenciaExterna: 'A0-260605320:PROMOCION' },
    ]);

    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res.puntosAcumulados).toBe(80);
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });
});

describe('procesarVentaKore — catálogo desconocido', () => {
  it('resincroniza una vez, sigue conservador y registra proceso fallido si el concepto sigue sin existir', async () => {
    mockCard(makeCard());
    LoyaltyKoreConcepto.findOne.mockResolvedValue(null);
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(configService.sincronizarConceptosKore).toHaveBeenCalledTimes(1);
    expect(res.status).toBe('no_califica');
    expect(registrarVentaFallida).toHaveBeenCalledWith(
      'A0-260605320',
      expect.objectContaining({ serieFolio: 'A0-260605320', conceptoId: 'CONCEPTO-1' }),
      expect.stringContaining('Concepto desconocido'),
    );
  });
});

describe('procesarVentaKore — sin movimientos', () => {
  it('status no_califica cuando no hay nada que acumular ni descontar', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto({ participaLealtad: false }));
    mockCard(makeCard());
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res).toMatchObject({ status: 'no_califica', puntosAcumulados: 0, puntosDescontados: 0 });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });
});
