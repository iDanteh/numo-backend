'use strict';

/**
 * loyalty-kore-venta.service.test.js — webhook Kore→Numo de venta (acumulación +
 * devengación combinadas), contrato 2026-11-06 con `abonos[]` (una CxC puede recibir
 * varios abonos/cobros parciales). Mismo patrón de mocking que loyalty-ledger.service.test.js
 * (modelos planos + sequelize.transaction invocando el callback con una tx falsa) — la
 * diferencia es que acá `./loyalty-ledger.service` NO se mockea: corre real sobre los
 * mismos modelos mockeados, para no reimplementar _aplicarMovimiento/_resolverCentroCosto*
 * en un doble.
 *
 * `sequelize.transaction` ahora se usa en DOS formas: `transaction(cb)` (la del request
 * completo) y `transaction({transaction: t}, cb)` (el savepoint de cada abono) — el mock
 * soporta ambas firmas, invocando el callback con un objeto de transacción falso en
 * cualquier caso (no se simula un ROLLBACK TO SAVEPOINT real; los tests que necesitan
 * verificar aislamiento entre abonos lo hacen con escenarios donde el abono que falla
 * nunca llega a escribir nada, no con una escritura que se revierte).
 */

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
jest.mock('../config/loyalty-config.service', () => ({
  obtenerPromocionVigente: jest.fn(),
  obtenerPorcentajeNivel: jest.fn(),
  sincronizarConceptosKore: jest.fn(),
  sincronizarFormasPagoKore: jest.fn(),
}));
jest.mock('./loyalty-venta-fallida.service', () => ({
  registrarVentaFallida: jest.fn(),
}));
// Notificación en vivo (2026-11-10) — ver mismo criterio en loyalty-kore-cliente.service.test.js.
jest.mock('../../../banks/shared/socket', () => ({ emitToLoyaltyCard: jest.fn() }));

const {
  LoyaltyCard, LoyaltyPointsLedger, LoyaltyKoreConcepto, LoyaltyKoreFormaPago, CentroCosto,
} = require('../../../shared/models/postgres');
const { sequelize } = require('../../../config/database.postgres');
const centrosCostoService = require('../../../banks/domains/centros-costo/centros-costo.service');
const configService = require('../config/loyalty-config.service');
const { registrarVentaFallida } = require('./loyalty-venta-fallida.service');
const { emitToLoyaltyCard } = require('../../../banks/shared/socket');
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
 *  servicio vuelve a leer la MISMA fila con `findByPk` bajo candado de fila (una vez al tomar el
 *  lock y una vez más tras CADA abono, para resincronizar el saldo) — todas deben resolver a la
 *  misma instancia para que las mutaciones de `_aplicarMovimiento` se reflejen en el resultado. */
function mockCard(card) {
  LoyaltyCard.findOne.mockResolvedValue(card);
  LoyaltyCard.findByPk.mockResolvedValue(card);
  return card;
}

/** `LoyaltyPointsLedger.findAll` se llama con `where.origen` distinto para el blindaje manual
 *  ('manual') y para la idempotencia por abono ('kore') — este helper permite mockear cada uno por
 *  separado sin depender del orden de las llamadas. */
function mockLedgerRows({ manual = [], kore = [] } = {}) {
  LoyaltyPointsLedger.findAll.mockImplementation(async (opts) => (
    opts?.where?.origen === 'manual' ? manual : kore
  ));
}

function abono(overrides = {}) {
  return {
    idAbono: 'ABONO-1',
    fecha: '2026-10-26T12:00:00Z',
    formasPago: [{ formaPagoId: 'FP-EFECTIVO', importe: 1160 }],
    subtotal: 1000,
    total: 1160,
    usuarioAplicaId: 'U1',
    puntosAbonados: 50,
    ...overrides,
  };
}

const PAYLOAD_BASE = {
  telefono: '5512345678',
  serieFolio: 'A0-260605320',
  conceptoId: 'CONCEPTO-1',
  metodoPago: 'PUE',
  abonos: [abono()],
};

beforeEach(() => {
  jest.clearAllMocks();
  sequelize.transaction.mockImplementation(async (optionsOrCb, maybeCb) => {
    const cb = typeof optionsOrCb === 'function' ? optionsOrCb : maybeCb;
    return cb({});
  });
  centrosCostoService.resolveBySerieMap.mockResolvedValue({ A0: makeCentroCosto() });
  CentroCosto.findByPk.mockResolvedValue(makeCentroCosto());
  mockLedgerRows();
  LoyaltyPointsLedger.create.mockImplementation(async (data) => ({ ...data, saldoResultante: data.saldoResultante }));
  configService.obtenerPorcentajeNivel.mockResolvedValue(5);
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

  it('400 si abonos está vacío', async () => {
    await expect(ventaService.procesarVentaKore({ ...PAYLOAD_BASE, abonos: [] })).rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si un abono no tiene idAbono', async () => {
    const payload = { ...PAYLOAD_BASE, abonos: [abono({ idAbono: undefined })] };
    await expect(ventaService.procesarVentaKore(payload)).rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si puntosAbonados es negativo (cuando SÍ viene)', async () => {
    const payload = { ...PAYLOAD_BASE, abonos: [abono({ puntosAbonados: -5 })] };
    await expect(ventaService.procesarVentaKore(payload)).rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si dos abonos del mismo request traen el mismo idAbono', async () => {
    const payload = { ...PAYLOAD_BASE, abonos: [abono(), abono()] };
    await expect(ventaService.procesarVentaKore(payload)).rejects.toMatchObject({ statusCode: 400 });
  });

  it('400 si la suma de formasPago de un abono no coincide con su total', async () => {
    const payload = { ...PAYLOAD_BASE, abonos: [abono({ formasPago: [{ formaPagoId: 'FP-EFECTIVO', importe: 500 }] })] };
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

describe('procesarVentaKore — acumulación pura (nivel confiado de Kore)', () => {
  it('acredita exactamente puntosAbonados cuando el abono califica', async () => {
    mockCard(makeCard());
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res.status).toBe('aplicado');
    expect(res.puntosDescontados).toBe(0);
    expect(res.puntosAcumulados).toBe(50); // puntosAbonados del payload, NO recalculado
    expect(res.saldoResultante).toBe(150);
    expect(res.abonos).toEqual([
      { idAbono: 'ABONO-1', success: true, status: 'aplicado', puntosDescontados: 0, puntosAcumulados: 50 },
    ]);
  });

  it('si el % de nivel no está configurado pero Kore mandó puntosAbonados, igual acredita (ya no bloquea)', async () => {
    mockCard(makeCard());
    configService.obtenerPorcentajeNivel.mockResolvedValue(null);
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res.status).toBe('aplicado');
    expect(res.puntosAcumulados).toBe(50);
    expect(registrarVentaFallida).not.toHaveBeenCalled();
    const filaNivel = LoyaltyPointsLedger.create.mock.calls.map(([d]) => d).find(d => d.tipo === 'ACUMULACION');
    expect(filaNivel.porcentajeAplicado).toBeNull();
  });

  it('sin puntosAbonados, calcula con el % real del nivel (LoyaltyKoreNivel vía configService)', async () => {
    mockCard(makeCard());
    const payload = { ...PAYLOAD_BASE, abonos: [abono({ puntosAbonados: undefined })] };
    const res = await ventaService.procesarVentaKore(payload);
    expect(res.status).toBe('aplicado');
    // subtotal 1000, única forma de pago (EFECTIVO, acumula) => prorrateado = 1000, 5% => 50.
    expect(res.puntosAcumulados).toBe(50);
    expect(registrarVentaFallida).not.toHaveBeenCalled();
    const filaNivel = LoyaltyPointsLedger.create.mock.calls.map(([d]) => d).find(d => d.tipo === 'ACUMULACION');
    expect(filaNivel.porcentajeAplicado).toBe(5);
  });

  it('sin puntosAbonados y sin % de nivel configurado: no acredita nivel, pero canje sigue aplicándose y queda registrado como proceso fallido', async () => {
    configService.obtenerPorcentajeNivel.mockResolvedValue(null);
    mockCard(makeCard({ saldoPuntos: '600.00' }));
    const payload = {
      ...PAYLOAD_BASE,
      abonos: [abono({
        formasPago: [
          { formaPagoId: 'FP-EFECTIVO', importe: 660 },
          { formaPagoId: 'FP-PUNTOS', importe: 500 },
        ],
        total: 1160,
        puntosAbonados: undefined,
      })],
    };
    const res = await ventaService.procesarVentaKore(payload);
    expect(res.status).toBe('aplicado'); // el canje sí generó movimiento
    expect(res.puntosDescontados).toBe(500);
    expect(res.puntosAcumulados).toBe(0); // nivel no se pudo calcular
    expect(registrarVentaFallida).toHaveBeenCalledWith(
      'A0-260605320', expect.anything(), expect.stringContaining('ABONO-1'),
    );
    const filaNivel = LoyaltyPointsLedger.create.mock.calls.map(([d]) => d).find(d => d.tipo === 'ACUMULACION');
    expect(filaNivel).toBeUndefined();
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
  it('montoBaseCalculo sigue prorrateándose aunque los puntos vengan de Kore', async () => {
    mockCard(makeCard());
    LoyaltyKoreFormaPago.findOne.mockImplementation(({ where }) => {
      if (where.koreId === 'FP-EFECTIVO') return Promise.resolve({ koreId: 'FP-EFECTIVO', acumulaPuntos: true, esPuntos: false });
      if (where.koreId === 'FP-TRANSFERENCIA') return Promise.resolve({ koreId: 'FP-TRANSFERENCIA', acumulaPuntos: false, esPuntos: false });
      return Promise.resolve(null);
    });
    const payload = {
      ...PAYLOAD_BASE,
      abonos: [abono({
        formasPago: [
          { formaPagoId: 'FP-EFECTIVO', importe: 580 },
          { formaPagoId: 'FP-TRANSFERENCIA', importe: 580 },
        ],
        subtotal: 1000,
        total: 1160,
        puntosAbonados: 25, // lo que Kore calculó para la porción calificante
      })],
    };
    const res = await ventaService.procesarVentaKore(payload);
    expect(res.puntosAcumulados).toBe(25); // directo de puntosAbonados, no recalculado

    const filaNivel = LoyaltyPointsLedger.create.mock.calls.map(([d]) => d).find(d => d.tipo === 'ACUMULACION');
    expect(filaNivel.montoVenta).toBe(1000);
    // Prorrateo: 580/1160*1000 = 500 — se sigue guardando como metadata informativa.
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
    abonos: [abono({
      formasPago: [
        { formaPagoId: 'FP-EFECTIVO', importe: 660 },
        { formaPagoId: 'FP-PUNTOS', importe: 500 },
      ],
      total: 1160,
    })],
  };

  it('descuenta puntos independientemente de si el abono acumula', async () => {
    CentroCosto.findByPk.mockResolvedValue(makeCentroCosto({ participaLealtad: false })); // no acumula
    mockCard(makeCard({ saldoPuntos: '600.00' }));
    const res = await ventaService.procesarVentaKore(payloadConPuntos);
    expect(res.status).toBe('aplicado');
    expect(res.puntosDescontados).toBe(500);
    expect(res.puntosAcumulados).toBe(0);
    expect(res.saldoResultante).toBe(100);
  });

  it('el abono falla (statusCode 400) sin abortar la respuesta si el saldo no alcanza', async () => {
    mockCard(makeCard({ saldoPuntos: '100.00' }));
    const res = await ventaService.procesarVentaKore(payloadConPuntos);
    expect(res.status).toBe('fallido');
    expect(res.abonos).toEqual([
      expect.objectContaining({ idAbono: 'ABONO-1', success: false, statusCode: 400 }),
    ]);
  });

  it('el abono falla (statusCode 400) si la tarjeta está inactiva', async () => {
    mockCard(makeCard({ activa: false, saldoPuntos: '600.00' }));
    const res = await ventaService.procesarVentaKore(payloadConPuntos);
    expect(res.status).toBe('fallido');
    expect(res.abonos[0]).toMatchObject({ success: false, statusCode: 400 });
  });
});

describe('procesarVentaKore — múltiples abonos en un mismo request', () => {
  it('2 abonos que califican generan 2 filas ACUMULACION y el saldo se acumula correctamente', async () => {
    mockCard(makeCard({ saldoPuntos: '0.00' }));
    const payload = {
      ...PAYLOAD_BASE,
      abonos: [
        abono({ idAbono: 'ABONO-1', puntosAbonados: 10 }),
        abono({ idAbono: 'ABONO-2', puntosAbonados: 15 }),
      ],
    };
    const res = await ventaService.procesarVentaKore(payload);
    expect(res.status).toBe('aplicado');
    expect(res.puntosAcumulados).toBe(25);
    expect(res.saldoResultante).toBe(25);
    expect(res.abonos).toEqual([
      { idAbono: 'ABONO-1', success: true, status: 'aplicado', puntosDescontados: 0, puntosAcumulados: 10 },
      { idAbono: 'ABONO-2', success: true, status: 'aplicado', puntosDescontados: 0, puntosAcumulados: 15 },
    ]);
    expect(LoyaltyPointsLedger.create).toHaveBeenCalledTimes(2);
  });

  it('un abono que falla por saldo insuficiente no aborta a los demás abonos del mismo request', async () => {
    mockCard(makeCard({ saldoPuntos: '100.00' }));
    const payload = {
      ...PAYLOAD_BASE,
      abonos: [
        abono({
          idAbono: 'ABONO-CANJE-OK',
          formasPago: [{ formaPagoId: 'FP-PUNTOS', importe: 50 }],
          subtotal: 50, total: 50, puntosAbonados: 0,
        }),
        abono({
          idAbono: 'ABONO-CANJE-EXCEDE',
          formasPago: [{ formaPagoId: 'FP-PUNTOS', importe: 200 }],
          subtotal: 200, total: 200, puntosAbonados: 0,
        }),
      ],
    };
    const res = await ventaService.procesarVentaKore(payload);
    expect(res.status).toBe('parcial');
    expect(res.abonos[0]).toMatchObject({ idAbono: 'ABONO-CANJE-OK', success: true, puntosDescontados: 50 });
    expect(res.abonos[1]).toMatchObject({ idAbono: 'ABONO-CANJE-EXCEDE', success: false, statusCode: 400 });
    // Solo el primer canje (50) se descuenta: 100 - 50 = 50, nunca llega a tocar el segundo.
    expect(res.saldoResultante).toBe(50);
  });

  it('reenvío con 1 abono ya procesado + 1 abono nuevo: el viejo responde idempotente, el nuevo se procesa', async () => {
    mockCard(makeCard({ saldoPuntos: '50.00' }));
    mockLedgerRows({
      kore: [
        { tipo: 'ACUMULACION', puntos: '50.00', montoVenta: '1000.00', referenciaExterna: 'A0-260605320:ABONO-OLD:ACUMULACION' },
      ],
    });
    const payload = {
      ...PAYLOAD_BASE,
      abonos: [
        abono({ idAbono: 'ABONO-OLD', subtotal: 1000, total: 1160 }), // mismo subtotal que la fila guardada
        abono({ idAbono: 'ABONO-NEW', subtotal: 500, total: 580, formasPago: [{ formaPagoId: 'FP-EFECTIVO', importe: 580 }], puntosAbonados: 20 }),
      ],
    };
    const res = await ventaService.procesarVentaKore(payload);
    expect(res.abonos).toEqual([
      { idAbono: 'ABONO-OLD', success: true, status: 'aplicado', puntosDescontados: 0, puntosAcumulados: 50 },
      { idAbono: 'ABONO-NEW', success: true, status: 'aplicado', puntosDescontados: 0, puntosAcumulados: 20 },
    ]);
    expect(res.puntosAcumulados).toBe(70);
    // Solo el abono NUEVO escribe — el viejo no vuelve a crear nada.
    expect(LoyaltyPointsLedger.create).toHaveBeenCalledTimes(1);
  });

  it('abono reenviado con un subtotal distinto al guardado responde 409 aislado a ese abono', async () => {
    mockCard(makeCard({ saldoPuntos: '50.00' }));
    mockLedgerRows({
      kore: [
        { tipo: 'ACUMULACION', puntos: '50.00', montoVenta: '1000.00', referenciaExterna: 'A0-260605320:ABONO-1:ACUMULACION' },
      ],
    });
    const payload = { ...PAYLOAD_BASE, abonos: [abono({ subtotal: 2000 })] };
    const res = await ventaService.procesarVentaKore(payload);
    expect(res.status).toBe('fallido');
    expect(res.abonos[0]).toMatchObject({ idAbono: 'ABONO-1', success: false, statusCode: 409 });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });
});

describe('procesarVentaKore — blindaje cruzado manual↔kore', () => {
  it('si la venta ya se acreditó a mano, TODOS los abonos del request responden "aplicado" sin duplicar', async () => {
    mockCard(makeCard({ saldoPuntos: '50.00' }));
    mockLedgerRows({
      manual: [
        { tipo: 'ACUMULACION', puntos: '50.00', montoVenta: '1000.00', referenciaExterna: 'A0-260605320', origen: 'manual' },
      ],
    });
    const payload = {
      ...PAYLOAD_BASE,
      abonos: [abono({ idAbono: 'ABONO-1' }), abono({ idAbono: 'ABONO-2' })],
    };
    const res = await ventaService.procesarVentaKore(payload);
    expect(res).toMatchObject({
      serieFolio: 'A0-260605320', status: 'aplicado', puntosDescontados: 0, puntosAcumulados: 50, saldoResultante: 50,
    });
    expect(res.abonos).toHaveLength(2);
    expect(res.abonos[0]).toMatchObject({ idAbono: 'ABONO-1', success: true, status: 'aplicado' });
    expect(res.abonos[1]).toMatchObject({ idAbono: 'ABONO-2', success: true, status: 'aplicado' });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  it('blindaje cruzado de CANJE: si el canje ya se aplicó a mano, responde "aplicado" sin duplicar', async () => {
    mockCard(makeCard({ saldoPuntos: '50.00' }));
    mockLedgerRows({
      manual: [
        { tipo: 'CANJE', puntos: '-15.00', referenciaExterna: 'A0-260605320:CANJE', origen: 'manual' },
      ],
    });
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res).toMatchObject({
      serieFolio: 'A0-260605320', status: 'aplicado', puntosDescontados: 15, puntosAcumulados: 0, saldoResultante: 50,
    });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });
});

describe('procesarVentaKore — acumulación con promoción vigente (filas separadas, por abono)', () => {
  it('genera DOS filas ACUMULACION (nivel y promoción) con referenciaExterna distintos para el mismo abono', async () => {
    mockCard(makeCard());
    configService.obtenerPromocionVigente.mockResolvedValue({ nombre: 'Buen Fin', porcentajeBono: '3' });

    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);

    // Nivel: puntosAbonados=50 (de Kore). Promo: subtotal 1000 * 3% = 30. Total 80.
    expect(res.status).toBe('aplicado');
    expect(res.puntosAcumulados).toBe(80);
    expect(res.saldoResultante).toBe(180);

    const llamadas = LoyaltyPointsLedger.create.mock.calls.map(([data]) => data);
    const filaNivel = llamadas.find(d => d.referenciaExterna === 'A0-260605320:ABONO-1:ACUMULACION');
    const filaPromo = llamadas.find(d => d.referenciaExterna === 'A0-260605320:ABONO-1:PROMOCION');
    expect(filaNivel).toMatchObject({ tipo: 'ACUMULACION', puntos: '50.00', nivelAplicado: 'Plata', promocionNombre: null });
    expect(filaPromo).toMatchObject({ tipo: 'ACUMULACION', puntos: '30.00', promocionNombre: 'Buen Fin', promocionPorcentaje: '3' });
  });

  it('cada abono prorratea su propia promoción sobre su propio monto', async () => {
    mockCard(makeCard({ saldoPuntos: '0.00' }));
    configService.obtenerPromocionVigente.mockResolvedValue({ nombre: 'Buen Fin', porcentajeBono: '10' });
    const payload = {
      ...PAYLOAD_BASE,
      abonos: [
        abono({ idAbono: 'ABONO-1', subtotal: 1000, total: 1160, puntosAbonados: 50 }),
        abono({ idAbono: 'ABONO-2', subtotal: 200, total: 232, formasPago: [{ formaPagoId: 'FP-EFECTIVO', importe: 232 }], puntosAbonados: 10 }),
      ],
    };
    const res = await ventaService.procesarVentaKore(payload);
    // Abono 1: nivel 50 + promo (1000*10%=100) = 150. Abono 2: nivel 10 + promo (200*10%=20) = 30.
    expect(res.abonos[0].puntosAcumulados).toBe(150);
    expect(res.abonos[1].puntosAcumulados).toBe(30);
    expect(res.puntosAcumulados).toBe(180);
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

describe('procesarLoteVentasKore', () => {
  it('ventas vacío → 400', async () => {
    await expect(ventaService.procesarLoteVentasKore([])).rejects.toMatchObject({ statusCode: 400 });
  });

  it('ventas no es un arreglo → 400', async () => {
    await expect(ventaService.procesarLoteVentasKore(undefined)).rejects.toMatchObject({ statusCode: 400 });
    await expect(ventaService.procesarLoteVentasKore({})).rejects.toMatchObject({ statusCode: 400 });
  });

  it('más de 100 ventas en un lote → 400, no procesa ninguna', async () => {
    const ventas = Array.from({ length: 101 }, (_, i) => ({ ...PAYLOAD_BASE, serieFolio: `A0-${260605320 + i}` }));
    await expect(ventaService.procesarLoteVentasKore(ventas)).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyPointsLedger.create).not.toHaveBeenCalled();
  });

  it('procesa varias ventas reusando procesarVentaKore, cada una independiente', async () => {
    mockCard(makeCard({ saldoPuntos: '0.00' }));
    const ventas = [
      { ...PAYLOAD_BASE, serieFolio: 'A0-260605320' },
      { ...PAYLOAD_BASE, serieFolio: 'A0-260605321' },
    ];
    const { resultados } = await ventaService.procesarLoteVentasKore(ventas);
    expect(resultados).toHaveLength(2);
    expect(resultados[0]).toMatchObject({ serieFolio: 'A0-260605320', success: true, status: 'aplicado' });
    expect(resultados[1]).toMatchObject({ serieFolio: 'A0-260605321', success: true, status: 'aplicado' });
    expect(LoyaltyPointsLedger.create).toHaveBeenCalledTimes(2); // una fila ACUMULACION por venta
  });

  it('una venta inválida dentro del lote NO aborta las demás', async () => {
    mockCard(makeCard({ saldoPuntos: '0.00' }));
    const ventas = [
      { ...PAYLOAD_BASE, serieFolio: 'A0-260605320' },
      { ...PAYLOAD_BASE, serieFolio: 'invalido' }, // formato de Serie-Folio inválido → 400 individual
      { ...PAYLOAD_BASE, serieFolio: 'A0-260605322' },
    ];
    const { resultados } = await ventaService.procesarLoteVentasKore(ventas);
    expect(resultados).toHaveLength(3);
    expect(resultados[0]).toMatchObject({ success: true });
    expect(resultados[1]).toMatchObject({ serieFolio: 'invalido', success: false, statusCode: 400 });
    expect(resultados[1].error).toEqual(expect.any(String));
    expect(resultados[2]).toMatchObject({ success: true });
  });
});

// Notificación en vivo (2026-11-10, pedido explícito del usuario): UNA sola emisión por webhook,
// a la sala de la tarjeta puntual — nunca una por abono (ver docstring de procesarVentaKore).
// `cardId` es interno (orienta el socket) y nunca debe viajar en la respuesta pública del webhook.
describe('procesarVentaKore — notificación en vivo (emitToLoyaltyCard)', () => {
  it('emite lealtad-tarjeta:actualizada UNA sola vez aunque el request traiga 2 abonos', async () => {
    mockCard(makeCard({ id: 77 }));
    const payload = {
      ...PAYLOAD_BASE,
      abonos: [abono({ idAbono: 'ABONO-1' }), abono({ idAbono: 'ABONO-2' })],
    };

    await ventaService.procesarVentaKore(payload);

    expect(emitToLoyaltyCard).toHaveBeenCalledTimes(1);
    expect(emitToLoyaltyCard).toHaveBeenCalledWith(77, 'lealtad-tarjeta:actualizada', { cardId: 77, motivo: 'venta-kore' });
  });

  it('el cardId interno nunca viaja en la respuesta pública del webhook', async () => {
    mockCard(makeCard({ id: 77 }));
    const res = await ventaService.procesarVentaKore(PAYLOAD_BASE);
    expect(res.cardId).toBeUndefined();
  });

  it('blindaje cruzado manual↔kore: también emite (nada nuevo se escribió, pero es inofensivo)', async () => {
    mockCard(makeCard({ id: 78, saldoPuntos: '50.00' }));
    mockLedgerRows({
      manual: [
        { tipo: 'ACUMULACION', puntos: '50.00', montoVenta: '1000.00', referenciaExterna: 'A0-260605320', origen: 'manual' },
      ],
    });

    await ventaService.procesarVentaKore(PAYLOAD_BASE);

    expect(emitToLoyaltyCard).toHaveBeenCalledWith(78, 'lealtad-tarjeta:actualizada', { cardId: 78, motivo: 'venta-kore' });
  });

  it('error de validación (400) antes de resolver la tarjeta: nunca emite', async () => {
    await expect(ventaService.procesarVentaKore({ telefono: '5512345678' })).rejects.toMatchObject({ statusCode: 400 });
    expect(emitToLoyaltyCard).not.toHaveBeenCalled();
  });
});
