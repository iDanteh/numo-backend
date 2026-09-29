'use strict';

// netpay-match.service.test.js — Fase C del matching Netpay↔BBVA: obtenerBandejaNetpay()
// agrupa transacciones (ya traídas por consultarTransaccionesNetpay, mockeada acá) por
// almacen+terminalID+día, descarta lo ya resuelto (NetpayMatch) y busca candidatos BBVA
// dentro de tolerancia/ventana. bank.service.js NO se mockea — solo se usa para leer la
// constante real ERP_TOLERANCE, sin tocar Mongo.
jest.mock('../banks/BankMovement.model');
jest.mock('../../../shared/services/global-config.service');
jest.mock('./NetpayMatch.model');
jest.mock('./netpay-transacciones.service');

const BankMovement = require('../banks/BankMovement.model');
const NetpayMatch = require('./NetpayMatch.model');
const globalConfigService = require('../../../shared/services/global-config.service');
const { consultarTransaccionesNetpay } = require('./netpay-transacciones.service');
const {
  obtenerBandejaNetpay, _ventanaDiasNetpay, _agruparPorTerminalYDia, _diaMx, _normalizarMarcadorDia,
  VENTANA_DEFAULT_DIAS, _agruparPorTerminalDiaYMarca, _marcasDiferidas, MARCAS_DIFERIDAS_DEFAULT,
} = require('./netpay-match.service');

function fakeFind(result) {
  return { lean: jest.fn().mockResolvedValue(result) };
}

function t(overrides = {}) {
  return {
    amount: 100, commission: 10, almacen: 'A0', terminalID: '2840403056',
    transactionDate: '2026-09-10T14:00:00Z',
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  globalConfigService.getValue.mockResolvedValue('2');
  NetpayMatch.find = jest.fn(() => fakeFind([]));
});

describe('_ventanaDiasNetpay', () => {
  test('config sin sembrar: usa el default interno', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error('No existe la configuración bancos.X'));
    expect(await _ventanaDiasNetpay()).toBe(VENTANA_DEFAULT_DIAS);
  });

  test('config con un valor numérico válido: lo usa', async () => {
    globalConfigService.getValue.mockResolvedValue('4');
    expect(await _ventanaDiasNetpay()).toBe(4);
  });

  test('config con basura: usa el default interno', async () => {
    globalConfigService.getValue.mockResolvedValue('no-es-numero');
    expect(await _ventanaDiasNetpay()).toBe(VENTANA_DEFAULT_DIAS);
  });
});

describe('_agruparPorTerminalYDia', () => {
  test('agrupa por terminal+día, suma monto/comisión, calcula neto', () => {
    const grupos = _agruparPorTerminalYDia([
      t({ amount: 100, commission: 10 }),
      t({ amount: 200, commission: 20 }),
    ]);
    expect(grupos).toEqual([{
      terminalID: '2840403056', almacen: 'A0', dia: _diaMx('2026-09-10T14:00:00Z'),
      montoBruto: 300, comision: 30, cantidadTransacciones: 2, netoEsperado: 270,
    }]);
  });

  test('terminales o días distintos: grupos separados', () => {
    const grupos = _agruparPorTerminalYDia([
      t({ terminalID: 'T1', transactionDate: '2026-09-10T10:00:00Z' }),
      t({ terminalID: 'T2', transactionDate: '2026-09-10T10:00:00Z' }),
      t({ terminalID: 'T1', transactionDate: '2026-09-11T10:00:00Z' }),
    ]);
    expect(grupos.length).toBe(3);
  });

  // CORRECCIÓN 2026-09-22 (bug real, mismo de fondo que la ventana de consulta): antes
  // (_diaUTC, sin desplazar) una transacción de las 18:00-23:59 hora MX quedaba agrupada
  // bajo el día calendario SIGUIENTE en UTC — el día equivocado desde el punto de vista
  // del negocio (la tienda cerró ese depósito el día D, no D+1).
  test('una transacción a las 22:00 hora MX del día 10 (=04:00 UTC del día 11) se agrupa bajo el día 10, no el 11', () => {
    const grupos = _agruparPorTerminalYDia([t({ transactionDate: '2026-09-11T04:00:00Z' })]);
    expect(grupos[0].dia).toEqual(_diaMx('2026-09-11T04:00:00Z'));
    expect(grupos[0].dia.toISOString()).toBe('2026-09-10T00:00:00.000Z');
  });

  test('una transacción a las 10:00 hora MX (=16:00 UTC, bien lejos del corte) se agrupa bajo el mismo día en ambos criterios', () => {
    const grupos = _agruparPorTerminalYDia([t({ transactionDate: '2026-09-10T16:00:00Z' })]);
    expect(grupos[0].dia.toISOString()).toBe('2026-09-10T00:00:00.000Z');
  });
});

describe('_normalizarMarcadorDia — trunca un marcador YA bucketizado, sin desplazar (a diferencia de _diaMx)', () => {
  test('un marcador de medianoche UTC se mantiene igual (no lo corre un día para atrás)', () => {
    expect(_normalizarMarcadorDia('2026-09-10T00:00:00.000Z').toISOString()).toBe('2026-09-10T00:00:00.000Z');
  });

  test('acepta también un Date ya truncado, idempotente', () => {
    const marcador = _diaMx('2026-09-10T22:00:00Z'); // => 2026-09-10T00:00:00.000Z
    expect(_normalizarMarcadorDia(marcador).getTime()).toBe(marcador.getTime());
  });
});

// _marcasDiferidas — netpay-matching-v2 (design.md "Lagging brand list"): config global
// bancos.NETPAY_MARCAS_DIFERIDAS (CSV), mismo patrón que _ventanaDiasNetpay. Default AMEX
// cuando la config no está sembrada.
describe('_marcasDiferidas', () => {
  test('config sin sembrar: usa el default interno (AMEX)', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error('No existe la configuración bancos.X'));
    expect(await _marcasDiferidas()).toEqual(MARCAS_DIFERIDAS_DEFAULT);
    expect(MARCAS_DIFERIDAS_DEFAULT).toEqual(['AMEX']);
  });

  test('config con una sola marca: la usa en mayúsculas', async () => {
    globalConfigService.getValue.mockResolvedValue('amex');
    expect(await _marcasDiferidas()).toEqual(['AMEX']);
  });

  test('config con varias marcas separadas por coma: arreglo con las 3, normalizado', async () => {
    globalConfigService.getValue.mockResolvedValue('AMEX, diners , JCB');
    expect(await _marcasDiferidas()).toEqual(['AMEX', 'DINERS', 'JCB']);
  });

  test('config vacía/basura: usa el default interno', async () => {
    globalConfigService.getValue.mockResolvedValue('   ');
    expect(await _marcasDiferidas()).toEqual(MARCAS_DIFERIDAS_DEFAULT);
  });
});

// _agruparPorTerminalDiaYMarca — netpay-matching-v2 (design.md): la unidad de decisión
// pasa a ser (terminalID, dia, bucket). bucket='general' salvo que cardTypeName EMPIECE
// con una de las marcas diferidas configuradas (NUNCA cardType, ese campo se ignora a
// propósito — proposal: "split each terminal-day by cardTypeName, not cardType").
describe('_agruparPorTerminalDiaYMarca', () => {
  test('día sin ninguna marca diferida: 1 solo bucket general, mismos valores que la función legacy _agruparPorTerminalYDia (regresión)', () => {
    const transacciones = [
      t({ amount: 100, commission: 10, cardTypeName: 'VISA' }),
      t({ amount: 200, commission: 20, cardTypeName: 'MASTERCARD' }),
    ];
    const legacy = _agruparPorTerminalYDia(transacciones);
    const nuevo = _agruparPorTerminalDiaYMarca(transacciones, ['AMEX']);

    expect(nuevo).toHaveLength(1);
    expect(nuevo[0].bucket).toBe('general');
    expect(nuevo[0].montoBruto).toBe(legacy[0].montoBruto);
    expect(nuevo[0].comision).toBe(legacy[0].comision);
    expect(nuevo[0].netoEsperado).toBe(legacy[0].netoEsperado);
    expect(nuevo[0].dia).toEqual(legacy[0].dia);
  });

  test('cardTypeName ausente (undefined/null): cae en general', () => {
    const nuevo = _agruparPorTerminalDiaYMarca([t({ cardTypeName: undefined }), t({ cardTypeName: null })], ['AMEX']);
    expect(nuevo).toHaveLength(1);
    expect(nuevo[0].bucket).toBe('general');
  });

  // Proposal + spec: "split each terminal-day by cardTypeName, not cardType" — cardType
  // (un campo DISTINTO, código legacy de Kore) NUNCA debe decidir el bucket.
  test('cardType (campo distinto de cardTypeName) se ignora por completo', () => {
    const nuevo = _agruparPorTerminalDiaYMarca([t({ cardTypeName: 'VISA', cardType: 'AMEX' })], ['AMEX']);
    expect(nuevo).toHaveLength(1);
    expect(nuevo[0].bucket).toBe('general');
  });

  // $60,758.23 fixture (proposal): un AMEX entre Visa/Mastercard del mismo terminal+día ->
  // 2 buckets separados, AMEX aparte del general.
  test('una venta AMEX entre Visa/Mastercard del mismo terminal+día: 2 buckets separados (general y AMEX)', () => {
    const transacciones = [
      t({ amount: 48099.79, commission: 418.39, cardTypeName: 'VISA' }),
      t({ amount: 12658.44, commission: 513.932664, cardTypeName: 'AMEX' }),
    ];
    const grupos = _agruparPorTerminalDiaYMarca(transacciones, ['AMEX']);

    expect(grupos).toHaveLength(2);
    const general = grupos.find(g => g.bucket === 'general');
    const amex = grupos.find(g => g.bucket === 'AMEX');
    expect(general.netoEsperado).toBeCloseTo(48099.79 - 418.39);
    expect(amex.netoEsperado).toBeCloseTo(12658.44 - 513.932664);
  });

  test('cardTypeName que EMPIEZA con una marca diferida configurada (no coincidencia exacta): va a ese bucket', () => {
    const grupos = _agruparPorTerminalDiaYMarca([t({ cardTypeName: 'AMEX CREDITO' })], ['AMEX']);
    expect(grupos[0].bucket).toBe('AMEX');
  });

  test('terminales o días distintos siguen separando grupos igual que antes, ahora también por bucket', () => {
    const grupos = _agruparPorTerminalDiaYMarca([
      t({ terminalID: 'T1', cardTypeName: 'VISA' }),
      t({ terminalID: 'T1', cardTypeName: 'AMEX' }),
      t({ terminalID: 'T2', cardTypeName: 'VISA' }),
    ], ['AMEX']);
    expect(grupos.length).toBe(3);
  });
});

describe('obtenerBandejaNetpay', () => {
  test('sin transacciones: pendientes []', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [] });
    const resultado = await obtenerBandejaNetpay({});
    expect(resultado).toEqual({ pendientes: [] });
    expect(BankMovement.find).not.toHaveBeenCalled();
  });

  test('1 candidato BBVA exacto dentro de tolerancia: aparece en pendientes', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [t({ amount: 300, commission: 20 })] });
    const mov = { _id: 'mov-1', banco: 'BBVA', deposito: 280, fecha: new Date('2026-09-10T00:00:00Z') };
    BankMovement.find = jest.fn(() => fakeFind([mov]));

    const resultado = await obtenerBandejaNetpay({});

    expect(resultado.pendientes.length).toBe(1);
    expect(resultado.pendientes[0].grupo.netoEsperado).toBe(280);
    expect(resultado.pendientes[0].candidatos).toEqual([[mov]]);
  });

  test('candidato fuera de tolerancia ($1 MXN): no aparece', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [t({ amount: 300, commission: 20 })] });
    const mov = { _id: 'mov-1', banco: 'BBVA', deposito: 275, fecha: new Date('2026-09-10T00:00:00Z') };
    BankMovement.find = jest.fn(() => fakeFind([mov]));

    const resultado = await obtenerBandejaNetpay({});

    expect(resultado.pendientes[0].candidatos).toEqual([]);
  });

  test('sin ningún BankMovement BBVA elegible: grupo pendiente sin candidatos', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [t()] });
    BankMovement.find = jest.fn(() => fakeFind([]));

    const resultado = await obtenerBandejaNetpay({});

    expect(resultado.pendientes.length).toBe(1);
    expect(resultado.pendientes[0].candidatos).toEqual([]);
  });

  test('grupo YA resuelto (existe NetpayMatch para terminalID+día): no aparece en pendientes', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [t()] });
    NetpayMatch.find = jest.fn(() => fakeFind([
      { terminalID: '2840403056', dia: _diaMx('2026-09-10T14:00:00Z') },
    ]));

    const resultado = await obtenerBandejaNetpay({});

    expect(resultado.pendientes).toEqual([]);
    expect(BankMovement.find).not.toHaveBeenCalled();
  });

  test('consulta BankMovement por banco BBVA, erpLinks vacío, status distinto de identificado', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [t()] });
    BankMovement.find = jest.fn(() => fakeFind([]));

    await obtenerBandejaNetpay({});

    const filtro = BankMovement.find.mock.calls[0][0];
    expect(filtro.banco).toBe('BBVA');
    expect(filtro.erpLinks).toEqual({ $size: 0 });
    expect(filtro.status).toEqual({ $ne: 'identificado' });
  });

  // dateFrom/dateTo viajan pelados (YYYY-MM-DD, 2026-09-22) — es consultarTransaccionesNetpay
  // (mockeada acá) quien arma el instante UTC real en hora MX, no esta función.
  test('pasa dateFrom/dateTo/terminalID tal cual a consultarTransaccionesNetpay', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [] });
    await obtenerBandejaNetpay({ dateFrom: '2026-09-01', dateTo: '2026-09-15', terminalID: '2840403056' });

    expect(consultarTransaccionesNetpay).toHaveBeenCalledWith({
      dateFrom: '2026-09-01', dateTo: '2026-09-15', terminalID: '2840403056',
    });
  });
});
