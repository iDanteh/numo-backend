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
  obtenerBandejaNetpay, _ventanaDiasNetpay, _agruparPorTerminalYDia, _diaUTC,
  VENTANA_DEFAULT_DIAS,
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
      terminalID: '2840403056', almacen: 'A0', dia: _diaUTC('2026-09-10T14:00:00Z'),
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
      { terminalID: '2840403056', dia: _diaUTC('2026-09-10T14:00:00Z') },
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

  test('pasa dateFrom/dateTo/terminalID tal cual a consultarTransaccionesNetpay', async () => {
    consultarTransaccionesNetpay.mockResolvedValue({ transacciones: [] });
    await obtenerBandejaNetpay({ dateFrom: '2026-09-01T00:00:00Z', dateTo: '2026-09-15T23:59:59Z', terminalID: '2840403056' });

    expect(consultarTransaccionesNetpay).toHaveBeenCalledWith({
      dateFrom: '2026-09-01T00:00:00Z', dateTo: '2026-09-15T23:59:59Z', terminalID: '2840403056',
    });
  });
});
