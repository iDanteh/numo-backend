'use strict';

// caja-transferencia-match.service.test.js — Fase C del proceso de matching de
// transferencias entre cajas: buscarCandidatos() (1:1 y 1:2 por monto+ventana,
// sin acotar por banco).
//
// bank.service.js NO se mockea — solo se usa para leer la constante real
// ERP_TOLERANCE, sin tocar Mongo (requerir el módulo no hace I/O).
jest.mock('../banks/BankMovement.model');
jest.mock('../../../shared/services/global-config.service');

const BankMovement       = require('../banks/BankMovement.model');
const globalConfigService = require('../../../shared/services/global-config.service');
const {
  buscarCandidatos, _ventanaDias, _normalizarCategoria, VENTANA_DEFAULT_DIAS,
} = require('./caja-transferencia-match.service');

const CATEGORIA = 'Depósito en efectivo'; // forma "canónica" usada en los fixtures de este archivo

function fakeFind(result) {
  return { lean: jest.fn().mockResolvedValue(result) };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('_ventanaDias', () => {
  test('config sin sembrar: usa el default interno', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error('No existe la configuración bancos.X'));
    expect(await _ventanaDias()).toBe(VENTANA_DEFAULT_DIAS);
  });

  test('config con un valor numérico válido: lo usa', async () => {
    globalConfigService.getValue.mockResolvedValue('7');
    expect(await _ventanaDias()).toBe(7);
  });

  test('config con basura (no numérica): usa el default interno', async () => {
    globalConfigService.getValue.mockResolvedValue('no-es-numero');
    expect(await _ventanaDias()).toBe(VENTANA_DEFAULT_DIAS);
  });
});

describe('buscarCandidatos', () => {
  beforeEach(() => {
    globalConfigService.getValue.mockResolvedValue('5');
  });

  test('sin fechaRecepcion: no consulta Mongo, devuelve []', async () => {
    const candidatos = await buscarCandidatos({ monto: 100, fechaRecepcion: null });
    expect(candidatos).toEqual([]);
    expect(BankMovement.find).not.toHaveBeenCalled();
  });

  // Bug real 2026-09-03 (reportado por el usuario): un movimiento puede quedar
  // 'identificado' sin ningún erpLink — vía `ficha` (folio físico que carga un
  // contador, ver bank.service.js#aplicarLogicaErp). Filtrar solo por erpLinks
  // vacío no alcanza: la bandeja sugería como candidatos depósitos que un contador
  // YA había resuelto a mano. La query debe excluir status:'identificado' también.
  test('consulta por erpLinks+status+fecha (categoria NO se filtra en Mongo — es texto libre, se normaliza en JS)', async () => {
    BankMovement.find = jest.fn(() => fakeFind([]));
    await buscarCandidatos({ monto: 100, fechaRecepcion: new Date('2026-09-01T00:00:00Z') });

    expect(BankMovement.find).toHaveBeenCalledTimes(1);
    const filtro = BankMovement.find.mock.calls[0][0];
    expect(filtro.categoria).toBeUndefined();
    expect(filtro.erpLinks).toEqual({ $size: 0 });
    expect(filtro.status).toEqual({ $ne: 'identificado' });
    expect(filtro.fecha.$gte).toBeInstanceOf(Date);
    expect(filtro.fecha.$lte).toBeInstanceOf(Date);
  });

  test('match 1:1 exacto: devuelve un solo grupo con ese movimiento', async () => {
    const mov = { _id: 'mov-1', categoria: CATEGORIA, deposito: 1500 };
    BankMovement.find = jest.fn(() => fakeFind([mov, { _id: 'mov-2', categoria: CATEGORIA, deposito: 300 }]));

    const candidatos = await buscarCandidatos({ monto: 1500, fechaRecepcion: new Date() });

    expect(candidatos).toEqual([[mov]]);
  });

  test('dentro de tolerancia ($1 MXN, ERP_TOLERANCE): cuenta como match exacto', async () => {
    const mov = { _id: 'mov-1', categoria: CATEGORIA, deposito: 1500.5 };
    BankMovement.find = jest.fn(() => fakeFind([mov]));

    const candidatos = await buscarCandidatos({ monto: 1500, fechaRecepcion: new Date() });

    expect(candidatos).toEqual([[mov]]);
  });

  test('sin match 1:1 pero sí un par cuya suma matchea (caso real: límite de depósito por banco)', async () => {
    const movA = { _id: 'mov-a', categoria: CATEGORIA, deposito: 1000 };
    const movB = { _id: 'mov-b', categoria: CATEGORIA, deposito: 500 };
    const movC = { _id: 'mov-c', categoria: CATEGORIA, deposito: 200 }; // no participa de ningún match
    BankMovement.find = jest.fn(() => fakeFind([movA, movB, movC]));

    const candidatos = await buscarCandidatos({ monto: 1500, fechaRecepcion: new Date() });

    expect(candidatos).toEqual([[movA, movB]]);
  });

  test('ningún movimiento ni combinación de 2 matchea: []', async () => {
    BankMovement.find = jest.fn(() => fakeFind([
      { _id: 'mov-1', categoria: CATEGORIA, deposito: 100 },
      { _id: 'mov-2', categoria: CATEGORIA, deposito: 50 },
    ]));

    const candidatos = await buscarCandidatos({ monto: 999, fechaRecepcion: new Date() });

    expect(candidatos).toEqual([]);
  });

  // Bug real 2026-09-01 (reportado por el usuario): la regla de categorización de este
  // ambiente se llama "DEPOSITO EN EFECTIVO" (mayúsculas, sin acento) — con un `===`
  // exacto contra 'Depósito en efectivo', TODAS las transferencias mostraban "Sin
  // candidatos" sin importar fecha/monto. La comparación normaliza mayúsculas+acentos.
  test('reconoce la categoría sin importar mayúsculas/acentos (caso real: regla "DEPOSITO EN EFECTIVO")', async () => {
    const mov = { _id: 'mov-1', categoria: 'DEPOSITO EN EFECTIVO', deposito: 1000 };
    BankMovement.find = jest.fn(() => fakeFind([mov]));

    const candidatos = await buscarCandidatos({ monto: 1000, fechaRecepcion: new Date() });

    expect(candidatos).toEqual([[mov]]);
  });

  test('descarta movimientos con OTRA categoría, aunque el monto/fecha calcen', async () => {
    BankMovement.find = jest.fn(() => fakeFind([{ _id: 'mov-1', categoria: 'Traspaso entre cuentas propias', deposito: 1000 }]));

    const candidatos = await buscarCandidatos({ monto: 1000, fechaRecepcion: new Date() });

    expect(candidatos).toEqual([]);
  });

  test('descarta movimientos sin categoria (null)', async () => {
    BankMovement.find = jest.fn(() => fakeFind([{ _id: 'mov-1', categoria: null, deposito: 1000 }]));

    const candidatos = await buscarCandidatos({ monto: 1000, fechaRecepcion: new Date() });

    expect(candidatos).toEqual([]);
  });
});

describe('_normalizarCategoria', () => {
  test('mayúsculas, acentos y espacios de sobra no importan', () => {
    expect(_normalizarCategoria('Depósito en efectivo')).toBe('DEPOSITO EN EFECTIVO');
    expect(_normalizarCategoria('DEPOSITO EN EFECTIVO')).toBe('DEPOSITO EN EFECTIVO');
    expect(_normalizarCategoria('  depósito EN Efectivo  ')).toBe('DEPOSITO EN EFECTIVO');
  });

  test('null/undefined no revientan, dan string vacío', () => {
    expect(_normalizarCategoria(null)).toBe('');
    expect(_normalizarCategoria(undefined)).toBe('');
  });
});
