'use strict';

// caja-transferencia-match.service.test.js — Fase C del proceso de matching de
// transferencias entre cajas: buscarCandidatos() (SOLO 1:1 exacto por monto+ventana,
// sin acotar por banco — combinaciones de 2+ movimientos se quitaron el 2026-09-08,
// ver comentario en el service).
//
// bank.service.js NO se mockea — solo se usa para leer la constante real
// ERP_TOLERANCE, sin tocar Mongo (requerir el módulo no hace I/O).
jest.mock('../banks/BankMovement.model');
jest.mock('../../../shared/services/global-config.service');
jest.mock('./CajaTransferencia.model');

const BankMovement       = require('../banks/BankMovement.model');
const CajaTransferencia  = require('./CajaTransferencia.model');
const globalConfigService = require('../../../shared/services/global-config.service');
const {
  buscarCandidatos, reclasificarHistoricasDescartadas, _buscarCoincidenciasHistoricas,
  FECHA_CORTE_LOGICA_HISTORICA, _ventanaDias, _normalizarCategoria, VENTANA_DEFAULT_DIAS,
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

  // CORRECCIÓN 2026-09-08 (bug real reportado por el usuario, caso real de producción):
  // `.find()` devolvía solo el PRIMERO de varios candidatos empatados en monto (orden
  // natural de Mongo, sin ninguna señal real de cuál es el correcto) — los otros
  // quedaban invisibles. Ahora TODOS los que empatan se devuelven, cada uno como su
  // propio grupo de 1 elemento, para que un humano elija.
  test('varios movimientos empatan EXACTO en monto (caso real: 3 depósitos de $1,200): devuelve los 3, cada uno su propio grupo', async () => {
    const movA = { _id: 'mov-a', categoria: CATEGORIA, deposito: 1200 };
    const movB = { _id: 'mov-b', categoria: CATEGORIA, deposito: 1200 };
    const movC = { _id: 'mov-c', categoria: CATEGORIA, deposito: 1200 };
    BankMovement.find = jest.fn(() => fakeFind([movA, movB, movC]));

    const candidatos = await buscarCandidatos({ monto: 1200, fechaRecepcion: new Date() });

    expect(candidatos).toEqual([[movA], [movB], [movC]]);
  });

  test('dentro de tolerancia ($1 MXN, ERP_TOLERANCE): cuenta como match exacto', async () => {
    const mov = { _id: 'mov-1', categoria: CATEGORIA, deposito: 1500.5 };
    BankMovement.find = jest.fn(() => fakeFind([mov]));

    const candidatos = await buscarCandidatos({ monto: 1500, fechaRecepcion: new Date() });

    expect(candidatos).toEqual([[mov]]);
  });

  // CORRECCIÓN 2026-09-08 (caso real de producción reportado por el usuario): el monto
  // exacto de una transferencia ya estaba 'identificado' (excluido del pool), y el
  // fallback de pares encontró 2 movimientos NO relacionados cuya suma coincidía por
  // pura casualidad numérica — falso positivo. Se quitó la búsqueda de pares: aunque
  // exista una combinación de 2 que sume el monto, NO debe sugerirse.
  test('sin match 1:1 pero existe un par cuya suma matchea: NO se sugiere (solo 1:1 por ahora)', async () => {
    const movA = { _id: 'mov-a', categoria: CATEGORIA, deposito: 1000 };
    const movB = { _id: 'mov-b', categoria: CATEGORIA, deposito: 500 };
    BankMovement.find = jest.fn(() => fakeFind([movA, movB]));

    const candidatos = await buscarCandidatos({ monto: 1500, fechaRecepcion: new Date() });

    expect(candidatos).toEqual([]);
  });

  test('ningún movimiento matchea: []', async () => {
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

describe('_buscarCoincidenciasHistoricas', () => {
  beforeEach(() => {
    globalConfigService.getValue.mockResolvedValue('5');
  });

  test('consulta por fecha SOLAMENTE — a diferencia de buscarCandidatos, no excluye erpLinks/status', async () => {
    BankMovement.find = jest.fn(() => fakeFind([]));
    await _buscarCoincidenciasHistoricas({ monto: 100, fechaRecepcion: new Date('2026-06-01T00:00:00Z') });

    const filtro = BankMovement.find.mock.calls[0][0];
    expect(filtro.erpLinks).toBeUndefined();
    expect(filtro.status).toBeUndefined();
    expect(filtro.fecha.$gte).toBeInstanceOf(Date);
  });

  test('encuentra un depósito ya identificado que calza exacto por monto', async () => {
    const mov = { _id: 'mov-1', categoria: CATEGORIA, deposito: 1500, status: 'identificado' };
    BankMovement.find = jest.fn(() => fakeFind([mov]));

    const resultado = await _buscarCoincidenciasHistoricas({ monto: 1500, fechaRecepcion: new Date() });

    expect(resultado).toEqual([mov]);
  });

  test('sin fechaRecepcion: no consulta Mongo, devuelve []', async () => {
    const resultado = await _buscarCoincidenciasHistoricas({ monto: 100, fechaRecepcion: null });
    expect(resultado).toEqual([]);
    expect(BankMovement.find).not.toHaveBeenCalled();
  });
});

describe('reclasificarHistoricasDescartadas', () => {
  beforeEach(() => {
    globalConfigService.getValue.mockResolvedValue('5');
    CajaTransferencia.updateOne = jest.fn().mockResolvedValue({});
  });

  function fakeCajaFind(result) {
    return { lean: jest.fn().mockResolvedValue(result) };
  }

  test('solo revisa pendientes anteriores a FECHA_CORTE_LOGICA_HISTORICA, no excluidas por filtro', async () => {
    CajaTransferencia.find = jest.fn(() => fakeCajaFind([]));

    await reclasificarHistoricasDescartadas();

    expect(CajaTransferencia.find).toHaveBeenCalledWith({
      estatusMatch: 'pendiente',
      excluidaPorFiltro: { $ne: true },
      fechaRecepcion: { $lt: FECHA_CORTE_LOGICA_HISTORICA },
    });
  });

  test('tiene candidato accionable: no se toca, sigue pendiente', async () => {
    const t = { _id: 't1', monto: 100, fechaRecepcion: new Date('2026-06-01T00:00:00Z') };
    CajaTransferencia.find = jest.fn(() => fakeCajaFind([t]));
    BankMovement.find = jest.fn(() => fakeFind([{ _id: 'mov-1', categoria: CATEGORIA, deposito: 100 }]));

    const resultado = await reclasificarHistoricasDescartadas();

    expect(CajaTransferencia.updateOne).not.toHaveBeenCalled();
    expect(resultado).toEqual({ revisadas: 1, descartadas: 0 });
  });

  test('sin candidato accionable pero con match histórico ya identificado: se marca "descartada"', async () => {
    const t = { _id: 't1', monto: 100, fechaRecepcion: new Date('2026-06-01T00:00:00Z') };
    CajaTransferencia.find = jest.fn(() => fakeCajaFind([t]));
    BankMovement.find = jest.fn()
      .mockImplementationOnce(() => fakeFind([])) // buscarCandidatos: sin accionables
      .mockImplementationOnce(() => fakeFind([{ _id: 'mov-1', categoria: CATEGORIA, deposito: 100, status: 'identificado' }]));

    const resultado = await reclasificarHistoricasDescartadas();

    expect(CajaTransferencia.updateOne).toHaveBeenCalledWith({ _id: 't1' }, { $set: { estatusMatch: 'descartada' } });
    expect(resultado).toEqual({ revisadas: 1, descartadas: 1 });
  });

  // Decisión explícita del usuario 2026-09-09: un empate histórico donde TODOS los
  // depósitos que calzan ya están identificado también se descarta — no hay nada
  // accionable para un humano, sin importar si hay 1 o varios "atados".
  test('empate histórico con 2 depósitos, AMBOS ya identificado: también se descarta', async () => {
    const t = { _id: 't1', monto: 100, fechaRecepcion: new Date('2026-06-01T00:00:00Z') };
    CajaTransferencia.find = jest.fn(() => fakeCajaFind([t]));
    BankMovement.find = jest.fn()
      .mockImplementationOnce(() => fakeFind([]))
      .mockImplementationOnce(() => fakeFind([
        { _id: 'mov-1', categoria: CATEGORIA, deposito: 100, status: 'identificado' },
        { _id: 'mov-2', categoria: CATEGORIA, deposito: 100, status: 'identificado' },
      ]));

    const resultado = await reclasificarHistoricasDescartadas();

    expect(CajaTransferencia.updateOne).toHaveBeenCalledWith({ _id: 't1' }, { $set: { estatusMatch: 'descartada' } });
    expect(resultado).toEqual({ revisadas: 1, descartadas: 1 });
  });

  test('sin ningún match, ni siquiera histórico: queda pendiente (huérfana real)', async () => {
    const t = { _id: 't1', monto: 100, fechaRecepcion: new Date('2026-06-01T00:00:00Z') };
    CajaTransferencia.find = jest.fn(() => fakeCajaFind([t]));
    BankMovement.find = jest.fn(() => fakeFind([]));

    const resultado = await reclasificarHistoricasDescartadas();

    expect(CajaTransferencia.updateOne).not.toHaveBeenCalled();
    expect(resultado).toEqual({ revisadas: 1, descartadas: 0 });
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
