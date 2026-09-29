'use strict';

// migrate-netpay-v2.test.js — netpay-matching-v2 (Phase 4, design.md "Migration
// Classification Rule" + "Migration / Rollout"). Unit/integration tests for the CLI wrapper
// (scripts/migrate-netpay-v2.js), mocking the 3 Mongoose models (same jest.mock() convention
// as src/banks/domains/erp/*.test.js) — netpay-migracion.service.js (the pure classifier,
// PR2) is NEVER mocked here, we want the real classification rules exercised end to end.
//
// ABSOLUTELY no test in this file connects to any live/production/synced Mongo — every
// model call is a jest mock. Task 4.2 (running --dry-run against a real snapshot) is
// explicitly deferred to the user, not simulated here.
jest.mock('../src/banks/domains/erp/NetpayMatch.model');
jest.mock('../src/banks/domains/erp/NetpayReporte.model');
jest.mock('../src/banks/domains/banks/BankMovement.model');

const mongoose = require('mongoose');
const NetpayMatch = require('../src/banks/domains/erp/NetpayMatch.model');
const NetpayReporte = require('../src/banks/domains/erp/NetpayReporte.model');
const BankMovement = require('../src/banks/domains/banks/BankMovement.model');

const {
  parseArgs,
  clasificarMatches,
  clasificarReportes,
  clasificarTodo,
  imprimirResumen,
  runDryRun,
  runApply,
  runRevert,
  main,
  _dropearIndiceViejo,
  _bulkOpsMatches,
  _bulkOpsReportes,
  _tieneLinkMatch,
  _tieneLinkReporte,
  _verificarGuardaRevert,
  _bulkOpsRevertMatches,
  _bulkOpsRevertReportes,
} = require('./migrate-netpay-v2');

function fakeFind(result) {
  return { lean: jest.fn().mockResolvedValue(result) };
}

function movMatch(overrides = {}) {
  return { _id: 'm1', erpLinks: [{ erpId: 'NETPAY-T1-2026-09-10' }], ...overrides };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('parseArgs', () => {
  test('sin flags: modo dry-run por default', () => {
    expect(parseArgs([])).toEqual({ modo: 'dry-run' });
  });

  test('--dry-run explícito: modo dry-run', () => {
    expect(parseArgs(['--dry-run'])).toEqual({ modo: 'dry-run' });
  });

  test('--apply: modo apply', () => {
    expect(parseArgs(['--apply'])).toEqual({ modo: 'apply' });
  });

  test('--revert: modo revert', () => {
    expect(parseArgs(['--revert'])).toEqual({ modo: 'revert' });
  });

  test('flag desconocido: arroja', () => {
    expect(() => parseArgs(['--foo'])).toThrow(/no reconocido/);
  });

  test('dos modos a la vez: arroja (nunca corre 2 modos mezclados)', () => {
    expect(() => parseArgs(['--apply', '--revert'])).toThrow(/un modo a la vez/);
  });
});

describe('clasificarMatches', () => {
  test('legacy matcheada con movimiento con erpLink NETPAY-: mapeado a confirmado_automatico (clasificador real, no mockeado)', async () => {
    const doc = { _id: 'd1', estatusMatch: 'matcheada', movementIdsConfirmados: ['m1'] };
    const movimientosPorId = new Map([['m1', movMatch()]]);

    const r = await clasificarMatches([doc], movimientosPorId);

    expect(r.yaMigrados).toEqual([]);
    expect(r.noMapeados).toEqual([]);
    expect(r.mapeados).toHaveLength(1);
    expect(r.mapeados[0].nuevo.estatusMatch).toBe('confirmado_automatico');
    expect(r.mapeados[0].nuevo.estatusLegacy).toBe('matcheada');
  });

  test('legacy descartada-manual: mapeado a rechazado', async () => {
    const doc = { _id: 'd2', estatusMatch: 'descartada-manual', movementIdsConfirmados: [] };
    const r = await clasificarMatches([doc], new Map());
    expect(r.mapeados[0].nuevo.estatusMatch).toBe('rechazado');
  });

  test('estado ya v2 (ej. discrepancia): va a yaMigrados, NO se reclasifica ni se toca', async () => {
    const doc = { _id: 'd3', estatusMatch: 'discrepancia', estatusLegacy: null };
    const r = await clasificarMatches([doc], new Map());
    expect(r.yaMigrados).toEqual([doc]);
    expect(r.mapeados).toEqual([]);
    expect(r.noMapeados).toEqual([]);
  });

  test('estado desconocido/corrupto: no mapeado, con el mensaje de error del clasificador', async () => {
    const doc = { _id: 'd4', estatusMatch: 'algo-raro', movementIdsConfirmados: [] };
    const r = await clasificarMatches([doc], new Map());
    expect(r.mapeados).toEqual([]);
    expect(r.noMapeados).toHaveLength(1);
    expect(r.noMapeados[0].doc).toBe(doc);
    expect(r.noMapeados[0].error).toMatch(/no reconocido/);
  });
});

describe('clasificarReportes', () => {
  test('legacy confirmado con erpLink NETPAYRPT- correcto: mapeado a resuelto_por_reporte/erp-link', async () => {
    const doc = {
      _id: 'r1', estatus: 'confirmado', claveRastreo: 'CLAVE-1', movementIdConfirmado: 'm1',
    };
    const movimientosPorId = new Map([['m1', { _id: 'm1', erpLinks: [{ erpId: 'NETPAYRPT-CLAVE-1' }] }]]);

    const r = await clasificarReportes([doc], movimientosPorId);

    expect(r.mapeados).toHaveLength(1);
    expect(r.mapeados[0].nuevo.estatus).toBe('resuelto_por_reporte');
    expect(r.mapeados[0].nuevo.vinculo).toBe('erp-link');
  });

  test('legacy pendiente: SIEMPRE discrepancia/sin_candidato', async () => {
    const doc = { _id: 'r2', estatus: 'pendiente', claveRastreo: 'C2', movementIdConfirmado: null };
    const r = await clasificarReportes([doc], new Map());
    expect(r.mapeados[0].nuevo.estatus).toBe('discrepancia');
    expect(r.mapeados[0].nuevo.motivoDiscrepancia).toBe('sin_candidato');
  });

  test('legacy descartado: rechazado', async () => {
    const doc = { _id: 'r3', estatus: 'descartado', claveRastreo: 'C3' };
    const r = await clasificarReportes([doc], new Map());
    expect(r.mapeados[0].nuevo.estatus).toBe('rechazado');
  });

  test('estado ya v2: va a yaMigrados', async () => {
    const doc = { _id: 'r4', estatus: 'resuelto_manual' };
    const r = await clasificarReportes([doc], new Map());
    expect(r.yaMigrados).toEqual([doc]);
  });

  test('estado desconocido: no mapeado', async () => {
    const doc = { _id: 'r5', estatus: 'algo-raro', claveRastreo: 'C5' };
    const r = await clasificarReportes([doc], new Map());
    expect(r.noMapeados).toHaveLength(1);
  });

  test('movementIdConfirmado presente pero el movimiento no aparece en el Map (nunca cargado): pasa null al clasificador, nunca revienta', async () => {
    const doc = { _id: 'r6', estatus: 'confirmado', claveRastreo: 'C6', movementIdConfirmado: 'm-fantasma' };
    const r = await clasificarReportes([doc], new Map());
    expect(r.mapeados[0].nuevo.estatus).toBe('discrepancia');
    expect(r.mapeados[0].nuevo.motivoDiscrepancia).toBe('vinculo_huerfano');
  });
});

describe('clasificarTodo', () => {
  test('carga NetpayMatch/NetpayReporte, junta los ids de ambos en UNA sola consulta BankMovement, clasifica ambos', async () => {
    const matchDoc = { _id: 'd1', estatusMatch: 'matcheada', movementIdsConfirmados: ['m1'] };
    const reporteDoc = {
      _id: 'r1', estatus: 'confirmado', claveRastreo: 'CLAVE-1', movementIdConfirmado: 'm2',
    };
    NetpayMatch.find = jest.fn(() => fakeFind([matchDoc]));
    NetpayReporte.find = jest.fn(() => fakeFind([reporteDoc]));
    BankMovement.find = jest.fn(() => fakeFind([
      movMatch({ _id: 'm1' }),
      { _id: 'm2', erpLinks: [{ erpId: 'NETPAYRPT-CLAVE-1' }] },
    ]));

    const { matches, reportes } = await clasificarTodo();

    expect(matches.mapeados[0].nuevo.estatusMatch).toBe('confirmado_automatico');
    expect(reportes.mapeados[0].nuevo.estatus).toBe('resuelto_por_reporte');
    const filtro = BankMovement.find.mock.calls[0][0];
    expect(filtro._id.$in.sort()).toEqual(['m1', 'm2']);
  });

  test('sin ids referenciados (todo movementIdsConfirmados vacío / movementIdConfirmado null): no consulta BankMovement', async () => {
    NetpayMatch.find = jest.fn(() => fakeFind([{ _id: 'd1', estatusMatch: 'descartada-manual', movementIdsConfirmados: [] }]));
    NetpayReporte.find = jest.fn(() => fakeFind([]));
    BankMovement.find = jest.fn();

    await clasificarTodo();

    expect(BankMovement.find).not.toHaveBeenCalled();
  });
});

describe('imprimirResumen', () => {
  let logSpy;
  beforeEach(() => { logSpy = jest.spyOn(console, 'log').mockImplementation(() => {}); });
  afterEach(() => { logSpy.mockRestore(); });

  test('0 no mapeados: reporta 100% mapeado', () => {
    const resultado = {
      matches: { yaMigrados: [], mapeados: [{ doc: { _id: 'd1' }, nuevo: {} }], noMapeados: [] },
      reportes: { yaMigrados: [], mapeados: [], noMapeados: [] },
    };
    const { totalNoMapeados } = imprimirResumen(resultado);
    expect(totalNoMapeados).toBe(0);
    expect(logSpy.mock.calls.some(c => c[0].includes('100% mapeado'))).toBe(true);
  });

  test('con no mapeados: los lista y NO dice 100% mapeado', () => {
    const resultado = {
      matches: { yaMigrados: [], mapeados: [], noMapeados: [{ doc: { _id: 'd1' }, error: 'no reconocido: X' }] },
      reportes: { yaMigrados: [], mapeados: [], noMapeados: [] },
    };
    const { totalNoMapeados } = imprimirResumen(resultado);
    expect(totalNoMapeados).toBe(1);
    expect(logSpy.mock.calls.some(c => c[0].includes('100% mapeado'))).toBe(false);
    expect(logSpy.mock.calls.some(c => c[0].includes('d1'))).toBe(true);
  });
});

describe('runDryRun', () => {
  test('reporta pero NUNCA escribe (sin bulkWrite/updateOne en ningún modelo)', async () => {
    NetpayMatch.find = jest.fn(() => fakeFind([{ _id: 'd1', estatusMatch: 'matcheada', movementIdsConfirmados: [] }]));
    NetpayReporte.find = jest.fn(() => fakeFind([]));
    BankMovement.find = jest.fn(() => fakeFind([]));
    NetpayMatch.bulkWrite = jest.fn();
    NetpayReporte.bulkWrite = jest.fn();
    jest.spyOn(console, 'log').mockImplementation(() => {});

    const { totalNoMapeados } = await runDryRun();

    expect(totalNoMapeados).toBe(0);
    expect(NetpayMatch.bulkWrite).not.toHaveBeenCalled();
    expect(NetpayReporte.bulkWrite).not.toHaveBeenCalled();
  });
});

describe('_dropearIndiceViejo', () => {
  test('índice viejo {terminalID,dia} existe: lo dropea por nombre', async () => {
    const dropIndex = jest.fn().mockResolvedValue(undefined);
    const indexes = jest.fn().mockResolvedValue([
      { name: 'terminalID_1_dia_1', key: { terminalID: 1, dia: 1 }, unique: true },
      { name: 'terminalID_1_dia_1_bucket_1', key: { terminalID: 1, dia: 1, bucket: 1 }, unique: true },
    ]);
    mongoose.connection.db = { collection: jest.fn(() => ({ indexes, dropIndex })) };

    const r = await _dropearIndiceViejo();

    expect(dropIndex).toHaveBeenCalledWith('terminalID_1_dia_1');
    expect(r.dropeado).toBe(true);
  });

  test('índice viejo ya no existe: no dropea nada, no revienta', async () => {
    const dropIndex = jest.fn();
    const indexes = jest.fn().mockResolvedValue([
      { name: 'terminalID_1_dia_1_bucket_1', key: { terminalID: 1, dia: 1, bucket: 1 }, unique: true },
    ]);
    mongoose.connection.db = { collection: jest.fn(() => ({ indexes, dropIndex })) };
    jest.spyOn(console, 'log').mockImplementation(() => {});

    const r = await _dropearIndiceViejo();

    expect(dropIndex).not.toHaveBeenCalled();
    expect(r.dropeado).toBe(false);
  });
});

describe('_bulkOpsMatches / _bulkOpsReportes', () => {
  test('_bulkOpsMatches: un updateOne por doc mapeado, filtrado por _id, $set con el resultado del clasificador', () => {
    const mapeados = [{ doc: { _id: 'd1' }, nuevo: { estatusMatch: 'rechazado', estatusLegacy: 'descartada-manual', bucket: 'general' } }];
    const ops = _bulkOpsMatches(mapeados);
    expect(ops).toEqual([{ updateOne: { filter: { _id: 'd1' }, update: { $set: mapeados[0].nuevo } } }]);
  });

  test('_bulkOpsReportes: excluye bucket (no existe en el schema de NetpayReporte)', () => {
    const mapeados = [{ doc: { _id: 'r1' }, nuevo: { estatus: 'rechazado', estatusLegacy: 'descartado', bucket: 'general' } }];
    const ops = _bulkOpsReportes(mapeados);
    expect(ops[0].updateOne.update.$set).toEqual({ estatus: 'rechazado', estatusLegacy: 'descartado' });
    expect(ops[0].updateOne.update.$set.bucket).toBeUndefined();
  });
});

describe('runApply', () => {
  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
  });

  test('feliz: 0 no mapeados -> dropea índice viejo y hace bulkWrite en ambos modelos', async () => {
    NetpayMatch.find = jest.fn(() => fakeFind([{ _id: 'd1', estatusMatch: 'matcheada', movementIdsConfirmados: [] }]));
    NetpayReporte.find = jest.fn(() => fakeFind([{ _id: 'r1', estatus: 'descartado', claveRastreo: 'C1' }]));
    BankMovement.find = jest.fn(() => fakeFind([]));
    const dropIndex = jest.fn().mockResolvedValue(undefined);
    const indexes = jest.fn().mockResolvedValue([{ name: 'terminalID_1_dia_1', key: { terminalID: 1, dia: 1 }, unique: true }]);
    mongoose.connection.db = { collection: jest.fn(() => ({ indexes, dropIndex })) };
    NetpayMatch.bulkWrite = jest.fn().mockResolvedValue({ modifiedCount: 1 });
    NetpayReporte.bulkWrite = jest.fn().mockResolvedValue({ modifiedCount: 1 });

    await runApply();

    expect(dropIndex).toHaveBeenCalledWith('terminalID_1_dia_1');
    expect(NetpayMatch.bulkWrite).toHaveBeenCalledTimes(1);
    expect(NetpayReporte.bulkWrite).toHaveBeenCalledTimes(1);
  });

  test('con no mapeados: arroja y NUNCA dropea índice ni escribe nada (cero escrituras parciales)', async () => {
    NetpayMatch.find = jest.fn(() => fakeFind([{ _id: 'd1', estatusMatch: 'algo-raro', movementIdsConfirmados: [] }]));
    NetpayReporte.find = jest.fn(() => fakeFind([]));
    BankMovement.find = jest.fn(() => fakeFind([]));
    const dropIndex = jest.fn();
    mongoose.connection.db = { collection: jest.fn(() => ({ indexes: jest.fn(), dropIndex })) };
    NetpayMatch.bulkWrite = jest.fn();
    NetpayReporte.bulkWrite = jest.fn();

    await expect(runApply()).rejects.toThrow(/no mapeado/);

    expect(dropIndex).not.toHaveBeenCalled();
    expect(NetpayMatch.bulkWrite).not.toHaveBeenCalled();
    expect(NetpayReporte.bulkWrite).not.toHaveBeenCalled();
  });
});

describe('_tieneLinkMatch / _tieneLinkReporte', () => {
  test('_tieneLinkMatch: true si movementIdsConfirmados tiene al menos 1', () => {
    expect(_tieneLinkMatch({ movementIdsConfirmados: ['m1'] })).toBe(true);
    expect(_tieneLinkMatch({ movementIdsConfirmados: [] })).toBe(false);
    expect(_tieneLinkMatch({})).toBe(false);
  });

  test('_tieneLinkReporte: true si movementIdConfirmado no es null/undefined', () => {
    expect(_tieneLinkReporte({ movementIdConfirmado: 'm1' })).toBe(true);
    expect(_tieneLinkReporte({ movementIdConfirmado: null })).toBe(false);
    expect(_tieneLinkReporte({})).toBe(false);
  });
});

describe('_verificarGuardaRevert', () => {
  test('ningún doc con estatusLegacy:null + link: no arroja', async () => {
    await expect(_verificarGuardaRevert(
      [{ _id: 'd1', estatusLegacy: 'matcheada', movementIdsConfirmados: ['m1'] }],
      [{ _id: 'r1', estatusLegacy: null, movementIdConfirmado: null }],
    )).resolves.toBeUndefined();
  });

  test('un NetpayMatch con estatusLegacy:null y link real: arroja', async () => {
    await expect(_verificarGuardaRevert(
      [{ _id: 'd1', estatusLegacy: null, movementIdsConfirmados: ['m1'] }],
      [],
    )).rejects.toThrow(/abortado/);
  });

  test('un NetpayReporte con estatusLegacy:null y movementIdConfirmado: arroja', async () => {
    await expect(_verificarGuardaRevert(
      [],
      [{ _id: 'r1', estatusLegacy: null, movementIdConfirmado: 'm9' }],
    )).rejects.toThrow(/abortado/);
  });
});

describe('_bulkOpsRevertMatches / _bulkOpsRevertReportes', () => {
  test('_bulkOpsRevertMatches: solo docs con estatusLegacy no-null, restaura estatusMatch y limpia estatusLegacy', () => {
    const docs = [
      { _id: 'd1', estatusLegacy: 'matcheada' },
      { _id: 'd2', estatusLegacy: null },
    ];
    const ops = _bulkOpsRevertMatches(docs);
    expect(ops).toHaveLength(1);
    expect(ops[0]).toEqual({
      updateOne: { filter: { _id: 'd1' }, update: { $set: { estatusMatch: 'matcheada' }, $unset: { estatusLegacy: '' } } },
    });
  });

  test('_bulkOpsRevertReportes: mismo criterio con estatus', () => {
    const docs = [{ _id: 'r1', estatusLegacy: 'confirmado' }];
    const ops = _bulkOpsRevertReportes(docs);
    expect(ops[0].updateOne.update.$set).toEqual({ estatus: 'confirmado' });
  });
});

describe('runRevert', () => {
  test('feliz: restaura solo los migrados, deja intactos los estatusLegacy:null sin link', async () => {
    NetpayMatch.find = jest.fn(() => fakeFind([
      { _id: 'd1', estatusLegacy: 'matcheada', movementIdsConfirmados: [] },
      { _id: 'd2', estatusLegacy: null, movementIdsConfirmados: [] },
    ]));
    NetpayReporte.find = jest.fn(() => fakeFind([]));
    NetpayMatch.bulkWrite = jest.fn().mockResolvedValue({ modifiedCount: 1 });
    NetpayReporte.bulkWrite = jest.fn().mockResolvedValue({ modifiedCount: 0 });
    jest.spyOn(console, 'log').mockImplementation(() => {});

    await runRevert();

    expect(NetpayMatch.bulkWrite).toHaveBeenCalledWith([
      { updateOne: { filter: { _id: 'd1' }, update: { $set: { estatusMatch: 'matcheada' }, $unset: { estatusLegacy: '' } } } },
    ]);
  });

  test('bloqueado: un doc con estatusLegacy:null y link real aborta TODO (cero bulkWrite)', async () => {
    NetpayMatch.find = jest.fn(() => fakeFind([
      { _id: 'd1', estatusLegacy: null, movementIdsConfirmados: ['m1'] },
    ]));
    NetpayReporte.find = jest.fn(() => fakeFind([]));
    NetpayMatch.bulkWrite = jest.fn();
    NetpayReporte.bulkWrite = jest.fn();

    await expect(runRevert()).rejects.toThrow(/abortado/);

    expect(NetpayMatch.bulkWrite).not.toHaveBeenCalled();
    expect(NetpayReporte.bulkWrite).not.toHaveBeenCalled();
  });
});

describe('main', () => {
  beforeEach(() => {
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(mongoose, 'connect').mockResolvedValue(undefined);
    jest.spyOn(mongoose, 'disconnect').mockResolvedValue(undefined);
  });

  test('flag inválido: arroja ANTES de conectar a Mongo (fail-fast, cero conexión desperdiciada)', async () => {
    await expect(main(['--foo'])).rejects.toThrow(/no reconocido/);
    expect(mongoose.connect).not.toHaveBeenCalled();
  });

  test('modo dry-run (default): conecta, corre, desconecta siempre (finally)', async () => {
    NetpayMatch.find = jest.fn(() => fakeFind([]));
    NetpayReporte.find = jest.fn(() => fakeFind([]));
    BankMovement.find = jest.fn(() => fakeFind([]));

    await main([]);

    expect(mongoose.connect).toHaveBeenCalledTimes(1);
    expect(mongoose.disconnect).toHaveBeenCalledTimes(1);
  });

  test('desconecta también si el modo elegido revienta (finally se ejecuta en error)', async () => {
    NetpayMatch.find = jest.fn(() => fakeFind([{ _id: 'd1', estatusMatch: 'algo-raro', movementIdsConfirmados: [] }]));
    NetpayReporte.find = jest.fn(() => fakeFind([]));
    BankMovement.find = jest.fn(() => fakeFind([]));
    mongoose.connection.db = { collection: jest.fn(() => ({ indexes: jest.fn(), dropIndex: jest.fn() })) };

    await expect(main(['--apply'])).rejects.toThrow(/no mapeado/);

    expect(mongoose.disconnect).toHaveBeenCalledTimes(1);
  });
});
