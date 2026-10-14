'use strict';

// Cobertura mínima del fix de seguridad 2026-10-14 (auditoría real): listMovimientosAuxiliar()
// no aplicaba NINGÚN filtro de ocultamiento sobre BankMovement, y el `limit` que mandaba el
// cliente no tenía tope (limit=100000 se llevaba todo de una vez). No se prueba nada más de
// este archivo (parseAuxiliaryFile/applyAuxiliaryMatching/resumenAuxiliarClientes quedan sin
// tests previos, fuera de alcance de este fix puntual).

const mockLean            = jest.fn().mockResolvedValue([]);
const mockCountDocuments  = jest.fn().mockResolvedValue(0);
let lastFindMatch         = null;
let lastLimitArg          = null;

jest.mock('./BankMovement.model', () => ({
  find: jest.fn((match) => {
    lastFindMatch = match;
    return {
      sort:  jest.fn().mockReturnThis(),
      skip:  jest.fn().mockReturnThis(),
      limit: jest.fn((n) => { lastLimitArg = n; return { lean: mockLean }; }),
    };
  }),
  countDocuments: (...args) => mockCountDocuments(...args),
}));

const { listMovimientosAuxiliar } = require('./bank-auxiliary.parser');

describe('listMovimientosAuxiliar', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockLean.mockResolvedValue([]);
    mockCountDocuments.mockResolvedValue(0);
    lastFindMatch = null;
    lastLimitArg  = null;
  });

  test('match siempre excluye oculto:true, sin importar rolActual', async () => {
    await listMovimientosAuxiliar({});
    expect(lastFindMatch.oculto).toEqual({ $ne: true });
  });

  test('sin rolActual (bypass de ocultar-roles): no se agrega filtro ocultoRoles', async () => {
    await listMovimientosAuxiliar({ rolActual: null });
    expect(lastFindMatch.ocultoRoles).toBeUndefined();
  });

  test('con rolActual: excluye los movimientos ocultos para ese rol', async () => {
    await listMovimientosAuxiliar({ rolActual: 'contabilidad' });
    expect(lastFindMatch.ocultoRoles).toEqual({ $ne: 'contabilidad' });
  });

  test('no aplica ningún filtro por usuario (vista de conciliación, cruza todo el equipo a propósito)', async () => {
    await listMovimientosAuxiliar({ rolActual: 'cobranza' });
    expect(lastFindMatch.identificadoPorUsuario).toBeUndefined();
    expect(lastFindMatch['identificadoPor.userId']).toBeUndefined();
  });

  test('limit sin tope (ej. 100000) se cappea a 200', async () => {
    const res = await listMovimientosAuxiliar({ limit: 100000 });
    expect(lastLimitArg).toBe(200);
    expect(res.pagination.limit).toBe(200);
  });

  test('limit dentro del tope se respeta tal cual', async () => {
    await listMovimientosAuxiliar({ limit: 25 });
    expect(lastLimitArg).toBe(25);
  });

  test('default (sin limit): sigue siendo 50', async () => {
    await listMovimientosAuxiliar({});
    expect(lastLimitArg).toBe(50);
  });
});
