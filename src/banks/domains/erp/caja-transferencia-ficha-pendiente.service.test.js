'use strict';

// caja-transferencia-ficha-pendiente.service.test.js — pedido explícito del usuario
// 2026-09-03: listarPendientesDeFicha() trae el total cross-banco de BankMovement con
// erpLinks.origen:'transferencia-caja' y ficha aún sin cargar. Solo se mockea
// BankMovement.model — la función es una query simple, sin lógica de negocio propia que
// aislar.
jest.mock('../banks/BankMovement.model');

const BankMovement = require('../banks/BankMovement.model');
const { listarPendientesDeFicha } = require('./caja-transferencia-ficha-pendiente.service');

// Replica la cadena real find().sort().limit().select().lean() — cada método debe
// devolver el mismo objeto encadenable, terminando en lean() que resuelve el resultado.
function fakeQuery(result) {
  const q = {
    sort:   jest.fn(() => q),
    limit:  jest.fn(() => q),
    select: jest.fn(() => q),
    lean:   jest.fn().mockResolvedValue(result),
  };
  return q;
}

beforeEach(() => {
  jest.clearAllMocks();
});

test('consulta por erpLinks.origen:transferencia-caja + ficha:null, ordenado por fecha desc, limitado a 200', async () => {
  const query = fakeQuery([]);
  BankMovement.find = jest.fn(() => query);

  await listarPendientesDeFicha();

  expect(BankMovement.find).toHaveBeenCalledWith({ 'erpLinks.origen': 'transferencia-caja', ficha: null });
  expect(query.sort).toHaveBeenCalledWith({ fecha: -1 });
  expect(query.limit).toHaveBeenCalledWith(200);
  expect(query.select).toHaveBeenCalledWith('_id banco fecha concepto deposito folio erpLinks');
});

test('sin movimientos pendientes: total 0, movimientos []', async () => {
  BankMovement.find = jest.fn(() => fakeQuery([]));

  const resultado = await listarPendientesDeFicha();

  expect(resultado).toEqual({ total: 0, movimientos: [] });
});

test('con movimientos pendientes: total = cantidad devuelta, movimientos tal cual vienen de Mongo', async () => {
  const movimientos = [
    { _id: 'mov-1', banco: 'BBVA', fecha: new Date('2026-09-01'), concepto: 'DEP', deposito: 1500, folio: null, erpLinks: [{ erpId: 'CAJA-abc', origen: 'transferencia-caja' }] },
    { _id: 'mov-2', banco: 'Banamex', fecha: new Date('2026-08-30'), concepto: 'DEP', deposito: 300, folio: null, erpLinks: [{ erpId: 'CAJA-def', origen: 'transferencia-caja' }] },
  ];
  BankMovement.find = jest.fn(() => fakeQuery(movimientos));

  const resultado = await listarPendientesDeFicha();

  expect(resultado).toEqual({ total: 2, movimientos });
});

// Nota del diseño (ver comentario en el servicio): con exactamente LIMIT (200) resultados,
// `total` refleja solo lo que trajo esta página, no el total real en Mongo — documentado a
// propósito, no es un bug de este test.
test('con exactamente LIMIT resultados: total queda truncado a LIMIT (comportamiento documentado, no paginación real)', async () => {
  const movimientos = Array.from({ length: 200 }, (_, i) => ({ _id: `mov-${i}` }));
  BankMovement.find = jest.fn(() => fakeQuery(movimientos));

  const resultado = await listarPendientesDeFicha();

  expect(resultado.total).toBe(200);
  expect(resultado.movimientos).toHaveLength(200);
});
