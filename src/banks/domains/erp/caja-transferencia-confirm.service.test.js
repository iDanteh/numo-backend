'use strict';

// caja-transferencia-confirm.service.test.js — Fase D del proceso de matching de
// transferencias entre cajas: confirmarMatch() re-valida elegibilidad server-side,
// reusa setErpIds() (bank.service.js) con un erpId sintético CAJA-<koreId> y
// origen:'transferencia-caja', y deja rastro (confirmadoPor/confirmadoEn) en la
// transferencia. bank.service.js NO se mockea completo — se mockea solo setErpIds,
// pero ERP_TOLERANCE se toma real (constante, sin I/O).
jest.mock('../banks/BankMovement.model');
jest.mock('./CajaTransferencia.model');
jest.mock('../../shared/socket', () => ({ emitToBanco: jest.fn() }));
jest.mock('../banks/bank.service', () => {
  const real = jest.requireActual('../banks/bank.service');
  return { setErpIds: jest.fn(), ERP_TOLERANCE: real.ERP_TOLERANCE };
});

const mockSession = {
  startTransaction:  jest.fn(),
  commitTransaction: jest.fn().mockResolvedValue(undefined),
  abortTransaction:  jest.fn().mockResolvedValue(undefined),
  endSession:        jest.fn().mockResolvedValue(undefined),
  inTransaction:     jest.fn(() => true),
};
// Solo se sobreescribe connection.startSession — BankMovement.model.js (aunque esté
// automockeado) ejecuta su Schema real al cargar, y necesita el resto de mongoose intacto.
jest.mock('mongoose', () => {
  const real = jest.requireActual('mongoose');
  return { ...real, connection: { ...real.connection, startSession: jest.fn() } };
});

const mongoose         = require('mongoose');
const BankMovement      = require('../banks/BankMovement.model');
const CajaTransferencia = require('./CajaTransferencia.model');
const { emitToBanco }   = require('../../shared/socket');
const { setErpIds }     = require('../banks/bank.service');
const { confirmarMatch } = require('./caja-transferencia-confirm.service');

const USER = { _id: 'user-1', nombre: 'Ana', role: 'contabilidad' };

function fakeTransferencia(overrides = {}) {
  return {
    _id: 't-1', koreId: '6a97291ab6007400011db828', monto: 1500, estatusMatch: 'pendiente',
    toObject: function () { return { ...this }; },
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  mongoose.connection.startSession.mockResolvedValue(mockSession);
  mockSession.inTransaction.mockReturnValue(true);
});

test('transferencia inexistente: NotFoundError', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(null);
  await expect(confirmarMatch('t-1', ['mov-1'], USER)).rejects.toThrow('Transferencia de caja');
});

test('transferencia ya no está pendiente: ConflictError', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia({ estatusMatch: 'huerfana' }));
  await expect(confirmarMatch('t-1', ['mov-1'], USER)).rejects.toThrow(/ya no está pendiente/);
});

test('movementIds vacío o con más de 2: BadRequestError', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia());
  await expect(confirmarMatch('t-1', [], USER)).rejects.toThrow('Se requiere 1 o 2 movementIds');
  await expect(confirmarMatch('t-1', ['a', 'b', 'c'], USER)).rejects.toThrow('Se requiere 1 o 2 movementIds');
});

test('algún movementId no existe: NotFoundError', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia());
  BankMovement.find = jest.fn().mockResolvedValue([{ _id: 'mov-1', categoria: 'Depósito en efectivo', erpLinks: [], deposito: 1500 }]);
  await expect(confirmarMatch('t-1', ['mov-1', 'mov-2'], USER)).rejects.toThrow('Uno o más movimientos bancarios');
});

// Bug real 2026-09-01 (reportado por el usuario): buscarCandidatos() ya normalizaba
// mayúsculas/acentos, pero confirmarMatch() seguía comparando `===` exacto — un candidato
// SUGERIDO por la bandeja (categoria real del ambiente: "DEPOSITO EN EFECTIVO") tronaba con
// ConflictError al confirmarlo. Ambos deben compartir el mismo criterio (esCategoriaDepositoEfectivo).
test('acepta la categoría sin importar mayúsculas/acentos (caso real: "DEPOSITO EN EFECTIVO")', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia());
  CajaTransferencia.updateOne = jest.fn().mockResolvedValue({});
  BankMovement.find = jest.fn().mockResolvedValue([
    { _id: 'mov-1', categoria: 'DEPOSITO EN EFECTIVO', erpLinks: [], deposito: 1500 },
  ]);
  setErpIds.mockResolvedValue({ _id: 'mov-1', banco: 'BBVA' });

  await expect(confirmarMatch('t-1', ['mov-1'], USER)).resolves.toBeDefined();
  expect(setErpIds).toHaveBeenCalledTimes(1);
});

test('movimiento con categoria distinta a Depósito en efectivo: ConflictError', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia());
  BankMovement.find = jest.fn().mockResolvedValue([{ _id: 'mov-1', categoria: 'Otro', erpLinks: [], deposito: 1500 }]);
  await expect(confirmarMatch('t-1', ['mov-1'], USER)).rejects.toThrow(/no es Depósito en efectivo/);
});

test('movimiento que YA tiene erpLinks (otro usuario lo tomó primero): ConflictError', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia());
  BankMovement.find = jest.fn().mockResolvedValue([
    { _id: 'mov-1', categoria: 'Depósito en efectivo', erpLinks: [{ erpId: 'CXC-1' }], deposito: 1500 },
  ]);
  await expect(confirmarMatch('t-1', ['mov-1'], USER)).rejects.toThrow(/ya tiene un ID ERP vinculado/);
});

test('suma de montos no coincide con el monto de la transferencia: ConflictError', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia({ monto: 1500 }));
  BankMovement.find = jest.fn().mockResolvedValue([
    { _id: 'mov-1', categoria: 'Depósito en efectivo', erpLinks: [], deposito: 999 },
  ]);
  await expect(confirmarMatch('t-1', ['mov-1'], USER)).rejects.toThrow(/no coincide con el monto/);
});

test('match 1:1 válido: llama setErpIds con erpId sintético y origen transferencia-caja, marca matcheada, emite socket', async () => {
  const transferencia = fakeTransferencia();
  CajaTransferencia.findById = jest.fn().mockResolvedValue(transferencia);
  CajaTransferencia.updateOne = jest.fn().mockResolvedValue({});
  BankMovement.find = jest.fn().mockResolvedValue([
    { _id: 'mov-1', categoria: 'Depósito en efectivo', erpLinks: [], deposito: 1500 },
  ]);
  const updatedMov = { _id: 'mov-1', banco: 'BBVA', erpLinks: [{ erpId: 'CAJA-6a97291ab6007400011db828' }] };
  setErpIds.mockResolvedValue(updatedMov);

  const res = await confirmarMatch('t-1', ['mov-1'], USER);

  expect(setErpIds).toHaveBeenCalledTimes(1);
  const [movId, links, user, opts] = setErpIds.mock.calls[0];
  expect(movId).toBe('mov-1');
  expect(links).toEqual([{
    erpId: 'CAJA-6a97291ab6007400011db828', origen: 'transferencia-caja',
    saldoPagadoTotal: 1500, saldoPagado: 1500, total: 1500,
  }]);
  expect(user).toBe(USER);
  expect(opts.session).toBe(mockSession);

  expect(CajaTransferencia.updateOne).toHaveBeenCalledWith(
    { _id: 't-1' },
    { $set: expect.objectContaining({
      estatusMatch: 'matcheada',
      confirmadoPor: { userId: 'user-1', nombre: 'Ana' },
      movementIdsConfirmados: ['mov-1'],
    }) },
    { session: mockSession },
  );
  expect(mockSession.commitTransaction).toHaveBeenCalled();
  expect(emitToBanco).toHaveBeenCalledWith('BBVA', 'bank:movement:updated', updatedMov);
  expect(res.movimientos).toEqual([updatedMov]);
});

test('match 1:2 válido (split real): llama setErpIds una vez por movimiento, cada uno con SU propio deposito', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia({ monto: 1500 }));
  CajaTransferencia.updateOne = jest.fn().mockResolvedValue({});
  BankMovement.find = jest.fn().mockResolvedValue([
    { _id: 'mov-a', categoria: 'Depósito en efectivo', erpLinks: [], deposito: 1000 },
    { _id: 'mov-b', categoria: 'Depósito en efectivo', erpLinks: [], deposito: 500 },
  ]);
  setErpIds
    .mockResolvedValueOnce({ _id: 'mov-a', banco: 'BBVA' })
    .mockResolvedValueOnce({ _id: 'mov-b', banco: 'Banamex' });

  await confirmarMatch('t-1', ['mov-a', 'mov-b'], USER);

  expect(setErpIds).toHaveBeenCalledTimes(2);
  expect(setErpIds.mock.calls[0][1][0]).toMatchObject({ saldoPagadoTotal: 1000, total: 1000 });
  expect(setErpIds.mock.calls[1][1][0]).toMatchObject({ saldoPagadoTotal: 500, total: 500 });
});

test('Mongo sin soporte de transacciones (standalone, code 20): cae al camino sin sesión', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia());
  CajaTransferencia.updateOne = jest.fn().mockResolvedValue({});
  BankMovement.find = jest.fn().mockResolvedValue([
    { _id: 'mov-1', categoria: 'Depósito en efectivo', erpLinks: [], deposito: 1500 },
  ]);
  const err = new Error('Transaction numbers are only allowed on a replica set member or mongos');
  err.code = 20;
  setErpIds.mockRejectedValueOnce(err).mockResolvedValueOnce({ _id: 'mov-1', banco: 'BBVA' });

  const res = await confirmarMatch('t-1', ['mov-1'], USER);

  expect(setErpIds).toHaveBeenCalledTimes(2);
  expect(setErpIds.mock.calls[1][3].session).toBeNull();
  expect(mockSession.abortTransaction).toHaveBeenCalled();
  expect(res.movimientos).toEqual([{ _id: 'mov-1', banco: 'BBVA' }]);
});
