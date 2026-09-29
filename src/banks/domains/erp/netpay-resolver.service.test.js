'use strict';

// netpay-resolver.service.test.js — netpay-matching-v2 (design.md "Approach": "Candidate
// picker removed for Netpay"): reemplaza confirmarMatchNetpay/descartarMatchNetpay
// (removidos, ver netpay-match-confirm.service.js) para un bucket NetpayMatch. `resolver`
// cierra un bucket 'discrepancia' con justificación humana OBLIGATORIA, opcionalmente
// vinculando 1-2 BankMovement (preserva el split manual de v1 — design.md "Resolve
// movement cardinality"). `rechazar` descarta desde cualquier estado activo.
jest.mock('../banks/BankMovement.model');
jest.mock('./NetpayMatch.model');
jest.mock('../../shared/socket', () => ({ emitToBanco: jest.fn(), emitToAll: jest.fn() }));
jest.mock('../banks/bank.service', () => ({ setErpIds: jest.fn() }));

const BankMovement = require('../banks/BankMovement.model');
const NetpayMatch = require('./NetpayMatch.model');
const { setErpIds } = require('../banks/bank.service');
const { emitToBanco, emitToAll } = require('../../shared/socket');
const { NotFoundError, ConflictError } = require('../../shared/errors/AppError');
const { resolver, rechazar } = require('./netpay-resolver.service');

const USER = { _id: 'user-1', nombre: 'Ana' };

function fakeBucketDoc(overrides = {}) {
  return {
    _id: 'nm-1', terminalID: '2840403056', dia: new Date('2026-09-10T00:00:00.000Z'), bucket: 'general',
    estatusMatch: 'discrepancia', motivoDiscrepancia: 'sin_candidato',
    justificacion: null, resueltoManualPor: null, resueltoManualEn: null,
    rechazoMotivo: null, descartadoManualmentePor: null, descartadoManualmenteEn: null,
    movementIdsConfirmados: [],
    save: jest.fn().mockResolvedValue(undefined),
    toObject: jest.fn(function () { return { ...this }; }),
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  BankMovement.find = jest.fn().mockResolvedValue([]);
});

describe('resolver', () => {
  test('sin justificación (vacía/blanco): BadRequestError, ni siquiera busca el bucket', async () => {
    await expect(resolver('nm-1', { justificacion: '' }, USER)).rejects.toThrow(/justificación/);
    await expect(resolver('nm-1', { justificacion: '   ' }, USER)).rejects.toThrow(/justificación/);
    expect(NetpayMatch.findById).not.toHaveBeenCalled();
  });

  test('más de 2 movementIds: BadRequestError', async () => {
    await expect(resolver('nm-1', { justificacion: 'ok', movementIds: ['a', 'b', 'c'] }, USER))
      .rejects.toThrow(/a lo sumo 2/);
  });

  test('bucket no existe: NotFoundError', async () => {
    NetpayMatch.findById = jest.fn().mockResolvedValue(null);
    await expect(resolver('nm-1', { justificacion: 'ok' }, USER)).rejects.toThrow(NotFoundError);
  });

  test('bucket NO está en discrepancia: ConflictError (409)', async () => {
    NetpayMatch.findById = jest.fn().mockResolvedValue(fakeBucketDoc({ estatusMatch: 'pendiente_por_marca' }));
    await expect(resolver('nm-1', { justificacion: 'ok' }, USER)).rejects.toThrow(/no está en discrepancia/);
  });

  test('sin movementIds (solo justificación): resuelve sin tocar BankMovement/setErpIds', async () => {
    const bucket = fakeBucketDoc();
    NetpayMatch.findById = jest.fn().mockResolvedValue(bucket);

    const res = await resolver('nm-1', { justificacion: 'Confirmado a mano contra el estado de cuenta' }, USER);

    expect(setErpIds).not.toHaveBeenCalled();
    expect(bucket.estatusMatch).toBe('resuelto_manual');
    expect(bucket.motivoDiscrepancia).toBeNull();
    expect(bucket.justificacion).toBe('Confirmado a mano contra el estado de cuenta');
    expect(bucket.resueltoManualPor).toEqual({ userId: 'user-1', nombre: 'Ana' });
    expect(bucket.resueltoManualEn).toBeInstanceOf(Date);
    expect(res.movimientos).toEqual([]);
  });

  test('con 1 movementId válido: vincula vía setErpIds (erpId NETPAY-...-MANUAL), audita, emite sockets', async () => {
    const bucket = fakeBucketDoc();
    NetpayMatch.findById = jest.fn().mockResolvedValue(bucket);
    const mov = { _id: 'mov-1', banco: 'BBVA', erpLinks: [], deposito: 280 };
    BankMovement.find = jest.fn().mockResolvedValue([mov]);
    const movActualizado = { _id: 'mov-1', banco: 'BBVA' };
    setErpIds.mockResolvedValue(movActualizado);

    const res = await resolver('nm-1', { justificacion: 'ok', movementIds: ['mov-1'] }, USER);

    expect(setErpIds).toHaveBeenCalledWith(
      'mov-1',
      [expect.objectContaining({ erpId: `NETPAY-2840403056-2026-09-10-MANUAL`, origen: 'netpay-matching-manual' })],
      USER,
    );
    expect(bucket.movementIdsConfirmados).toEqual(['mov-1']);
    expect(emitToBanco).toHaveBeenCalledWith('BBVA', 'bank:movement:updated', movActualizado);
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
    expect(res.movimientos).toEqual([movActualizado]);
  });

  test('con 2 movementIds (split): setErpIds una vez por movimiento', async () => {
    const bucket = fakeBucketDoc();
    NetpayMatch.findById = jest.fn().mockResolvedValue(bucket);
    BankMovement.find = jest.fn().mockResolvedValue([
      { _id: 'mov-1', banco: 'BBVA', erpLinks: [], deposito: 200 },
      { _id: 'mov-2', banco: 'BBVA', erpLinks: [], deposito: 80 },
    ]);
    setErpIds.mockResolvedValueOnce({ _id: 'mov-1', banco: 'BBVA' }).mockResolvedValueOnce({ _id: 'mov-2', banco: 'BBVA' });

    await resolver('nm-1', { justificacion: 'ok', movementIds: ['mov-1', 'mov-2'] }, USER);

    expect(setErpIds).toHaveBeenCalledTimes(2);
    expect(bucket.movementIdsConfirmados).toEqual(['mov-1', 'mov-2']);
  });

  test('un movementId no existe: NotFoundError, nunca llega a llamar setErpIds', async () => {
    NetpayMatch.findById = jest.fn().mockResolvedValue(fakeBucketDoc());
    BankMovement.find = jest.fn().mockResolvedValue([]);

    await expect(resolver('nm-1', { justificacion: 'ok', movementIds: ['mov-1'] }, USER)).rejects.toThrow(NotFoundError);
    expect(setErpIds).not.toHaveBeenCalled();
  });

  test('movimiento que no es de BBVA: ConflictError', async () => {
    NetpayMatch.findById = jest.fn().mockResolvedValue(fakeBucketDoc());
    BankMovement.find = jest.fn().mockResolvedValue([{ _id: 'mov-1', banco: 'Banamex', erpLinks: [], deposito: 280 }]);
    await expect(resolver('nm-1', { justificacion: 'ok', movementIds: ['mov-1'] }, USER)).rejects.toThrow(/no es de BBVA/);
  });

  test('movimiento ya tiene erpLinks: ConflictError', async () => {
    NetpayMatch.findById = jest.fn().mockResolvedValue(fakeBucketDoc());
    BankMovement.find = jest.fn().mockResolvedValue([{ _id: 'mov-1', banco: 'BBVA', erpLinks: [{ erpId: 'X' }], deposito: 280 }]);
    await expect(resolver('nm-1', { justificacion: 'ok', movementIds: ['mov-1'] }, USER)).rejects.toThrow(/ya tiene un ID ERP vinculado/);
  });

  // spec.md "Manual action cannot force auto-confirmed" — guard de diseño no negociable.
  test('el resultado NUNCA es confirmado_automatico, sin importar cuántos movimientos se vinculen', async () => {
    const bucket = fakeBucketDoc();
    NetpayMatch.findById = jest.fn().mockResolvedValue(bucket);
    BankMovement.find = jest.fn().mockResolvedValue([{ _id: 'mov-1', banco: 'BBVA', erpLinks: [], deposito: 280 }]);
    setErpIds.mockResolvedValue({ _id: 'mov-1', banco: 'BBVA' });

    await resolver('nm-1', { justificacion: 'ok', movementIds: ['mov-1'] }, USER);

    expect(bucket.estatusMatch).not.toBe('confirmado_automatico');
    expect(bucket.estatusMatch).toBe('resuelto_manual');
  });
});

describe('rechazar', () => {
  test('bucket no existe: NotFoundError', async () => {
    NetpayMatch.findById = jest.fn().mockResolvedValue(null);
    await expect(rechazar('nm-1', { motivo: 'x' }, USER)).rejects.toThrow(NotFoundError);
  });

  test('bucket ya rechazado: ConflictError (estado terminal)', async () => {
    NetpayMatch.findById = jest.fn().mockResolvedValue(fakeBucketDoc({ estatusMatch: 'rechazado' }));
    await expect(rechazar('nm-1', { motivo: 'x' }, USER)).rejects.toThrow(ConflictError);
  });

  test('bucket ya resuelto_manual: ConflictError (estado terminal)', async () => {
    NetpayMatch.findById = jest.fn().mockResolvedValue(fakeBucketDoc({ estatusMatch: 'resuelto_manual' }));
    await expect(rechazar('nm-1', { motivo: 'x' }, USER)).rejects.toThrow(ConflictError);
  });

  test('desde discrepancia: pasa a rechazado, audita motivo/quién/cuándo', async () => {
    const bucket = fakeBucketDoc();
    NetpayMatch.findById = jest.fn().mockResolvedValue(bucket);

    await rechazar('nm-1', { motivo: '  ya identificado a mano  ' }, USER);

    expect(bucket.estatusMatch).toBe('rechazado');
    expect(bucket.rechazoMotivo).toBe('ya identificado a mano');
    expect(bucket.descartadoManualmentePor).toEqual({ userId: 'user-1', nombre: 'Ana' });
    expect(bucket.descartadoManualmenteEn).toBeInstanceOf(Date);
    expect(bucket.save).toHaveBeenCalled();
  });

  // spec.md "any active state -> rechazado" — pendiente_por_marca también es rechazable.
  test('desde pendiente_por_marca: también permitido', async () => {
    const bucket = fakeBucketDoc({ estatusMatch: 'pendiente_por_marca', motivoDiscrepancia: null });
    NetpayMatch.findById = jest.fn().mockResolvedValue(bucket);

    await rechazar('nm-1', { motivo: 'ya no aplica' }, USER);

    expect(bucket.estatusMatch).toBe('rechazado');
  });

  test('sin motivo: rechazoMotivo queda null (no se exige)', async () => {
    const bucket = fakeBucketDoc();
    NetpayMatch.findById = jest.fn().mockResolvedValue(bucket);

    await rechazar('nm-1', {}, USER);

    expect(bucket.rechazoMotivo).toBeNull();
  });
});
