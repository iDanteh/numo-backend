'use strict';

// bank.service.primeraIdentificacion.test.js — smoke tests para los call-sites de
// bank.service.js que no tenían suite propia (updateStatus, updateCategoria) y que
// ahora aplican resolvePrimeraIdentificacion() al transicionar un movimiento a
// status='identificado'. Mismo patrón de mocking que bank.service.setErpIds.test.js:
// solo se mockean las dependencias externas que cada función toca.
jest.mock('./BankMovement.model');
jest.mock('./repositories/bank-rule.repository');
jest.mock('../../shared/socket');

const BankMovement  = require('./BankMovement.model');
const bankRuleRepo   = require('./repositories/bank-rule.repository');
const { emitToBanco } = require('../../shared/socket');
const bankService    = require('./bank.service');

function fakeMov(overrides = {}) {
  return {
    _id: 'mov-1', banco: 'BBVA', erpIds: ['CXC-1'], identificadoPor: [], status: 'no_identificado',
    save: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  bankRuleRepo.findBlockingRules = jest.fn().mockResolvedValue([]);
  bankRuleRepo.listByBanco       = jest.fn().mockResolvedValue([]);
});

describe('updateStatus — primeraIdentificacionAt/primeraIdentificacionPor', () => {
  test('transición a "identificado": se setea la primera vez', async () => {
    const mov = fakeMov({ primeraIdentificacionAt: null, primeraIdentificacionPor: null });
    BankMovement.findById.mockResolvedValue(mov);

    await bankService.updateStatus('mov-1', 'identificado', { _id: 'user-1', nombre: 'Usuario Uno', role: 'admin' });

    expect(mov.status).toBe('identificado');
    expect(mov.primeraIdentificacionAt).toBeInstanceOf(Date);
    expect(mov.primeraIdentificacionPor).toEqual({ userId: 'user-1', nombre: 'Usuario Uno' });
  });

  test('ya tenía primeraIdentificacionAt: no se sobreescribe aunque se re-identifique', async () => {
    const fechaOriginal = new Date('2026-01-01T00:00:00.000Z');
    const mov = fakeMov({ primeraIdentificacionAt: fechaOriginal, primeraIdentificacionPor: { userId: 'user-999', nombre: 'Otro' } });
    BankMovement.findById.mockResolvedValue(mov);

    await bankService.updateStatus('mov-1', 'identificado', { _id: 'user-1', nombre: 'Usuario Uno', role: 'admin' });

    expect(mov.primeraIdentificacionAt).toBe(fechaOriginal);
    expect(mov.primeraIdentificacionPor).toEqual({ userId: 'user-999', nombre: 'Otro' });
  });

  test('transición a status distinto de "identificado": no setea nada (queda null)', async () => {
    const mov = fakeMov({ status: 'identificado', primeraIdentificacionAt: null, primeraIdentificacionPor: null, erpIds: [] });
    BankMovement.findById.mockResolvedValue(mov);

    await bankService.updateStatus('mov-1', 'otros', { _id: 'user-1', nombre: 'Usuario Uno', role: 'admin' });

    expect(mov.status).toBe('otros');
    expect(mov.primeraIdentificacionAt).toBeNull();
    expect(mov.primeraIdentificacionPor).toBeNull();
  });
});

describe('updateStatus — ultimoCambioStatusAt (2026-10-06, corte histórico personalizado)', () => {
  test('el status SÍ cambia: se setea a la fecha actual', async () => {
    const mov = fakeMov({ status: 'no_identificado', ultimoCambioStatusAt: null });
    BankMovement.findById.mockResolvedValue(mov);

    await bankService.updateStatus('mov-1', 'identificado', { _id: 'user-1', nombre: 'Usuario Uno', role: 'admin' });

    expect(mov.ultimoCambioStatusAt).toBeInstanceOf(Date);
  });

  test('el status pedido es igual al actual: NO se toca ultimoCambioStatusAt', async () => {
    const mov = fakeMov({ status: 'identificado', ultimoCambioStatusAt: null, erpIds: ['CXC-1'] });
    BankMovement.findById.mockResolvedValue(mov);

    await bankService.updateStatus('mov-1', 'identificado', { _id: 'user-1', nombre: 'Usuario Uno', role: 'admin' });

    expect(mov.ultimoCambioStatusAt).toBeNull();
  });
});

describe('reclasifyMovements — ultimoCambioStatusAt (2026-10-06)', () => {
  test('el $set de cada updateOne incluye ultimoCambioStatusAt además de status', async () => {
    BankMovement.find = jest.fn().mockResolvedValue([
      { _id: 'mov-1', status: 'no_identificado' },
      { _id: 'mov-2', status: 'otros' },
    ]);
    BankMovement.bulkWrite = jest.fn().mockResolvedValue({});

    const result = await bankService.reclasifyMovements(['mov-1', 'mov-2'], { _id: 'user-1' });

    expect(result).toEqual({ reclasified: 2, omitidos: 0 });
    expect(BankMovement.find).toHaveBeenCalledWith(
      { _id: { $in: ['mov-1', 'mov-2'] }, status: { $ne: 'identificado' } },
      '_id status'
    );
    expect(BankMovement.bulkWrite).toHaveBeenCalledTimes(1);
    const ops = BankMovement.bulkWrite.mock.calls[0][0];
    expect(ops).toHaveLength(2);
    const [{ updateOne }] = ops;
    expect(updateOne.update.$set.status).toBe('reclasificado');
    expect(updateOne.update.$set.ultimoCambioStatusAt).toBeInstanceOf(Date);
  });

  test('sin ids: BadRequestError, no llama a find/bulkWrite', async () => {
    BankMovement.find = jest.fn();
    BankMovement.bulkWrite = jest.fn();
    await expect(bankService.reclasifyMovements([])).rejects.toThrow();
    expect(BankMovement.find).not.toHaveBeenCalled();
    expect(BankMovement.bulkWrite).not.toHaveBeenCalled();
  });

  test('bug real (2026-10-15): un movimiento ya identificado (ficha o erpLinks) queda excluido, no se pisa', async () => {
    BankMovement.find = jest.fn().mockResolvedValue([{ _id: 'mov-2', status: 'otros' }]);
    BankMovement.bulkWrite = jest.fn().mockResolvedValue({});

    const result = await bankService.reclasifyMovements(['mov-1', 'mov-2'], { _id: 'user-1' });

    expect(result).toEqual({ reclasified: 1, omitidos: 1 });
    const ops = BankMovement.bulkWrite.mock.calls[0][0];
    expect(ops).toHaveLength(1);
    expect(ops[0].updateOne.filter._id).toBe('mov-2');
  });

  test('registra autoría y status anterior en _changelog', async () => {
    BankMovement.find = jest.fn().mockResolvedValue([{ _id: 'mov-1', status: 'no_identificado' }]);
    BankMovement.bulkWrite = jest.fn().mockResolvedValue({});

    await bankService.reclasifyMovements(['mov-1'], { _id: 'user-1' });

    const [{ updateOne }] = BankMovement.bulkWrite.mock.calls[0][0];
    const entry = updateOne.update.$push._changelog;
    expect(entry.via).toBe('reclasificacion-masiva:user-1');
    expect(entry.campo).toBe('status');
    expect(entry.de).toBe('no_identificado');
    expect(entry.a).toBe('reclasificado');
    expect(entry.at).toBeInstanceOf(Date);
  });

  test('todos los movimientos ya están identificados: no llama a bulkWrite', async () => {
    BankMovement.find = jest.fn().mockResolvedValue([]);
    BankMovement.bulkWrite = jest.fn();

    const result = await bankService.reclasifyMovements(['mov-1'], { _id: 'user-1' });

    expect(result).toEqual({ reclasified: 0, omitidos: 1 });
    expect(BankMovement.bulkWrite).not.toHaveBeenCalled();
  });
});

describe('updateCategoria — primeraIdentificacionAt/primeraIdentificacionPor', () => {
  test('categoría cuya regla resuelve a "identificado" (status ya era identificado): agrega los campos al $set sin pisar valor existente', async () => {
    const fechaOriginal = new Date('2026-01-01T00:00:00.000Z');
    const porOriginal   = { userId: 'user-999', nombre: 'Otro' };
    const mov = fakeMov({
      status: 'identificado',
      categoria: null,
      primeraIdentificacionAt: fechaOriginal,
      primeraIdentificacionPor: porOriginal,
    });
    BankMovement.findById.mockResolvedValue(mov);
    BankMovement.updateOne = jest.fn().mockResolvedValue({});

    await bankService.updateCategoria('mov-1', 'Alguna Categoria', { _id: 'user-1', nombre: 'Usuario Uno' });

    expect(BankMovement.updateOne).toHaveBeenCalledTimes(1);
    const [, update] = BankMovement.updateOne.mock.calls[0];
    expect(update.$set.status).toBe('identificado');
    expect(update.$set.primeraIdentificacionAt).toBe(fechaOriginal);
    expect(update.$set.primeraIdentificacionPor).toBe(porOriginal);
  });
});
