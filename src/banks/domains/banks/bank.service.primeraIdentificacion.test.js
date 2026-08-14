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
