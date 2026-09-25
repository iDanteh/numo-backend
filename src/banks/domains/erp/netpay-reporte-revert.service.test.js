'use strict';

// netpay-reporte-revert.service.test.js — desvincular el erpId sintético
// NETPAYRPT-<claveRastreo> de un BankMovement debe volver el NetpayReporte a 'pendiente'
// SIN borrar el documento (a diferencia de netpay-match-revert.service.js, que sí borra —
// ver el comentario de diseño en el propio service).
jest.mock('./NetpayReporte.model');
jest.mock('../banks/bank.service', () => ({ registerErpUnlinkHook: jest.fn() }));
jest.mock('../../shared/socket', () => ({ emitToAll: jest.fn() }));

const NetpayReporte = require('./NetpayReporte.model');
const { registerErpUnlinkHook } = require('../banks/bank.service');
const { emitToAll } = require('../../shared/socket');
const { init, _revertirPorDesvinculacion } = require('./netpay-reporte-revert.service');

function fakeQuery(result) {
  const q = { session: jest.fn(() => q), then: (resolve) => resolve(result) };
  return q;
}

beforeEach(() => {
  jest.clearAllMocks();
});

test('init(): registra _revertirPorDesvinculacion como hook de desvinculación en bank.service.js', () => {
  init();
  expect(registerErpUnlinkHook).toHaveBeenCalledWith(_revertirPorDesvinculacion);
});

describe('_revertirPorDesvinculacion', () => {
  test('erpId que no empieza con NETPAYRPT- (ej. NETPAY- del matching automático, o real de Kore): no dispara ninguna query', async () => {
    NetpayReporte.findOne = jest.fn();
    await _revertirPorDesvinculacion({ erpId: 'NETPAY-2840403056-2026-09-10', movementId: 'mov-1', session: null, user: null });
    expect(NetpayReporte.findOne).not.toHaveBeenCalled();
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('erpId vacío/null: no dispara ninguna query', async () => {
    NetpayReporte.findOne = jest.fn();
    await _revertirPorDesvinculacion({ erpId: null, movementId: 'mov-1', session: null, user: null });
    expect(NetpayReporte.findOne).not.toHaveBeenCalled();
  });

  test('erpId NETPAYRPT- pero no hay reporte confirmado con ese movementId: no hace nada', async () => {
    NetpayReporte.findOne = jest.fn().mockReturnValue(fakeQuery(null));

    await _revertirPorDesvinculacion({ erpId: 'NETPAYRPT-CLAVE-1', movementId: 'mov-1', session: null, user: null });

    expect(NetpayReporte.findOne).toHaveBeenCalledWith({ movementIdConfirmado: 'mov-1', estatus: 'confirmado' });
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('erpId NETPAYRPT- con reporte confirmado: vuelve a pendiente, NO borra el documento, limpia trazabilidad de confirmación', async () => {
    const reporte = {
      _id: 'rep-1', estatus: 'confirmado', movementIdConfirmado: 'mov-1',
      confirmadoPor: { userId: 'u1', nombre: 'Ana' }, confirmadoEn: new Date(),
      save: jest.fn().mockResolvedValue(undefined),
    };
    NetpayReporte.findOne = jest.fn().mockReturnValue(fakeQuery(reporte));

    await _revertirPorDesvinculacion({ erpId: 'NETPAYRPT-CLAVE-1', movementId: 'mov-1', session: null, user: null });

    expect(reporte.estatus).toBe('pendiente');
    expect(reporte.movementIdConfirmado).toBeNull();
    expect(reporte.confirmadoPor).toBeNull();
    expect(reporte.confirmadoEn).toBeNull();
    expect(reporte.save).toHaveBeenCalled();
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
  });

  test('con session: se usa .session(session) en el find y se propaga al save', async () => {
    const sesionFalsa = { id: 'sesion-falsa' };
    const reporte = {
      _id: 'rep-1', estatus: 'confirmado', movementIdConfirmado: 'mov-1',
      confirmadoPor: {}, confirmadoEn: new Date(),
      save: jest.fn().mockResolvedValue(undefined),
    };
    const query = fakeQuery(reporte);
    NetpayReporte.findOne = jest.fn().mockReturnValue(query);

    await _revertirPorDesvinculacion({ erpId: 'NETPAYRPT-CLAVE-1', movementId: 'mov-1', session: sesionFalsa, user: null });

    expect(query.session).toHaveBeenCalledWith(sesionFalsa);
    expect(reporte.save).toHaveBeenCalledWith({ session: sesionFalsa });
  });

  test('prefijos NETPAY- y NETPAYRPT- nunca colisionan (guard de diseño)', () => {
    expect('NETPAYRPT-CLAVE-1'.startsWith('NETPAY-')).toBe(false);
  });
});
