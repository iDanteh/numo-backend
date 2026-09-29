'use strict';

// netpay-reporte-revert.service.test.js — netpay-matching-v2 (design.md "Unlink hooks" /
// "Revert (unlink)"): desvincular el erpId sintético NETPAYRPT-<claveRastreo> de un
// BankMovement YA NO vuelve el reporte a 'pendiente' (ese valor no existe en el enum v2) —
// pasa a discrepancia/revertido. También revierte CADA bucket NetpayMatch que este reporte
// había cerrado (snapshot.reporteIdOrigen === este reporte, estatusMatch:'resuelto_por_reporte')
// a discrepancia/reporte_revertido (design.md: "Every bucket it closed goes to
// discrepancia/reporte_revertido").
jest.mock('./NetpayReporte.model');
jest.mock('./NetpayMatch.model');
jest.mock('../banks/bank.service', () => ({ registerErpUnlinkHook: jest.fn() }));
jest.mock('../../shared/socket', () => ({ emitToAll: jest.fn() }));

const NetpayReporte = require('./NetpayReporte.model');
const NetpayMatch = require('./NetpayMatch.model');
const { registerErpUnlinkHook } = require('../banks/bank.service');
const { emitToAll } = require('../../shared/socket');
const { init, _revertirPorDesvinculacion } = require('./netpay-reporte-revert.service');

function fakeQuery(result) {
  const q = { session: jest.fn(() => q), then: (resolve) => resolve(result) };
  return q;
}

function fakeFind(result) {
  return { lean: jest.fn().mockResolvedValue(result) };
}

beforeEach(() => {
  jest.clearAllMocks();
  NetpayMatch.find = jest.fn(() => fakeFind([]));
  NetpayMatch.findOneAndUpdate = jest.fn().mockResolvedValue({});
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

  test('erpId NETPAYRPT- pero no hay reporte resuelto_por_reporte con ese movementId: no hace nada', async () => {
    NetpayReporte.findOne = jest.fn().mockReturnValue(fakeQuery(null));

    await _revertirPorDesvinculacion({ erpId: 'NETPAYRPT-CLAVE-1', movementId: 'mov-1', session: null, user: null });

    expect(NetpayReporte.findOne).toHaveBeenCalledWith({ movementIdConfirmado: 'mov-1', estatus: 'resuelto_por_reporte' });
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('erpId NETPAYRPT- con reporte resuelto_por_reporte: pasa a discrepancia/revertido (NUNCA vuelve a pendiente — ese valor ya no existe)', async () => {
    const reporte = {
      _id: 'rep-1', estatus: 'resuelto_por_reporte', vinculo: 'erp-link', movementIdConfirmado: 'mov-1',
      motivoDiscrepancia: null, revertido: null,
      save: jest.fn().mockResolvedValue(undefined),
    };
    NetpayReporte.findOne = jest.fn().mockReturnValue(fakeQuery(reporte));

    await _revertirPorDesvinculacion({ erpId: 'NETPAYRPT-CLAVE-1', movementId: 'mov-1', session: null, user: null });

    expect(reporte.estatus).toBe('discrepancia');
    expect(reporte.motivoDiscrepancia).toBe('revertido');
    expect(reporte.revertido).toEqual({ en: expect.any(Date), movementIds: ['mov-1'] });
    expect(reporte.save).toHaveBeenCalled();
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
  });

  // design.md: "Every bucket it closed goes to discrepancia/reporte_revertido".
  test('reporte que había cerrado 2 buckets (snapshot.reporteIdOrigen === este reporte): ambos pasan a discrepancia/reporte_revertido', async () => {
    const reporte = {
      _id: 'rep-1', estatus: 'resuelto_por_reporte', vinculo: 'erp-link', movementIdConfirmado: 'mov-1',
      motivoDiscrepancia: null, revertido: null,
      save: jest.fn().mockResolvedValue(undefined),
    };
    NetpayReporte.findOne = jest.fn().mockReturnValue(fakeQuery(reporte));
    NetpayMatch.find = jest.fn(() => fakeFind([
      { _id: 'nm-1', estatusMatch: 'resuelto_por_reporte' },
      { _id: 'nm-2', estatusMatch: 'resuelto_por_reporte' },
    ]));

    await _revertirPorDesvinculacion({ erpId: 'NETPAYRPT-CLAVE-1', movementId: 'mov-1', session: null, user: null });

    expect(NetpayMatch.find).toHaveBeenCalledWith({ 'snapshot.reporteIdOrigen': 'rep-1', estatusMatch: 'resuelto_por_reporte' });
    expect(NetpayMatch.findOneAndUpdate).toHaveBeenCalledTimes(2);
    expect(NetpayMatch.findOneAndUpdate).toHaveBeenCalledWith(
      { _id: 'nm-1', estatusMatch: 'resuelto_por_reporte' },
      { $set: { estatusMatch: 'discrepancia', motivoDiscrepancia: 'reporte_revertido' } },
    );
    expect(NetpayMatch.findOneAndUpdate).toHaveBeenCalledWith(
      { _id: 'nm-2', estatusMatch: 'resuelto_por_reporte' },
      { $set: { estatusMatch: 'discrepancia', motivoDiscrepancia: 'reporte_revertido' } },
    );
  });

  test('reporte sin ningún bucket cerrado: no llama a NetpayMatch.findOneAndUpdate', async () => {
    const reporte = {
      _id: 'rep-1', estatus: 'resuelto_por_reporte', movementIdConfirmado: 'mov-1',
      motivoDiscrepancia: null, revertido: null, save: jest.fn().mockResolvedValue(undefined),
    };
    NetpayReporte.findOne = jest.fn().mockReturnValue(fakeQuery(reporte));

    await _revertirPorDesvinculacion({ erpId: 'NETPAYRPT-CLAVE-1', movementId: 'mov-1', session: null, user: null });

    expect(NetpayMatch.findOneAndUpdate).not.toHaveBeenCalled();
  });

  test('con session: se usa .session(session) en el find del reporte, se propaga al save', async () => {
    const sesionFalsa = { id: 'sesion-falsa' };
    const reporte = {
      _id: 'rep-1', estatus: 'resuelto_por_reporte', movementIdConfirmado: 'mov-1',
      motivoDiscrepancia: null, revertido: null, save: jest.fn().mockResolvedValue(undefined),
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
