'use strict';

// netpay-match-revert.service.test.js — netpay-matching-v2 (design.md "Revert (unlink)"):
// desvincular el erpId sintético NETPAY-<terminalID>-<día> de un BankMovement YA NO borra
// el NetpayMatch (comportamiento v1) — el documento pasa a discrepancia/revertido y NUNCA
// se vuelve a auto-confirmar (netpay-evaluacion.service.js#_debeReevaluarse lo salta a
// propósito). También resetea cualquier NetpayReporte 'corroborado' que dependía del mismo
// movimiento (design.md: "resets any corroborado report on M the same way").
jest.mock('./NetpayMatch.model');
jest.mock('./NetpayReporte.model');
jest.mock('../banks/bank.service', () => ({ registerErpUnlinkHook: jest.fn() }));
jest.mock('../../shared/socket', () => ({ emitToAll: jest.fn() }));

const NetpayMatch = require('./NetpayMatch.model');
const NetpayReporte = require('./NetpayReporte.model');
const { registerErpUnlinkHook } = require('../banks/bank.service');
const { emitToAll } = require('../../shared/socket');
const { init, _revertirPorDesvinculacion } = require('./netpay-match-revert.service');

function fakeQuery(result) {
  const q = { session: jest.fn(() => q), then: (resolve) => resolve(result) };
  return q;
}

beforeEach(() => {
  jest.clearAllMocks();
  NetpayReporte.findOne = jest.fn().mockReturnValue(fakeQuery(null));
});

test('init(): registra _revertirPorDesvinculacion como hook de desvinculación en bank.service.js', () => {
  init();
  expect(registerErpUnlinkHook).toHaveBeenCalledWith(_revertirPorDesvinculacion);
});

describe('_revertirPorDesvinculacion', () => {
  test('erpId que no empieza con NETPAY- (ej. NETPAYRPT- o real de Kore): no dispara ninguna query', async () => {
    NetpayMatch.findOne = jest.fn();
    await _revertirPorDesvinculacion({ erpId: 'NETPAYRPT-CLAVE-1', movementId: 'mov-1', session: null, user: null });
    expect(NetpayMatch.findOne).not.toHaveBeenCalled();
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('erpId vacío/null: no dispara ninguna query', async () => {
    NetpayMatch.findOne = jest.fn();
    await _revertirPorDesvinculacion({ erpId: null, movementId: 'mov-1', session: null, user: null });
    expect(NetpayMatch.findOne).not.toHaveBeenCalled();
  });

  test('erpId NETPAY- pero no hay bucket confirmado_automatico/resuelto_manual con ese movementId: no hace nada', async () => {
    NetpayMatch.findOne = jest.fn().mockReturnValue(fakeQuery(null));

    await _revertirPorDesvinculacion({ erpId: 'NETPAY-2840403056-2026-09-10', movementId: 'mov-1', session: null, user: null });

    expect(NetpayMatch.findOne).toHaveBeenCalledWith({
      movementIdsConfirmados: 'mov-1',
      estatusMatch: { $in: ['confirmado_automatico', 'resuelto_manual'] },
    });
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('bucket confirmado_automatico: pasa a discrepancia/revertido — NO BORRA el documento (a diferencia de v1)', async () => {
    const match = { _id: 'nm-1', estatusMatch: 'confirmado_automatico', motivoDiscrepancia: null, revertido: null, save: jest.fn().mockResolvedValue(undefined) };
    NetpayMatch.findOne = jest.fn().mockReturnValue(fakeQuery(match));
    NetpayMatch.deleteOne = jest.fn();

    await _revertirPorDesvinculacion({ erpId: 'NETPAY-2840403056-2026-09-10', movementId: 'mov-1', session: null, user: null });

    expect(NetpayMatch.deleteOne).not.toHaveBeenCalled();
    expect(match.estatusMatch).toBe('discrepancia');
    expect(match.motivoDiscrepancia).toBe('revertido');
    expect(match.revertido).toEqual({ en: expect.any(Date), movementIds: ['mov-1'] });
    expect(match.save).toHaveBeenCalled();
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
  });

  test('bucket resuelto_manual: también se revierte a discrepancia/revertido (mismo criterio que confirmado_automatico)', async () => {
    const match = { _id: 'nm-2', estatusMatch: 'resuelto_manual', motivoDiscrepancia: null, revertido: null, save: jest.fn().mockResolvedValue(undefined) };
    NetpayMatch.findOne = jest.fn().mockReturnValue(fakeQuery(match));

    await _revertirPorDesvinculacion({ erpId: 'NETPAY-2840403056-2026-09-10', movementId: 'mov-2', session: null, user: null });

    expect(match.estatusMatch).toBe('discrepancia');
    expect(match.motivoDiscrepancia).toBe('revertido');
  });

  // design.md "Revert (unlink)": "resets any corroborado report on M the same way" — un
  // NetpayReporte que se resolvió como 'corroborado' (sin link propio, solo confirmando un
  // bucket YA confirmado_automatico con el mismo movimiento) debe volver a discrepancia
  // cuando ESE movimiento se desvincula, aunque el reporte en sí nunca tuvo su propio erpId.
  test('reporte vinculo:corroborado sobre el MISMO movimiento: también se resetea a discrepancia/reporte_revertido', async () => {
    const match = { _id: 'nm-1', estatusMatch: 'confirmado_automatico', motivoDiscrepancia: null, revertido: null, save: jest.fn().mockResolvedValue(undefined) };
    NetpayMatch.findOne = jest.fn().mockReturnValue(fakeQuery(match));
    const reporteCorroborado = {
      _id: 'rep-1', estatus: 'resuelto_por_reporte', vinculo: 'corroborado', motivoDiscrepancia: null, revertido: null,
      save: jest.fn().mockResolvedValue(undefined),
    };
    NetpayReporte.findOne = jest.fn().mockReturnValue(fakeQuery(reporteCorroborado));

    await _revertirPorDesvinculacion({ erpId: 'NETPAY-2840403056-2026-09-10', movementId: 'mov-1', session: null, user: null });

    expect(NetpayReporte.findOne).toHaveBeenCalledWith({
      movementIdConfirmado: 'mov-1', vinculo: 'corroborado', estatus: 'resuelto_por_reporte',
    });
    expect(reporteCorroborado.estatus).toBe('discrepancia');
    expect(reporteCorroborado.motivoDiscrepancia).toBe('reporte_revertido');
    expect(reporteCorroborado.save).toHaveBeenCalled();
  });

  test('sin reporte corroborado sobre ese movimiento: no toca NetpayReporte', async () => {
    const match = { _id: 'nm-1', estatusMatch: 'confirmado_automatico', motivoDiscrepancia: null, revertido: null, save: jest.fn().mockResolvedValue(undefined) };
    NetpayMatch.findOne = jest.fn().mockReturnValue(fakeQuery(match));

    await _revertirPorDesvinculacion({ erpId: 'NETPAY-2840403056-2026-09-10', movementId: 'mov-1', session: null, user: null });

    expect(NetpayReporte.findOne).toHaveBeenCalled();
  });

  test('con session: se usa .session(session) en ambos finds y se propaga a ambos saves', async () => {
    const sesionFalsa = { id: 'sesion-falsa' };
    const match = { _id: 'nm-1', estatusMatch: 'confirmado_automatico', motivoDiscrepancia: null, revertido: null, save: jest.fn().mockResolvedValue(undefined) };
    const matchQuery = fakeQuery(match);
    NetpayMatch.findOne = jest.fn().mockReturnValue(matchQuery);
    const reporteCorroborado = { _id: 'rep-1', estatus: 'resuelto_por_reporte', vinculo: 'corroborado', motivoDiscrepancia: null, revertido: null, save: jest.fn().mockResolvedValue(undefined) };
    const reporteQuery = fakeQuery(reporteCorroborado);
    NetpayReporte.findOne = jest.fn().mockReturnValue(reporteQuery);

    await _revertirPorDesvinculacion({ erpId: 'NETPAY-2840403056-2026-09-10', movementId: 'mov-1', session: sesionFalsa, user: null });

    expect(matchQuery.session).toHaveBeenCalledWith(sesionFalsa);
    expect(reporteQuery.session).toHaveBeenCalledWith(sesionFalsa);
    expect(match.save).toHaveBeenCalledWith({ session: sesionFalsa });
    expect(reporteCorroborado.save).toHaveBeenCalledWith({ session: sesionFalsa });
  });
});
