'use strict';

// caja-transferencia-descartar-manual.service.test.js — descarte MANUAL (pedido explícito
// del usuario 2026-09-10): re-valida 'pendiente' + sin candidatos server-side, marca
// 'descartada-manual' con trazabilidad, y NUNCA toca BankMovement/erpLinks.
jest.mock('./CajaTransferencia.model');
jest.mock('./caja-transferencia-match.service', () => ({ buscarCandidatos: jest.fn() }));

const CajaTransferencia    = require('./CajaTransferencia.model');
const { buscarCandidatos } = require('./caja-transferencia-match.service');
const { descartarManual }  = require('./caja-transferencia-descartar-manual.service');

const USER = { _id: 'user-1', nombre: 'Ana' };

function fakeTransferencia(overrides = {}) {
  return {
    _id: 't-1', koreId: 'kore-1', monto: 1500, estatusMatch: 'pendiente',
    toObject: function () { return { ...this }; },
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
});

test('transferencia inexistente: NotFoundError', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(null);
  await expect(descartarManual('t-1', USER)).rejects.toThrow('Transferencia de caja');
});

test('transferencia ya no está pendiente: ConflictError', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia({ estatusMatch: 'matcheada' }));
  await expect(descartarManual('t-1', USER)).rejects.toThrow(/ya no está pendiente/);
  expect(buscarCandidatos).not.toHaveBeenCalled();
});

test('la transferencia ya tiene candidato(s) reaparecidos: ConflictError, no se descarta', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia());
  buscarCandidatos.mockResolvedValue([[{ _id: 'mov-1' }]]);

  await expect(descartarManual('t-1', USER)).rejects.toThrow(/ya tiene candidato\(s\)/);
  expect(CajaTransferencia.updateOne).not.toHaveBeenCalled();
});

test('descarte válido: marca descartada-manual con trazabilidad, no toca BankMovement/erpLinks', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia());
  CajaTransferencia.updateOne = jest.fn().mockResolvedValue({});
  buscarCandidatos.mockResolvedValue([]);

  const res = await descartarManual('t-1', USER);

  expect(CajaTransferencia.updateOne).toHaveBeenCalledWith(
    { _id: 't-1' },
    { $set: expect.objectContaining({
      estatusMatch: 'descartada-manual',
      descartadoManualmentePor: { userId: 'user-1', nombre: 'Ana' },
      descartadoManualmenteEn: expect.any(Date),
    }) },
  );
  expect(res.transferencia.estatusMatch).toBe('descartada-manual');
});

test('descarte válido: usa el email como nombre si el usuario no trae nombre', async () => {
  CajaTransferencia.findById = jest.fn().mockResolvedValue(fakeTransferencia());
  CajaTransferencia.updateOne = jest.fn().mockResolvedValue({});
  buscarCandidatos.mockResolvedValue([]);

  await descartarManual('t-1', { _id: 'user-2', email: 'ana@numo.mx' });

  expect(CajaTransferencia.updateOne).toHaveBeenCalledWith(
    { _id: 't-1' },
    { $set: expect.objectContaining({
      descartadoManualmentePor: { userId: 'user-2', nombre: 'ana@numo.mx' },
    }) },
  );
});
