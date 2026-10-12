'use strict';

// Descarga manual: un checkpoint que quedó en 'descargando' (ej. el contenedor se
// reinició a media descarga) ya no bloquea para siempre con el 409.

jest.mock('express-validator', () => ({ validationResult: () => ({ isEmpty: () => true }) }));
jest.mock('../jobs/satSyncJob', () => ({ procesarDescarga: jest.fn(() => new Promise(() => {})), ejecutarDescargaERP: jest.fn() }));
jest.mock('../services/periodoFiscal.service', () => ({ resolverPeriodo: jest.fn(async () => ({})) }));
jest.mock('../sat/credenciales', () => ({ tieneCredenciales: jest.fn(async () => ({ tiene: true })), obtener: jest.fn(async () => ({})), limpiarBuffers: jest.fn() }));
jest.mock('../sat/rateLimiter', () => ({ puedeIniciar: jest.fn(async () => ({ puede: true })), registrarInicio: jest.fn(async () => {}), registrarFin: jest.fn(), getEstado: jest.fn() }));
jest.mock('../models/SatJobCheckpoint', () => ({ updateMany: jest.fn(), findOne: jest.fn() }));

const SatJobCheckpoint = require('../models/SatJobCheckpoint');
const { startDownload } = require('./sat.controller');

const llamar = async () => {
  // asyncHandler no devuelve la promesa: se espera a que el handler responda (o falle)
  const res = { status: jest.fn(function () { return this; }) };
  await new Promise((resolve, reject) => {
    res.json = jest.fn(resolve);
    startDownload({ body: {
      rfc: 'CCO011113663', fechaInicio: '2026-08-01', fechaFin: '2026-08-05',
      tipoComprobante: 'Recibidos', tipoSolicitud: 'CFDI', ejercicio: 2026, periodo: 8,
    } }, res, reject);
  });
  return res;
};

beforeEach(() => jest.clearAllMocks());

test('antes de checar el 409 marca como error los checkpoints sin avance en 2 h', async () => {
  SatJobCheckpoint.updateMany.mockResolvedValue({ modifiedCount: 1 });
  SatJobCheckpoint.findOne.mockReturnValue({ lean: async () => null });
  const res = await llamar();
  const [filtro, upd] = SatJobCheckpoint.updateMany.mock.calls[0];
  expect(filtro.status.$in).toEqual(['solicitando', 'verificando', 'descargando']);
  const horas = (Date.now() - filtro.updatedAt.$lt.getTime()) / 3_600_000;
  expect(horas).toBeCloseTo(2, 1);
  expect(upd.$set.status).toBe('error');
  expect(res.status).toHaveBeenCalledWith(202);
});

test('uno que sí avanza (reciente) sigue bloqueando con 409', async () => {
  SatJobCheckpoint.updateMany.mockResolvedValue({ modifiedCount: 0 });
  SatJobCheckpoint.findOne.mockReturnValue({ lean: async () => ({ tipoComprobante: 'Recibidos', fecha: '2026-08-01', status: 'descargando' }) });
  const res = await llamar();
  expect(res.status).toHaveBeenCalledWith(409);
});
