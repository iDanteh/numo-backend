'use strict';

// Timeout de 60 min en el polling del SAT: el checkpoint conserva el IdSolicitud
// (solicitudPendiente) y recuperarSolicitudesPendientes() lo retoma sin pedir una
// solicitud nueva.

jest.mock('node-cron', () => ({ schedule: jest.fn(() => ({ stop: jest.fn() })) }));
jest.mock('../models/AppConfig', () => ({ find: () => ({ lean: async () => [] }) }));
jest.mock('../controllers/schedule.controller', () => ({ restaurarProgramados: jest.fn() }));
jest.mock('../models/SatDescargaLog', () => ({ create: jest.fn(async () => ({ _id: 'log1' })), updateOne: jest.fn(async () => ({})) }));
jest.mock('../models/SatJobCheckpoint', () => ({
  findOne: jest.fn(),
  find: jest.fn(),
  findById: jest.fn(),
  findOneAndUpdate: jest.fn(),
  updateOne: jest.fn(async () => ({})),
  updateMany: jest.fn(async () => ({})),
}));
jest.mock('../sat/download', () => ({
  solicitar: jest.fn(),
  verificar: jest.fn(),
  descargarPaquete: jest.fn(async () => []),
  descargarPaqueteMetadata: jest.fn(async () => []),
}));
jest.mock('../sat/credenciales', () => ({ obtener: jest.fn(async () => ({ cer: 1 })), tieneCredenciales: jest.fn() }));
jest.mock('../sat/rateLimiter', () => ({ puedeIniciar: jest.fn(async () => ({ puede: false })), registrarInicio: jest.fn(), registrarFin: jest.fn() }));

const SatJobCheckpoint = require('../models/SatJobCheckpoint');
const { solicitar, verificar } = require('../sat/download');
const { recuperarSolicitudesPendientes, procesarDescarga } = require('./satSyncJob');

const pendiente = (extra = {}) => ({
  _id: 'cp1', rfc: 'CCO011113663', fecha: '2026-10-09', fechaFin: '2026-10-11',
  tipoComprobante: 'Ingresos', ejercicio: 2026, periodo: 10, status: 'error',
  solicitudPendiente: true, idSolicitud: '60826a79', solicitadaEn: new Date(), ...extra,
});

const timeoutErr = () => Object.assign(new Error('Timeout: solicitud 60826a79 no terminó después de 3 minutos'), { code: 'SAT_TIMEOUT_POLLING' });

const setsDe = (mock) => mock.mock.calls.map(([, upd]) => upd.$set);

beforeEach(() => {
  jest.clearAllMocks();
  SatJobCheckpoint.find.mockReturnValue({ lean: async () => [pendiente()] });
  SatJobCheckpoint.findOneAndUpdate.mockResolvedValue(pendiente());
});

test('sin pendientes no consulta al SAT', async () => {
  SatJobCheckpoint.find.mockReturnValue({ lean: async () => [] });
  await recuperarSolicitudesPendientes();
  expect(verificar).not.toHaveBeenCalled();
  expect(solicitar).not.toHaveBeenCalled();
});

test('separa Emitidos de Recibidos en el filtro', async () => {
  await recuperarSolicitudesPendientes({ recibidos: true });
  const filtro = SatJobCheckpoint.find.mock.calls[0][0];
  expect(filtro.tipoComprobante.$regex.test('Recibidos')).toBe(true);
  await recuperarSolicitudesPendientes();
  const filtroE = SatJobCheckpoint.find.mock.calls[1][0];
  expect(filtroE.tipoComprobante.$not.test('Ingresos')).toBe(false);
});

test('si el SAT sigue en proceso, la deja pendiente sin pedir otra', async () => {
  verificar.mockRejectedValue(timeoutErr());
  await recuperarSolicitudesPendientes();
  expect(verificar).toHaveBeenCalledWith('60826a79', 'CCO011113663', expect.anything(), { maxIntentos: 3 });
  expect(setsDe(SatJobCheckpoint.updateOne)).toContainEqual({ solicitudPendiente: true });
  expect(solicitar).not.toHaveBeenCalled();
});

test('si el SAT la rechaza, la descarta (queda como error normal)', async () => {
  verificar.mockRejectedValue(new Error('SAT: La solicitud 60826a79 venció (72 horas).'));
  await recuperarSolicitudesPendientes();
  const sets = setsDe(SatJobCheckpoint.updateOne);
  expect(sets.some(s => s.solicitudPendiente === true)).toBe(false);
  expect(sets[0].error).toMatch(/no recuperable/);
  expect(solicitar).not.toHaveBeenCalled();
});

test('una falla de red no la descarta', async () => {
  verificar.mockRejectedValue(new Error('socket hang up'));
  await recuperarSolicitudesPendientes();
  expect(setsDe(SatJobCheckpoint.updateOne)).toContainEqual({ solicitudPendiente: true });
});

test('terminada sin paquetes → completado', async () => {
  verificar.mockResolvedValue({ idsPaquetes: [], totalCfdis: 0 });
  await recuperarSolicitudesPendientes();
  expect(setsDe(SatJobCheckpoint.updateOne)[0]).toMatchObject({ status: 'completado' });
});

test('terminada con paquetes → descargando y baja la MISMA solicitud', async () => {
  verificar.mockResolvedValue({ idsPaquetes: ['P1'], totalCfdis: 0 });
  // descargarPorSubtipo lee el checkpoint ya en 'descargando'
  SatJobCheckpoint.findOne.mockResolvedValue({ ...pendiente(), status: 'descargando', idsPaquetes: ['P1'], paquetesProcesados: [], updatedAt: new Date() });
  SatJobCheckpoint.findById.mockReturnValue({ lean: async () => ({ paquetesProcesados: ['P1'], paquetesFallidos: [] }) });
  await recuperarSolicitudesPendientes();
  expect(setsDe(SatJobCheckpoint.updateOne)[0]).toMatchObject({ status: 'descargando', idsPaquetes: ['P1'], totalReportadoSAT: 0 });
  expect(SatJobCheckpoint.findOne).toHaveBeenCalledWith({ rfc: 'CCO011113663', fecha: '2026-10-09', tipoComprobante: 'Ingresos' });
  expect(verificar.mock.calls.every(c => c[0] === '60826a79')).toBe(true);
  expect(solicitar).not.toHaveBeenCalled();
});

test('otra corrida ya la reclamó → no la procesa', async () => {
  SatJobCheckpoint.findOneAndUpdate.mockResolvedValue(null);
  await recuperarSolicitudesPendientes();
  expect(verificar).not.toHaveBeenCalled();
});

describe('al vencer el polling de 60 min', () => {
  const correr = (tipoSolicitud) => procesarDescarga({
    rfc: 'CCO011113663', fechaInicio: '2026-10-09T00:00:00', fechaFin: '2026-10-11T23:59:59',
    tipoComprobante: 'Ingresos', tipoSolicitud, creds: {}, ejercicio: 2026, periodo: 10,
  }).catch(() => {});

  beforeEach(() => {
    SatJobCheckpoint.findOne.mockResolvedValue(null);
    SatJobCheckpoint.findOneAndUpdate.mockResolvedValue({ _id: 'cp1' });
    solicitar.mockResolvedValue('60826a79');
    verificar.mockRejectedValue(timeoutErr());
  });

  test('XML: queda en error con solicitudPendiente y conserva la solicitud', async () => {
    await correr('CFDI');
    const sets = setsDe(SatJobCheckpoint.updateOne);
    expect(sets).toContainEqual(expect.objectContaining({ idSolicitud: '60826a79', status: 'verificando', solicitadaEn: expect.any(Date) }));
    const final = sets.find(s => s.status === 'error');
    expect(final).toMatchObject({ solicitudPendiente: true });
    expect(final.error).toMatch(/^Timeout: .*se retomará/);
    expect(final).not.toHaveProperty('idSolicitud');
    expect(solicitar).toHaveBeenCalledTimes(1);
  });

  test('Metadata: no se marca para recuperar', async () => {
    await correr('Metadata');
    const final = setsDe(SatJobCheckpoint.updateOne).find(s => s.status === 'error');
    expect(final.solicitudPendiente).toBe(false);
  });
});

describe('descarga del mismo rango con una solicitud pendiente', () => {
  // Pedida el 12-oct 10:00 MX: el rango 9→11 ya había terminado → su foto está completa
  const pedida = new Date('2026-10-12T16:00:00Z');
  const cpPendiente = (extra = {}) => ({ ...pendiente({ solicitadaEn: pedida }), paquetesProcesados: [], updatedAt: pedida, ...extra });
  const correr = (fechaFin = '2026-10-11T23:59:59') => procesarDescarga({
    rfc: 'CCO011113663', fechaInicio: '2026-10-09T00:00:00', fechaFin,
    tipoComprobante: 'Ingresos', tipoSolicitud: 'CFDI', creds: {}, ejercicio: 2026, periodo: 10, tipo: 'manual',
  });

  beforeEach(() => {
    jest.useFakeTimers({ now: new Date('2026-10-12T20:00:00Z'), doNotFake: ['setTimeout', 'setImmediate', 'nextTick'] });
    SatJobCheckpoint.findOne.mockResolvedValue(cpPendiente());
    SatJobCheckpoint.findOneAndUpdate.mockResolvedValue({ _id: 'cp1' });
    SatJobCheckpoint.findById.mockReturnValue({ lean: async () => ({ paquetesProcesados: ['P1'], paquetesFallidos: [] }) });
    solicitar.mockResolvedValue('nueva01');
  });
  afterEach(() => jest.useRealTimers());

  test('el SAT ya la terminó → baja SUS paquetes sin pedir otra', async () => {
    verificar.mockResolvedValue({ idsPaquetes: ['P1'], totalCfdis: 0 });
    await correr().catch(() => {});
    expect(verificar).toHaveBeenCalledWith('60826a79', 'CCO011113663', expect.anything(), { maxIntentos: 3 });
    expect(setsDe(SatJobCheckpoint.updateOne)).toContainEqual(expect.objectContaining({ status: 'descargando', idsPaquetes: ['P1'] }));
    expect(solicitar).not.toHaveBeenCalled();
  });

  test('sigue en proceso → avisa, la conserva y no pide otra', async () => {
    verificar.mockRejectedValue(timeoutErr());
    await expect(correr()).rejects.toThrow(/aún está procesando la solicitud 60826a79/);
    expect(setsDe(SatJobCheckpoint.updateOne)).toContainEqual({ solicitudPendiente: true });
    expect(solicitar).not.toHaveBeenCalled();
  });

  test('el SAT la rechazó → pide una nueva', async () => {
    verificar.mockRejectedValueOnce(new Error('SAT: La solicitud 60826a79 venció (72 horas).'))
      .mockResolvedValue({ idsPaquetes: [], totalCfdis: 0 });
    await correr().catch(() => {});
    expect(solicitar).toHaveBeenCalledTimes(1);
  });

  test('se pide hasta otra fecha (lo nuevo) → solicitud nueva, no toca la vieja', async () => {
    verificar.mockResolvedValue({ idsPaquetes: [], totalCfdis: 0 });
    await correr('2026-10-12T23:59:59').catch(() => {});
    expect(solicitar).toHaveBeenCalledTimes(1);
    expect(verificar).not.toHaveBeenCalledWith('60826a79', expect.anything(), expect.anything(), expect.anything());
  });

  test('mismo rango pero pedida antes de que terminara → foto incompleta, solicitud nueva', async () => {
    SatJobCheckpoint.findOne.mockResolvedValue(cpPendiente({ solicitadaEn: new Date('2026-10-11T20:00:00Z') }));
    verificar.mockResolvedValue({ idsPaquetes: [], totalCfdis: 0 });
    await correr().catch(() => {});
    expect(solicitar).toHaveBeenCalledTimes(1);
  });

  test('más de 72 h → solicitud nueva', async () => {
    jest.setSystemTime(new Date('2026-10-15T17:00:00Z'));
    verificar.mockResolvedValue({ idsPaquetes: [], totalCfdis: 0 });
    await correr().catch(() => {});
    expect(solicitar).toHaveBeenCalledTimes(1);
  });
});
