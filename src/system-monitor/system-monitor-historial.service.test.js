'use strict';

jest.mock('./SystemMonitorSnapshot.model');
jest.mock('./SystemMonitorErrorLog.model');
jest.mock('../shared/utils/db-health');

const SystemMonitorSnapshot = require('./SystemMonitorSnapshot.model');
const SystemMonitorErrorLog = require('./SystemMonitorErrorLog.model');
const { checkMongoOk } = require('../shared/utils/db-health');
const { guardarSnapshot, getHistorial, guardarError, getErroresHistorial } = require('./system-monitor-historial.service');

function snapshotFixture(overrides = {}) {
  return {
    generadoEn: '2026-09-24T18:00:00.000Z',
    estadoGeneral: 'normal',
    requestsPorMinuto: 42,
    requestsEnCurso: 3,
    tasaErrorPct: 1.5,
    tiempoRespuestaPromedioMs: 85,
    uptimeSegundos: 3725,
    memoria: { rssMb: 120, heapUsedMb: 60, heapTotalMb: 90 },
    memoriaHost: { totalMb: 16384, freeMb: 4096, usadoPct: 75 },
    cpu: { load1: 1.2, load5: 1.1, load15: 0.9, cores: 4 },
    eventLoopLagMs: 4.2,
    serieUltimaHora: [
      { ts: 0, total: 5, errores: 1 },
      { ts: 60000, total: 8, errores: 2 }, // el más reciente
    ],
    ...overrides,
  };
}

describe('guardarSnapshot', () => {
  beforeEach(() => jest.clearAllMocks());

  test('guarda el resumen con erroresUltimoMinuto tomado del ÚLTIMO punto de serieUltimaHora', async () => {
    await guardarSnapshot(snapshotFixture());

    expect(SystemMonitorSnapshot.create).toHaveBeenCalledTimes(1);
    const doc = SystemMonitorSnapshot.create.mock.calls[0][0];
    expect(doc.fecha).toEqual(new Date('2026-09-24T18:00:00.000Z'));
    expect(doc.erroresUltimoMinuto).toBe(2); // NO el primero (1), el último del array
    expect(doc.requestsPorMinuto).toBe(42);
    expect(doc.estadoGeneral).toBe('normal');
    expect(doc.cpu).toEqual({ load1: 1.2, load5: 1.1, load15: 0.9, cores: 4 });
    expect(doc.memoriaHost).toEqual({ totalMb: 16384, freeMb: 4096, usadoPct: 75 });
  });

  test('serieUltimaHora vacía no revienta — erroresUltimoMinuto cae a 0', async () => {
    await guardarSnapshot(snapshotFixture({ serieUltimaHora: [] }));

    const doc = SystemMonitorSnapshot.create.mock.calls[0][0];
    expect(doc.erroresUltimoMinuto).toBe(0);
  });
});

describe('getHistorial', () => {
  let selectMock;
  let sortMock;
  let leanMock;

  beforeEach(() => {
    jest.clearAllMocks();
    leanMock = jest.fn().mockResolvedValue([]);
    sortMock = jest.fn().mockReturnValue({ lean: leanMock });
    selectMock = jest.fn().mockReturnValue({ sort: sortMock });
    SystemMonitorSnapshot.find.mockReturnValue({ select: selectMock });
  });

  test('con fechaInicio/fechaFin explícitos: usa el día calendario completo en hora de México', async () => {
    await getHistorial({ fechaInicio: '2026-09-20', fechaFin: '2026-09-22' });

    const match = SystemMonitorSnapshot.find.mock.calls[0][0];
    expect(match.fecha.$gte).toEqual(new Date('2026-09-20T06:00:00.000Z'));
    expect(match.fecha.$lte).toEqual(new Date('2026-09-23T05:59:59.999Z'));
  });

  test('sin rango: cae a las últimas 24 horas reales', async () => {
    const ahora = new Date('2026-09-24T18:00:00.000Z').getTime();
    const spy = jest.spyOn(Date, 'now').mockReturnValue(ahora);
    try {
      await getHistorial({});
      const match = SystemMonitorSnapshot.find.mock.calls[0][0];
      expect(match.fecha.$gte).toEqual(new Date(ahora - 24 * 60 * 60 * 1000));
    } finally {
      spy.mockRestore();
    }
  });

  test('ordena cronológicamente ascendente (más viejo primero)', async () => {
    await getHistorial({ fechaInicio: '2026-09-20', fechaFin: '2026-09-20' });
    expect(sortMock).toHaveBeenCalledWith({ fecha: 1 });
  });
});

describe('guardarError', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    checkMongoOk.mockReturnValue(true);
  });

  test('guarda el error 5xx tal cual (ts/metodo/path/status)', async () => {
    const ts = new Date('2026-10-08T12:00:00.000Z');
    await guardarError({ ts, metodo: 'GET', path: '/api/boom', status: 503 });

    expect(SystemMonitorErrorLog.create).toHaveBeenCalledTimes(1);
    expect(SystemMonitorErrorLog.create).toHaveBeenCalledWith({ ts, metodo: 'GET', path: '/api/boom', status: 503 });
  });

  test('propaga el rechazo si Mongo falla (el llamador decide cómo tratarlo)', async () => {
    SystemMonitorErrorLog.create.mockRejectedValueOnce(new Error('mongo caído'));

    await expect(guardarError({ ts: new Date(), metodo: 'GET', path: '/x', status: 500 }))
      .rejects.toThrow('mongo caído');
  });

  test('Mongo ya caído (readyState != 1): omite la escritura sin intentar create()', async () => {
    checkMongoOk.mockReturnValue(false);

    await guardarError({ ts: new Date(), metodo: 'GET', path: '/x', status: 500 });

    expect(SystemMonitorErrorLog.create).not.toHaveBeenCalled();
  });
});

describe('getErroresHistorial', () => {
  let selectMock;
  let sortMock;
  let limitMock;
  let leanMock;

  beforeEach(() => {
    jest.clearAllMocks();
    leanMock = jest.fn().mockResolvedValue([]);
    limitMock = jest.fn().mockReturnValue({ lean: leanMock });
    sortMock = jest.fn().mockReturnValue({ limit: limitMock });
    selectMock = jest.fn().mockReturnValue({ sort: sortMock });
    SystemMonitorErrorLog.find.mockReturnValue({ select: selectMock });
  });

  test('con fechaInicio/fechaFin explícitos: usa el día calendario completo en hora de México', async () => {
    await getErroresHistorial({ fechaInicio: '2026-09-20', fechaFin: '2026-09-22' });

    const match = SystemMonitorErrorLog.find.mock.calls[0][0];
    expect(match.ts.$gte).toEqual(new Date('2026-09-20T06:00:00.000Z'));
    expect(match.ts.$lte).toEqual(new Date('2026-09-23T05:59:59.999Z'));
  });

  test('sin rango: cae a las últimas 24 horas reales', async () => {
    const ahora = new Date('2026-09-24T18:00:00.000Z').getTime();
    const spy = jest.spyOn(Date, 'now').mockReturnValue(ahora);
    try {
      await getErroresHistorial({});
      const match = SystemMonitorErrorLog.find.mock.calls[0][0];
      expect(match.ts.$gte).toEqual(new Date(ahora - 24 * 60 * 60 * 1000));
    } finally {
      spy.mockRestore();
    }
  });

  test('ordena desc + limita, y revierte a cronológico ascendente (más viejo primero) para el consumidor', async () => {
    leanMock.mockResolvedValue([{ ts: 2 }, { ts: 1 }]);

    const resultado = await getErroresHistorial({ fechaInicio: '2026-09-20', fechaFin: '2026-09-20' });

    expect(sortMock).toHaveBeenCalledWith({ ts: -1 });
    expect(limitMock).toHaveBeenCalledWith(2000);
    expect(resultado).toEqual([{ ts: 1 }, { ts: 2 }]);
  });
});
