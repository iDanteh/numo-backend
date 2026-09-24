'use strict';

jest.mock('./SystemMonitorSnapshot.model');

const SystemMonitorSnapshot = require('./SystemMonitorSnapshot.model');
const { guardarSnapshot, getHistorial } = require('./system-monitor-historial.service');

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
