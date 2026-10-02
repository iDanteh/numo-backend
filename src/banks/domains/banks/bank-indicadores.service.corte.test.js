'use strict';

// bank-indicadores.service.corte.test.js — getCorteConciliacion() (2026-10-02, control
// periódico de conciliación: rezagados / nuevos del periodo / identificados en el periodo).
// Mismas dependencias mockeadas que bank-indicadores.service.test.js (requiere bank.service.js
// para reusar _rangoAnioMesMexico/_inicioDiaMx/_finDiaMx).
jest.mock('./BankMovement.model');
jest.mock('../../shared/socket');
jest.mock('./drive-fichas.service');

const BankMovement = require('./BankMovement.model');
const { getCorteConciliacion } = require('./bank-indicadores.service');

// "Ahora" fijo para que los tests sean deterministas: miércoles 2026-10-07, 15:00 México
// (21:00 UTC). Semana en curso → lunes 2026-10-05 00:00 México. Mes en curso → 2026-10-01
// 00:00 México.
const AHORA_MX = new Date(Date.UTC(2026, 9, 7, 21, 0, 0));
const INICIO_SEMANA_ESPERADO = new Date(Date.UTC(2026, 9, 5, 6, 0, 0));
const INICIO_MES_ESPERADO    = new Date(Date.UTC(2026, 9, 1, 6, 0, 0));

beforeEach(() => {
  jest.clearAllMocks();
  jest.useFakeTimers().setSystemTime(AHORA_MX);
});

afterEach(() => {
  jest.useRealTimers();
});

function mockAggregates({ rezagados = [], nuevos = [], identificados = [] }) {
  BankMovement.aggregate
    .mockResolvedValueOnce(rezagados)
    .mockResolvedValueOnce(nuevos)
    .mockResolvedValueOnce(identificados);
}

describe('getCorteConciliacion', () => {
  test('periodo semanal (default): usa el lunes de la semana en curso como inicio', async () => {
    mockAggregates({});
    const result = await getCorteConciliacion({});

    expect(result.periodo).toBe('semanal');
    expect(result.inicio).toEqual(INICIO_SEMANA_ESPERADO);

    const [rezagadosCall, nuevosCall] = BankMovement.aggregate.mock.calls;
    expect(rezagadosCall[0][0].$match.fecha).toEqual({ $lt: INICIO_SEMANA_ESPERADO });
    expect(nuevosCall[0][0].$match.fecha).toEqual({ $gte: INICIO_SEMANA_ESPERADO });
  });

  test('periodo mensual: usa el día 1 del mes en curso como inicio', async () => {
    mockAggregates({});
    const result = await getCorteConciliacion({ periodo: 'mensual' });

    expect(result.periodo).toBe('mensual');
    expect(result.inicio).toEqual(INICIO_MES_ESPERADO);
  });

  test('rezagados: solo no_identificado/reclasificado con fecha anterior al periodo, con total', async () => {
    mockAggregates({
      rezagados: [
        { _id: 'no_identificado', count: 12 },
        { _id: 'reclasificado',    count: 3  },
      ],
    });
    const { rezagados } = await getCorteConciliacion({ periodo: 'semanal' });

    expect(rezagados).toEqual({ no_identificado: 12, reclasificado: 3, total: 15 });
  });

  test('rezagados en cero (sin backlog) no explota — defaults a 0', async () => {
    mockAggregates({ rezagados: [] });
    const { rezagados } = await getCorteConciliacion({});
    expect(rezagados).toEqual({ no_identificado: 0, reclasificado: 0, total: 0 });
  });

  test('nuevos: desglosa por estatus del periodo, incluye pendientes/otros/total', async () => {
    mockAggregates({
      nuevos: [
        { _id: 'no_identificado', count: 20 },
        { _id: 'reclasificado',    count: 2  },
        { _id: 'identificado',     count: 30 },
        { _id: 'otros',            count: 1  },
      ],
    });
    const { nuevos } = await getCorteConciliacion({});

    expect(nuevos).toEqual({
      no_identificado: 20,
      reclasificado:   2,
      identificado:    30,
      otros:           1,
      pendientes:      22, // no_identificado + reclasificado
      total:           53,
    });
  });

  test('identificadosEnPeriodo: separa origen rezagado vs nuevo, con total', async () => {
    mockAggregates({
      identificados: [
        { _id: 'rezagado', count: 5 },
        { _id: 'nuevo',    count: 30 },
      ],
    });
    const { identificadosEnPeriodo } = await getCorteConciliacion({});

    expect(identificadosEnPeriodo).toEqual({ deRezagados: 5, deNuevos: 30, total: 35 });
  });

  test('banco: se pasa al $match de las 3 agregaciones', async () => {
    mockAggregates({});
    await getCorteConciliacion({ banco: 'BBVA' });

    for (const call of BankMovement.aggregate.mock.calls) {
      expect(call[0][0].$match.banco).toBe('BBVA');
      expect(call[0][0].$match.deposito).toEqual({ $gt: 0 }); // buildBaseMatch — solo depósitos
    }
  });
});
