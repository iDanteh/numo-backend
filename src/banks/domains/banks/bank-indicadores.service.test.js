'use strict';

// bank-indicadores.service.test.js — cubre solo el mapeo/agregación de
// getIndicadoresIdentificacion(): BankMovement.aggregate() se mockea completo (3
// llamadas en paralelo, mismo orden que el código: tiempo / backlog / porUsuario),
// no se prueba contra Mongo real. El gate de permiso + paso de query params vive en
// bank.routes.test.js.
jest.mock('./BankMovement.model');

const BankMovement = require('./BankMovement.model');
const { MOVEMENT_SCOPE } = require('../../../shared/config/rbac');
const { getIndicadoresIdentificacion } = require('./bank-indicadores.service');

beforeEach(() => {
  jest.clearAllMocks();
});

// Encadena los 3 resultados de aggregate() en el orden fijo que usa el service:
// [tiempoAgg, backlogAgg, porUsuarioAgg].
function mockAggregateSequence(tiempoAgg, backlogAgg, porUsuarioAgg) {
  BankMovement.aggregate
    .mockResolvedValueOnce(tiempoAgg)
    .mockResolvedValueOnce(backlogAgg)
    .mockResolvedValueOnce(porUsuarioAgg);
}

describe('getIndicadoresIdentificacion — mapeo de buckets del backlog (histórico/nuevo vía $facet)', () => {
  test('mapea los 4 buckets de "historico" cuando todos tienen documentos', async () => {
    mockAggregateSequence(
      [{ _id: null, promedioHoras: 10, n: 5 }],
      [{
        historico: [
          { _id: 0,   count: 2 },
          { _id: 24,  count: 3 },
          { _id: 72,  count: 1 },
          { _id: 168, count: 4 },
        ],
        nuevo: [],
      }],
      [],
    );

    const result = await getIndicadoresIdentificacion({});

    expect(result.backlog.historico).toEqual({ menos24h: 2, de1a3d: 3, de3a7d: 1, mas7d: 4 });
    expect(result.backlog.nuevo).toEqual({ menos24h: 0, de1a3d: 0, de3a7d: 0, mas7d: 0 });
  });

  test('mapea los 4 buckets de "nuevo" cuando todos tienen documentos, sin tocar "historico"', async () => {
    mockAggregateSequence(
      [{ _id: null, promedioHoras: 10, n: 5 }],
      [{
        historico: [],
        nuevo: [
          { _id: 0,   count: 1 },
          { _id: 24,  count: 2 },
          { _id: 72,  count: 3 },
          { _id: 168, count: 4 },
        ],
      }],
      [],
    );

    const result = await getIndicadoresIdentificacion({});

    expect(result.backlog.nuevo).toEqual({ menos24h: 1, de1a3d: 2, de3a7d: 3, mas7d: 4 });
    expect(result.backlog.historico).toEqual({ menos24h: 0, de1a3d: 0, de3a7d: 0, mas7d: 0 });
  });

  test('default-ea a 0 los buckets sin documentos en cada grupo (Mongo omite las llaves vacías)', async () => {
    mockAggregateSequence(
      [{ _id: null, promedioHoras: 10, n: 5 }],
      [{
        historico: [{ _id: 24, count: 3 }], // solo "de1a3d" tiene documentos
        nuevo:     [{ _id: 168, count: 1 }], // solo "mas7d" tiene documentos
      }],
      [],
    );

    const result = await getIndicadoresIdentificacion({});

    expect(result.backlog.historico).toEqual({ menos24h: 0, de1a3d: 3, de3a7d: 0, mas7d: 0 });
    expect(result.backlog.nuevo).toEqual({ menos24h: 0, de1a3d: 0, de3a7d: 0, mas7d: 1 });
  });

  test('backlog vacío (sin no_identificados): las 4 llaves en 0 en ambos grupos', async () => {
    mockAggregateSequence(
      [{ _id: null, promedioHoras: 10, n: 5 }],
      [{ historico: [], nuevo: [] }],
      [],
    );

    const result = await getIndicadoresIdentificacion({});

    expect(result.backlog.historico).toEqual({ menos24h: 0, de1a3d: 0, de3a7d: 0, mas7d: 0 });
    expect(result.backlog.nuevo).toEqual({ menos24h: 0, de1a3d: 0, de3a7d: 0, mas7d: 0 });
  });

  test('$facet vacío (backlogAgg[0] undefined): ambos grupos en 0 sin lanzar', async () => {
    mockAggregateSequence(
      [{ _id: null, promedioHoras: 10, n: 5 }],
      [], // aggregate() con $facet siempre debería devolver 1 doc, pero cubrimos el edge case
      [],
    );

    const result = await getIndicadoresIdentificacion({});

    expect(result.backlog.historico).toEqual({ menos24h: 0, de1a3d: 0, de3a7d: 0, mas7d: 0 });
    expect(result.backlog.nuevo).toEqual({ menos24h: 0, de1a3d: 0, de3a7d: 0, mas7d: 0 });
  });

  test('el $match del pipeline de backlog filtra status no_identificado + reclasificado (identificados y otros nunca entran)', async () => {
    mockAggregateSequence([], [{ historico: [], nuevo: [] }], []);

    await getIndicadoresIdentificacion({});

    const [, backlogCall] = BankMovement.aggregate.mock.calls;
    expect(backlogCall[0][0].$match.status).toEqual({ $in: ['no_identificado', 'reclasificado'] });
  });

  test('los 3 pipelines excluyen retiros (deposito>0) y movimientos ocultos, igual que getCards()', async () => {
    mockAggregateSequence([], [{ historico: [], nuevo: [] }], []);

    await getIndicadoresIdentificacion({});

    for (const call of BankMovement.aggregate.mock.calls) {
      expect(call[0][0].$match.deposito).toEqual({ $gt: 0 });
      expect(call[0][0].$match.oculto).toEqual({ $ne: true });
    }
  });
});

describe('getIndicadoresIdentificacion — defaults cuando las agregaciones vienen vacías', () => {
  test('tiempoAgg vacío: promedioHoras null, totalIdentificadosConDato 0', async () => {
    mockAggregateSequence([], [], []);

    const result = await getIndicadoresIdentificacion({});

    expect(result.promedioHoras).toBeNull();
    expect(result.totalIdentificadosConDato).toBe(0);
  });

  test('porUsuarioAgg vacío: porUsuario = []', async () => {
    mockAggregateSequence([{ _id: null, promedioHoras: 8, n: 2 }], [], []);

    const result = await getIndicadoresIdentificacion({});

    expect(result.porUsuario).toEqual([]);
  });

  test('porUsuarioAgg con datos: mapea userId/nombre/promedioHoras/count', async () => {
    mockAggregateSequence(
      [{ _id: null, promedioHoras: 8, n: 2 }],
      [],
      [{ _id: 'user-1', nombre: 'Ana', promedioHoras: 6.2, count: 2 }],
    );

    const result = await getIndicadoresIdentificacion({});

    expect(result.porUsuario).toEqual([
      { userId: 'user-1', nombre: 'Ana', promedioHoras: 6.2, count: 2 },
    ]);
  });
});

describe('getIndicadoresIdentificacion — filtros y restrictions', () => {
  test('sin restrictions: el $match de porUsuario NO se acota por userId', async () => {
    mockAggregateSequence([], [], []);

    await getIndicadoresIdentificacion({ banco: 'BBVA', categoria: 'Renta', year: '2026', month: '8' });

    const [tiempoCall, backlogCall, porUsuarioCall] = BankMovement.aggregate.mock.calls;
    expect(tiempoCall[0][0].$match.banco).toBe('BBVA');
    expect(tiempoCall[0][0].$match.categoria).toBe('Renta');
    expect(tiempoCall[0][0].$match.fecha).toEqual({
      $gte: new Date(2026, 7, 1),
      $lt:  new Date(2026, 8, 1),
    });
    // backlog: banco/categoria sí, pero SIN year/month (antigüedad se mide contra ahora).
    expect(backlogCall[0][0].$match.banco).toBe('BBVA');
    expect(backlogCall[0][0].$match.fecha).toBeUndefined();
    expect(porUsuarioCall[0][0].$match['primeraIdentificacionPor.userId']).toBeUndefined();
  });

  test('restrictions.scope OWN: el $match de porUsuario SÍ se acota a userId; tiempo/backlog no', async () => {
    mockAggregateSequence([], [], []);

    await getIndicadoresIdentificacion({ restrictions: { scope: MOVEMENT_SCOPE.OWN, userId: 'user-1' } });

    const [tiempoCall, backlogCall, porUsuarioCall] = BankMovement.aggregate.mock.calls;
    expect(porUsuarioCall[0][0].$match['primeraIdentificacionPor.userId']).toBe('user-1');
    expect(tiempoCall[0][0].$match['primeraIdentificacionPor.userId']).toBeUndefined();
    expect(backlogCall[0][0].$match['primeraIdentificacionPor.userId']).toBeUndefined();
  });

  test('restrictions.scope ALL: ningún pipeline se acota por userId (equipo completo)', async () => {
    mockAggregateSequence([], [], []);

    await getIndicadoresIdentificacion({ restrictions: { scope: MOVEMENT_SCOPE.ALL, userId: 'user-1' } });

    const [, , porUsuarioCall] = BankMovement.aggregate.mock.calls;
    expect(porUsuarioCall[0][0].$match['primeraIdentificacionPor.userId']).toBeUndefined();
  });

  test('year sin month: rango cubre el año completo', async () => {
    mockAggregateSequence([], [], []);

    await getIndicadoresIdentificacion({ year: '2026' });

    const [tiempoCall] = BankMovement.aggregate.mock.calls;
    expect(tiempoCall[0][0].$match.fecha).toEqual({
      $gte: new Date(2026, 0, 1),
      $lt:  new Date(2027, 0, 1),
    });
  });

  test('sin year: no se agrega filtro de fecha', async () => {
    mockAggregateSequence([], [], []);

    await getIndicadoresIdentificacion({});

    const [tiempoCall] = BankMovement.aggregate.mock.calls;
    expect(tiempoCall[0][0].$match.fecha).toBeUndefined();
  });
});
