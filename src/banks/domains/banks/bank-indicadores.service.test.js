'use strict';

// bank-indicadores.service.test.js — cubre getIndicadoresIdentificacion() y la función
// pura horasHabilesEntre(). Desde 2026-08-17, "tiempo" y "porUsuario" ya NO usan
// BankMovement.aggregate() — traen los documentos con find().select().lean() y calculan
// horas hábiles en JS (ver comentario en el service sobre por qué). El backlog (Pipeline 2)
// sigue siendo la única llamada a aggregate(). El gate de permiso + paso de query params
// vive en bank.routes.test.js.
jest.mock('./BankMovement.model');

const BankMovement = require('./BankMovement.model');
const { getIndicadoresIdentificacion, horasHabilesEntre } = require('./bank-indicadores.service');

beforeEach(() => {
  jest.clearAllMocks();
});

// Mockea la cadena find().select().lean() — devuelve `docs` tal cual, cada uno
// {createdAt, primeraIdentificacionAt, primeraIdentificacionPor?: {userId, nombre}}.
function mockIdentificados(docs) {
  const lean = jest.fn().mockResolvedValue(docs);
  const select = jest.fn().mockReturnValue({ lean });
  BankMovement.find.mockReturnValue({ select });
  return { select, lean };
}

function mockBacklog(backlogAgg) {
  BankMovement.aggregate.mockResolvedValueOnce(backlogAgg);
}

describe('horasHabilesEntre — 8:00-20:00 lunes a sábado, domingo 0', () => {
  test('mismo día, dentro de la ventana', () => {
    const h = horasHabilesEntre(new Date(2026, 7, 17, 10, 0), new Date(2026, 7, 17, 14, 0));
    expect(h).toBe(4);
  });

  test('antes de las 8:00 se clampea al inicio de la ventana', () => {
    const h = horasHabilesEntre(new Date(2026, 7, 17, 6, 0), new Date(2026, 7, 17, 9, 0));
    expect(h).toBe(1); // 8:00-9:00, no 6:00-9:00
  });

  test('después de las 20:00 se clampea al fin de la ventana', () => {
    const h = horasHabilesEntre(new Date(2026, 7, 17, 19, 0), new Date(2026, 7, 17, 22, 0));
    expect(h).toBe(1); // 19:00-20:00, no 19:00-22:00
  });

  test('cruza un sábado completo (2026-08-22): suma sus 12h', () => {
    // Viernes 21 20:00 (fin de ventana, aporta 0) → domingo 23 00:00 (aporta 0) — solo
    // queda el sábado completo en el medio.
    const h = horasHabilesEntre(new Date(2026, 7, 21, 20, 0), new Date(2026, 7, 23, 0, 0));
    expect(h).toBe(12);
  });

  test('domingo completo (2026-08-23) no suma nada dentro de un tramo mixto', () => {
    // Sábado 8:00 (12h) + domingo (0h) + lunes 8:00-20:00 (12h) = 24h exactas —
    // si el domingo sumara algo, el total no daría un número redondo de 24.
    const h = horasHabilesEntre(new Date(2026, 7, 22, 8, 0), new Date(2026, 7, 24, 20, 0));
    expect(h).toBe(24);
  });

  test('viernes 19:00 → lunes 10:00: 1h viernes + 12h sábado + 0h domingo + 2h lunes = 15h', () => {
    const h = horasHabilesEntre(new Date(2026, 7, 21, 19, 0), new Date(2026, 7, 24, 10, 0));
    expect(h).toBe(15);
  });

  test('span de más de una semana: lunes 8:00 al lunes siguiente 8:00 = 6 días hábiles completos (72h)', () => {
    // 17(lun) 18(mar) 19(mié) 20(jue) 21(vie) 22(sáb) = 6 días × 12h; 23(dom) = 0h;
    // 24(lun) aporta 0 porque el tramo termina justo a las 8:00, sin adelantarse a la ventana.
    const h = horasHabilesEntre(new Date(2026, 7, 17, 8, 0), new Date(2026, 7, 24, 8, 0));
    expect(h).toBe(72);
  });

  test('borde exacto: un día completo 8:00-20:00 da exactamente 12h, sin off-by-one', () => {
    const h = horasHabilesEntre(new Date(2026, 7, 17, 8, 0), new Date(2026, 7, 17, 20, 0));
    expect(h).toBe(12);
  });

  test('fin <= inicio devuelve 0 (guard, no lanza)', () => {
    const mismoInstante = new Date(2026, 7, 17, 10, 0);
    expect(horasHabilesEntre(mismoInstante, mismoInstante)).toBe(0);
    expect(horasHabilesEntre(new Date(2026, 7, 17, 12, 0), new Date(2026, 7, 17, 10, 0))).toBe(0);
  });
});

describe('getIndicadoresIdentificacion — promedio y mediana en horas hábiles', () => {
  test('un movimiento identificado durante un fin de semana usa horas hábiles, no de reloj', async () => {
    // Viernes 21 19:00 → lunes 24 10:00 = 15h hábiles (ver test de horasHabilesEntre) —
    // en tiempo de reloj serían ~63h. Si el service todavía calculara en reloj, este test fallaría.
    mockIdentificados([
      { createdAt: new Date(2026, 7, 21, 19, 0), primeraIdentificacionAt: new Date(2026, 7, 24, 10, 0) },
    ]);
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({});

    expect(result.promedioHoras).toBe(15);
    expect(result.medianaHoras).toBe(15);
    expect(result.totalIdentificadosConDato).toBe(1);
  });

  test('la mediana resiste un outlier que sí infla el promedio', async () => {
    // 4 movimientos de 2h hábiles + 1 de 72h hábiles (lunes a lunes, ver test de arriba).
    const rapido = () => ({ createdAt: new Date(2026, 7, 17, 8, 0), primeraIdentificacionAt: new Date(2026, 7, 17, 10, 0) }); // 2h
    const lento  = { createdAt: new Date(2026, 7, 17, 8, 0), primeraIdentificacionAt: new Date(2026, 7, 24, 8, 0) }; // 72h
    mockIdentificados([rapido(), rapido(), rapido(), rapido(), lento]);
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({});

    expect(result.promedioHoras).toBe(16);  // (2+2+2+2+72)/5
    expect(result.medianaHoras).toBe(2);    // valor central de [2,2,2,2,72]
    expect(result.promedioHoras).not.toBe(result.medianaHoras);
  });

  test('sin identificados: promedioHoras/medianaHoras null, totalIdentificadosConDato 0', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({});

    expect(result.promedioHoras).toBeNull();
    expect(result.medianaHoras).toBeNull();
    expect(result.totalIdentificadosConDato).toBe(0);
  });
});

describe('getIndicadoresIdentificacion — porUsuario (agrupado en JS, misma definición de horas)', () => {
  test('agrupa por userId, cuenta y promedia en horas hábiles; ordena por count desc', async () => {
    mockIdentificados([
      { createdAt: new Date(2026, 7, 17, 8, 0), primeraIdentificacionAt: new Date(2026, 7, 17, 10, 0), primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' } }, // 2h
      { createdAt: new Date(2026, 7, 17, 8, 0), primeraIdentificacionAt: new Date(2026, 7, 17, 12, 0), primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' } }, // 4h
      { createdAt: new Date(2026, 7, 17, 8, 0), primeraIdentificacionAt: new Date(2026, 7, 17, 9, 0),  primeraIdentificacionPor: { userId: 'user-2', nombre: 'Luis' } }, // 1h
    ]);
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({});

    expect(result.porUsuario).toEqual([
      { userId: 'user-1', nombre: 'Ana', promedioHoras: 3, count: 2 }, // (2+4)/2
      { userId: 'user-2', nombre: 'Luis', promedioHoras: 1, count: 1 },
    ]);
  });

  test('sin identificados: porUsuario = []', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({});

    expect(result.porUsuario).toEqual([]);
  });
});

describe('getIndicadoresIdentificacion — mapeo de buckets del backlog (un solo $bucket, sin historico/nuevo, tiempo de reloj)', () => {
  test('mapea los 4 buckets cuando todos tienen documentos', async () => {
    mockIdentificados([]);
    mockBacklog([
      { _id: 0,   count: 2 },
      { _id: 24,  count: 3 },
      { _id: 72,  count: 1 },
      { _id: 168, count: 4 },
    ]);

    const result = await getIndicadoresIdentificacion({});

    expect(result.backlog).toEqual({ menos24h: 2, de1a3d: 3, de3a7d: 1, mas7d: 4 });
  });

  test('default-ea a 0 los buckets sin documentos (Mongo omite las llaves vacías)', async () => {
    mockIdentificados([]);
    mockBacklog([{ _id: 24, count: 3 }]); // solo "de1a3d" tiene documentos

    const result = await getIndicadoresIdentificacion({});

    expect(result.backlog).toEqual({ menos24h: 0, de1a3d: 3, de3a7d: 0, mas7d: 0 });
  });

  test('backlog vacío (sin no_identificados): las 4 llaves en 0', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({});

    expect(result.backlog).toEqual({ menos24h: 0, de1a3d: 0, de3a7d: 0, mas7d: 0 });
  });

  test('el $match del backlog filtra status no_identificado + reclasificado (identificados y otros nunca entran)', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({});

    const backlogCall = BankMovement.aggregate.mock.calls[0];
    expect(backlogCall[0][0].$match.status).toEqual({ $in: ['no_identificado', 'reclasificado'] });
  });

  test('backlog sigue en tiempo de RELOJ (no horas hábiles) — a propósito, decisión de alcance', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({});

    const backlogCall = BankMovement.aggregate.mock.calls[0];
    // El $project del backlog sigue restando contra $$NOW directo, sin pasar por
    // horasHabilesEntre() — confirma que no se coló el cambio de horas hábiles acá.
    expect(backlogCall[0][1].$project.horas).toEqual({
      $divide: [{ $subtract: ['$$NOW', '$createdAt'] }, 3600000],
    });
  });
});

describe('getIndicadoresIdentificacion — fecha de corte INDICADORES_DESDE (2026-08-17)', () => {
  test('find() (tiempo/porUsuario) y aggregate() (backlog) filtran createdAt >= INDICADORES_DESDE', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({});

    const cutoff = new Date(2026, 7, 17);
    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch.createdAt).toEqual({ $gte: cutoff });

    const backlogCall = BankMovement.aggregate.mock.calls[0];
    expect(backlogCall[0][0].$match.createdAt).toEqual({ $gte: cutoff });
  });
});

describe('getIndicadoresIdentificacion — filtros', () => {
  test('banco/categoria/year/month: find() (tiempo/porUsuario) se acota por fecha, backlog no', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({ banco: 'BBVA', categoria: 'Renta', year: '2026', month: '8' });

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch.banco).toBe('BBVA');
    expect(findMatch.categoria).toBe('Renta');
    expect(findMatch.fecha).toEqual({
      $gte: new Date(2026, 7, 1),
      $lt:  new Date(2026, 8, 1),
    });

    // backlog: banco/categoria sí, pero SIN year/month (antigüedad se mide contra ahora).
    const backlogCall = BankMovement.aggregate.mock.calls[0];
    expect(backlogCall[0][0].$match.banco).toBe('BBVA');
    expect(backlogCall[0][0].$match.fecha).toBeUndefined();
  });

  test('year sin month: rango cubre el año completo', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({ year: '2026' });

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch.fecha).toEqual({
      $gte: new Date(2026, 0, 1),
      $lt:  new Date(2027, 0, 1),
    });
  });

  test('sin year: no se agrega filtro de fecha (aparte del cutoff createdAt)', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({});

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch.fecha).toBeUndefined();
  });

  test('find() (tiempo/porUsuario) y backlog excluyen retiros (deposito>0) y movimientos ocultos, igual que getCards()', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({});

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch.deposito).toEqual({ $gt: 0 });
    expect(findMatch.oculto).toEqual({ $ne: true });

    const backlogCall = BankMovement.aggregate.mock.calls[0];
    expect(backlogCall[0][0].$match.deposito).toEqual({ $gt: 0 });
    expect(backlogCall[0][0].$match.oculto).toEqual({ $ne: true });
  });
});
