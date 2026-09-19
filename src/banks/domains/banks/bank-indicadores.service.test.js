'use strict';

// bank-indicadores.service.test.js — cubre getIndicadoresIdentificacion() y la función
// pura horasHabilesEntre(). Desde 2026-08-17, "tiempo" y "porUsuario" ya NO usan
// BankMovement.aggregate() — traen los documentos con find().select().lean() y calculan
// horas hábiles en JS (ver comentario en el service sobre por qué). El backlog (Pipeline 2)
// sigue siendo la única llamada a aggregate(). El gate de permiso + paso de query params
// vive en bank.routes.test.js.
//
// bank-indicadores.service.js requiere bank.service.js (2026-09-09, para reusar
// _rangoAnioMesMexico) — se mockean las mismas dependencias con I/O que ese módulo necesita
// al cargarse (ver bank.service.ficha.test.js), aunque acá no se usen directamente.
jest.mock('./BankMovement.model');
jest.mock('../../shared/socket');
jest.mock('./drive-fichas.service');

const ExcelJS = require('exceljs');
const BankMovement = require('./BankMovement.model');
const {
  getIndicadoresIdentificacion, buildReporteIdentificacion, listUsuariosConIdentificaciones, horasHabilesEntre,
} = require('./bank-indicadores.service');

// Todos los tests de este archivo construyen instantes como "hora de PARED en México" — mx(y,
// mesIndex0, d, h, mi) devuelve el instante UTC real correspondiente, usando el offset fijo
// -06:00 (México no tiene horario de verano desde 2022). Esto es a propósito, NO cosmético:
// horasHabilesEntre()/_rangoAnioMesMexico() se blindaron el 2026-09-09 contra el TZ del proceso
// (el contenedor de producción corre en UTC) — si estos tests siguieran construyendo con
// `new Date(y, m, d, h)` (hora LOCAL DEL PROCESO que corre los tests), pasarían en esta máquina
// (que da la casualidad de estar en America/Mexico_City) pero mentirían: confirmado que 13/24
// tests de este archivo fallaban corriendo con `TZ=UTC npx jest ...` antes de este cambio.
function mx(year, monthIndex0, day, hour = 0, minute = 0) {
  return new Date(Date.UTC(year, monthIndex0, day, hour + 6, minute));
}

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

describe('horasHabilesEntre — 8:00-20:00 lunes a sábado, domingo 0 (hora de México)', () => {
  test('mismo día, dentro de la ventana', () => {
    const h = horasHabilesEntre(mx(2026, 7, 17, 10, 0), mx(2026, 7, 17, 14, 0));
    expect(h).toBe(4);
  });

  test('antes de las 8:00 se clampea al inicio de la ventana', () => {
    const h = horasHabilesEntre(mx(2026, 7, 17, 6, 0), mx(2026, 7, 17, 9, 0));
    expect(h).toBe(1); // 8:00-9:00, no 6:00-9:00
  });

  test('después de las 20:00 se clampea al fin de la ventana', () => {
    const h = horasHabilesEntre(mx(2026, 7, 17, 19, 0), mx(2026, 7, 17, 22, 0));
    expect(h).toBe(1); // 19:00-20:00, no 19:00-22:00
  });

  test('cruza un sábado completo (2026-08-22): suma sus 12h', () => {
    // Viernes 21 20:00 (fin de ventana, aporta 0) → domingo 23 00:00 (aporta 0) — solo
    // queda el sábado completo en el medio.
    const h = horasHabilesEntre(mx(2026, 7, 21, 20, 0), mx(2026, 7, 23, 0, 0));
    expect(h).toBe(12);
  });

  test('domingo completo (2026-08-23) no suma nada dentro de un tramo mixto', () => {
    // Sábado 8:00 (12h) + domingo (0h) + lunes 8:00-20:00 (12h) = 24h exactas —
    // si el domingo sumara algo, el total no daría un número redondo de 24.
    const h = horasHabilesEntre(mx(2026, 7, 22, 8, 0), mx(2026, 7, 24, 20, 0));
    expect(h).toBe(24);
  });

  test('viernes 19:00 → lunes 10:00: 1h viernes + 12h sábado + 0h domingo + 2h lunes = 15h', () => {
    const h = horasHabilesEntre(mx(2026, 7, 21, 19, 0), mx(2026, 7, 24, 10, 0));
    expect(h).toBe(15);
  });

  test('span de más de una semana: lunes 8:00 al lunes siguiente 8:00 = 6 días hábiles completos (72h)', () => {
    // 17(lun) 18(mar) 19(mié) 20(jue) 21(vie) 22(sáb) = 6 días × 12h; 23(dom) = 0h;
    // 24(lun) aporta 0 porque el tramo termina justo a las 8:00, sin adelantarse a la ventana.
    const h = horasHabilesEntre(mx(2026, 7, 17, 8, 0), mx(2026, 7, 24, 8, 0));
    expect(h).toBe(72);
  });

  test('borde exacto: un día completo 8:00-20:00 da exactamente 12h, sin off-by-one', () => {
    const h = horasHabilesEntre(mx(2026, 7, 17, 8, 0), mx(2026, 7, 17, 20, 0));
    expect(h).toBe(12);
  });

  test('fin <= inicio devuelve 0 (guard, no lanza)', () => {
    const mismoInstante = mx(2026, 7, 17, 10, 0);
    expect(horasHabilesEntre(mismoInstante, mismoInstante)).toBe(0);
    expect(horasHabilesEntre(mx(2026, 7, 17, 12, 0), mx(2026, 7, 17, 10, 0))).toBe(0);
  });

  // Caso real que el fix del 2026-09-09 blinda: bajo TZ del PROCESO = UTC (el contenedor de
  // producción), 2pm México cae en el mismo día calendario UTC (20:00Z) pero 11pm México ya
  // cruzó a las 05:00Z del día UTC siguiente — sin _comoRelojMexico(), el cálculo local hubiera
  // usado el día/hora de UTC en vez del de México, corriendo la ventana laboral 6h.
  test('inmune al TZ del proceso: mismo resultado corriendo bajo process.env.TZ = "UTC"', () => {
    const originalTz = process.env.TZ;
    try {
      process.env.TZ = 'UTC';
      // 2pm México (14:00) → 20:00Z el mismo día calendario UTC.
      const dosDeLaTarde = new Date('2026-08-17T20:00:00Z');
      // 11pm México (23:00) del mismo día → 05:00Z del día calendario UTC SIGUIENTE.
      const onceDeLaNoche = new Date('2026-08-18T05:00:00Z');
      const h = horasHabilesEntre(dosDeLaTarde, onceDeLaNoche);
      // 14:00→20:00 (fin de ventana) = 6h hábiles ese mismo día México; nada más suma.
      expect(h).toBe(6);
    } finally {
      process.env.TZ = originalTz;
    }
  });
});

describe('getIndicadoresIdentificacion — promedio y mediana en horas hábiles', () => {
  test('un movimiento identificado durante un fin de semana usa horas hábiles, no de reloj', async () => {
    // Viernes 21 19:00 → lunes 24 10:00 = 15h hábiles (ver test de horasHabilesEntre) —
    // en tiempo de reloj serían ~63h. Si el service todavía calculara en reloj, este test fallaría.
    mockIdentificados([
      { createdAt: mx(2026, 7, 21, 19, 0), primeraIdentificacionAt: mx(2026, 7, 24, 10, 0) },
    ]);
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({});

    expect(result.promedioHoras).toBe(15);
    expect(result.medianaHoras).toBe(15);
    expect(result.totalIdentificadosConDato).toBe(1);
  });

  test('la mediana resiste un outlier que sí infla el promedio', async () => {
    // 4 movimientos de 2h hábiles + 1 de 72h hábiles (lunes a lunes, ver test de arriba).
    const rapido = () => ({ createdAt: mx(2026, 7, 17, 8, 0), primeraIdentificacionAt: mx(2026, 7, 17, 10, 0) }); // 2h
    const lento  = { createdAt: mx(2026, 7, 17, 8, 0), primeraIdentificacionAt: mx(2026, 7, 24, 8, 0) }; // 72h
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
      { createdAt: mx(2026, 7, 17, 8, 0), primeraIdentificacionAt: mx(2026, 7, 17, 10, 0), primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' } }, // 2h
      { createdAt: mx(2026, 7, 17, 8, 0), primeraIdentificacionAt: mx(2026, 7, 17, 12, 0), primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' } }, // 4h
      { createdAt: mx(2026, 7, 17, 8, 0), primeraIdentificacionAt: mx(2026, 7, 17, 9, 0),  primeraIdentificacionPor: { userId: 'user-2', nombre: 'Luis' } }, // 1h
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

    const cutoff = mx(2026, 7, 17, 0, 0);
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
      $gte: mx(2026, 7, 1, 0, 0),
      $lt:  mx(2026, 8, 1, 0, 0),
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
      $gte: mx(2026, 0, 1, 0, 0),
      $lt:  mx(2027, 0, 1, 0, 0),
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

describe('getIndicadoresIdentificacion — default "hoy" sin year/month (2026-09-18, dashboard de Cobranza)', () => {
  // Congela "ahora" en un instante conocido de MÉXICO (usando el mismo helper mx() que ya
  // arma el resto del archivo) para poder predecir exactamente los boundaries de
  // _inicioDiaMx/_finDiaMx sin acoplar el test a la fecha real de ejecución.
  const HOY_MX = mx(2026, 8, 18, 15, 30); // 2026-09-18 15:30 hora de México

  beforeEach(() => {
    jest.useFakeTimers({ doNotFake: ['nextTick'] });
    jest.setSystemTime(HOY_MX);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('sin year: primeraIdentificacionAt se acota a HOY en México (inicio/fin del día)', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({});

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch.primeraIdentificacionAt).toEqual({
      $ne:  null,
      $gte: mx(2026, 8, 18, 0, 0),                                   // 2026-09-18 00:00 MX
      $lte: new Date(mx(2026, 8, 19, 0, 0).getTime() - 1),           // 2026-09-18 23:59:59.999 MX
    });
  });

  test('con year: primeraIdentificacionAt NO se acota por día, sigue siendo solo $ne:null (comportamiento sin cambios)', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({ year: '2026', month: '8' });

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch.primeraIdentificacionAt).toEqual({ $ne: null });
  });

  test('el BACKLOG no se ve afectado por el default "hoy" — sigue siendo de todos los pendientes actuales', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({});

    const backlogCall = BankMovement.aggregate.mock.calls[0];
    expect(backlogCall[0][0].$match.createdAt).toEqual({ $gte: mx(2026, 7, 17, 0, 0) });
    expect('primeraIdentificacionAt' in backlogCall[0][0].$match).toBe(false);
  });
});

describe('getIndicadoresIdentificacion — fechaInicio/fechaFin explícito (2026-09-18, rango de días del dashboard de Cobranza)', () => {
  test('rango explícito acota primeraIdentificacionAt a esos días (inicio/fin en México)', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({ fechaInicio: '2026-09-10', fechaFin: '2026-09-12' });

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch.primeraIdentificacionAt).toEqual({
      $ne:  null,
      $gte: mx(2026, 8, 10, 0, 0),                                    // 2026-09-10 00:00 MX
      $lte: new Date(mx(2026, 8, 13, 0, 0).getTime() - 1),            // 2026-09-12 23:59:59.999 MX
    });
  });

  test('rango explícito gana sobre year/month: ignora fecha (transacción) y no aplica el bucket de year', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({
      year: '2025', month: '3', fechaInicio: '2026-09-10', fechaFin: '2026-09-10',
    });

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch.fecha).toBeUndefined();
    expect(findMatch.primeraIdentificacionAt.$gte).toEqual(mx(2026, 8, 10, 0, 0));
  });

  test('solo fechaInicio sin fechaFin (o viceversa): se ignoran los dos, cae al default de siempre', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({ fechaInicio: '2026-09-10' });

    const findMatch = BankMovement.find.mock.calls[0][0];
    // Sin year y sin rango completo -> default "hoy", no el rango parcial.
    expect(findMatch.primeraIdentificacionAt.$gte).not.toEqual(mx(2026, 8, 10, 0, 0));
  });

  test('el BACKLOG no se ve afectado por el rango explícito', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({ fechaInicio: '2026-09-10', fechaFin: '2026-09-12' });

    const backlogCall = BankMovement.aggregate.mock.calls[0];
    expect('primeraIdentificacionAt' in backlogCall[0][0].$match).toBe(false);
  });
});

describe('buildReporteIdentificacion — Excel descargable (2026-09-18)', () => {
  function mockIdentificadosParaReporte(docs) {
    const lean = jest.fn().mockResolvedValue(docs);
    const sort = jest.fn().mockReturnValue({ lean });
    const select = jest.fn().mockReturnValue({ sort });
    BankMovement.find.mockReturnValue({ select });
    return { select, sort, lean };
  }

  test('genera un .xlsx válido con una fila por movimiento identificado, ordenado por fecha de identificación', async () => {
    mockIdentificadosParaReporte([
      {
        banco: 'BBVA', fecha: new Date('2026-09-10T12:00:00Z'), concepto: 'Depósito 1', deposito: 1000,
        categoria: 'Renta', createdAt: mx(2026, 8, 10, 8, 0), primeraIdentificacionAt: mx(2026, 8, 10, 10, 0),
        primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' },
      },
      {
        banco: 'Santander', fecha: new Date('2026-09-11T12:00:00Z'), concepto: 'Depósito 2', deposito: 2000,
        categoria: null, createdAt: mx(2026, 8, 11, 8, 0), primeraIdentificacionAt: mx(2026, 8, 11, 9, 0),
        primeraIdentificacionPor: null,
      },
    ]);

    const buffer = await buildReporteIdentificacion({ fechaInicio: '2026-09-10', fechaFin: '2026-09-11' });

    const wb = new ExcelJS.Workbook();
    await wb.xlsx.load(buffer);
    const ws = wb.getWorksheet('Identificación');
    expect(ws.rowCount).toBe(3); // header + 2 filas

    const headerVals = ws.getRow(1).values.slice(1);
    expect(headerVals).toEqual([
      'Banco', 'Fecha depósito', 'Concepto', 'Depósito', 'Categoría',
      'Identificado por', 'Fecha de identificación', 'Horas hábiles', 'Creado en Numo',
    ]);

    const fila1 = ws.getRow(2).values.slice(1);
    expect(fila1[0]).toBe('BBVA');
    expect(fila1[3]).toBe(1000);
    expect(fila1[5]).toBe('Ana');
    expect(fila1[7]).toBe(2); // horasHabilesEntre(8:00, 10:00) = 2

    // sin primeraIdentificacionPor -> celda vacía, no revienta (ExcelJS puede devolver
    // null o simplemente omitir la celda del array sparse de .values, según el caso).
    expect(ws.getRow(3).getCell(6).value).toBeFalsy();
  });

  test('sin movimientos: .xlsx válido con solo el header', async () => {
    mockIdentificadosParaReporte([]);

    const buffer = await buildReporteIdentificacion({ fechaInicio: '2026-09-10', fechaFin: '2026-09-10' });

    const wb = new ExcelJS.Workbook();
    await wb.xlsx.load(buffer);
    const ws = wb.getWorksheet('Identificación');
    expect(ws.rowCount).toBe(1);
  });

  test('usa el mismo criterio de filtro que getIndicadoresIdentificacion (_resolverMatchTiempo compartido)', async () => {
    mockIdentificadosParaReporte([]);

    await buildReporteIdentificacion({ banco: 'BBVA', scopeUserId: 'user-1', fechaInicio: '2026-09-10', fechaFin: '2026-09-10' });

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch.banco).toBe('BBVA');
    expect(findMatch['primeraIdentificacionPor.userId']).toBe('user-1');
    expect(findMatch.primeraIdentificacionAt.$gte).toEqual(mx(2026, 8, 10, 0, 0));
  });
});

describe('getIndicadoresIdentificacion — scopeUserId (dashboard de Cobranza, 2026-09-17)', () => {
  test('scopeUserId escalar: filtra find() por primeraIdentificacionPor.userId exacto', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({ scopeUserId: 'user-1' });

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch['primeraIdentificacionPor.userId']).toBe('user-1');
  });

  test('scopeUserId como array de 2: usa $in', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({ scopeUserId: ['user-1', 'user-2'] });

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch['primeraIdentificacionPor.userId']).toEqual({ $in: ['user-1', 'user-2'] });
  });

  test('scopeUserId como array VACÍO: se trata como sin filtro, nunca $in:[]', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({ scopeUserId: [] });

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect('primeraIdentificacionPor.userId' in findMatch).toBe(false);
  });

  test('sin scopeUserId: no se agrega la clave al match (equipo completo, comportamiento actual sin cambios)', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({});

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect('primeraIdentificacionPor.userId' in findMatch).toBe(false);
  });

  test('combinado con year/month: ambos filtros conviven en el mismo match', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({ scopeUserId: 'user-1', year: '2026', month: '8' });

    const findMatch = BankMovement.find.mock.calls[0][0];
    expect(findMatch['primeraIdentificacionPor.userId']).toBe('user-1');
    expect(findMatch.fecha).toEqual({ $gte: mx(2026, 7, 1, 0, 0), $lt: mx(2026, 8, 1, 0, 0) });
  });

  test('el BACKLOG nunca se acota por scopeUserId — sigue siendo de TODO el equipo', async () => {
    mockIdentificados([]);
    mockBacklog([]);

    await getIndicadoresIdentificacion({ scopeUserId: ['user-1', 'user-2'] });

    const backlogCall = BankMovement.aggregate.mock.calls[0];
    expect('primeraIdentificacionPor.userId' in backlogCall[0][0].$match).toBe(false);
  });

  test('scopeUserId sí filtra qué entra a promedio/mediana/porUsuario (no solo el match pedido a Mongo)', async () => {
    // El mock de find().select().lean() no simula el filtrado real de Mongo (devuelve los
    // docs tal cual) — este test confirma que, si Mongo SÍ aplicara el match (como en
    // producción), el resultado sería el esperado: acá se simula ese filtrado ya hecho,
    // pasando solo los docs de 'user-1' como si el $match ya los hubiera acotado.
    mockIdentificados([
      { createdAt: mx(2026, 7, 17, 8, 0), primeraIdentificacionAt: mx(2026, 7, 17, 10, 0), primeraIdentificacionPor: { userId: 'user-1', nombre: 'Ana' } },
    ]);
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({ scopeUserId: 'user-1' });

    expect(result.promedioHoras).toBe(2);
    expect(result.porUsuario).toEqual([{ userId: 'user-1', nombre: 'Ana', promedioHoras: 2, count: 1 }]);
  });
});

describe('listUsuariosConIdentificaciones — actividad real, sin filtrar por rol actual (dashboard de Cobranza)', () => {
  test('devuelve los userIds distintos que identificaron algo, filtrando null/undefined', async () => {
    BankMovement.distinct.mockResolvedValue(['user-1', 'user-2', null]);

    const ids = await listUsuariosConIdentificaciones();

    expect(ids).toEqual(['user-1', 'user-2']);
  });

  test('consulta con el criterio correcto: identificado + primeraIdentificacionPor real + cutoff INDICADORES_DESDE', async () => {
    BankMovement.distinct.mockResolvedValue([]);

    await listUsuariosConIdentificaciones();

    expect(BankMovement.distinct).toHaveBeenCalledWith('primeraIdentificacionPor.userId', {
      status: 'identificado',
      primeraIdentificacionPor: { $ne: null },
      createdAt: { $gte: mx(2026, 7, 17, 0, 0) },
    });
  });

  test('sin actividad: devuelve array vacío', async () => {
    BankMovement.distinct.mockResolvedValue([]);

    const ids = await listUsuariosConIdentificaciones();

    expect(ids).toEqual([]);
  });
});
