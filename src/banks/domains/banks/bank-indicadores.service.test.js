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
  _calificaPorSaldoRestanteBajo, _masRecienteIdentificadoPor, _proxyFechaEnRango, _enScopeUserId,
  _resolverCasiIdentificadosPorSaldoBajo, SALDO_RESTANTE_TOLERANCIA,
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

// getIndicadoresIdentificacion() ahora llama BankMovement.find() DOS veces en el mismo
// Promise.all (identificados reales, luego candidatos crudos de saldo-restante-bajo) — a
// diferencia de mockIdentificados() (que responde lo mismo a cualquier llamada), este helper
// encadena mockReturnValueOnce en el mismo orden en que el service arma el array del
// Promise.all, así cada find() devuelve la data que le corresponde.
function mockIdentificadosYCandidatos(identificadosDocs, candidatosDocs) {
  const leanIdentificados = jest.fn().mockResolvedValue(identificadosDocs);
  const selectIdentificados = jest.fn().mockReturnValue({ lean: leanIdentificados });
  const leanCandidatos = jest.fn().mockResolvedValue(candidatosDocs);
  const selectCandidatos = jest.fn().mockReturnValue({ lean: leanCandidatos });
  BankMovement.find
    .mockReturnValueOnce({ select: selectIdentificados })
    .mockReturnValueOnce({ select: selectCandidatos });
}

describe('horasHabilesEntre — L-V 8:00-20:00, sábado 8:00-15:00, domingo 0 (hora de México)', () => {
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

  test('cruza un sábado completo (2026-08-22): suma sus 7h (8:00-15:00)', () => {
    // Viernes 21 20:00 (fin de ventana, aporta 0) → domingo 23 00:00 (aporta 0) — solo
    // queda el sábado completo en el medio, acotado a 8:00-15:00.
    const h = horasHabilesEntre(mx(2026, 7, 21, 20, 0), mx(2026, 7, 23, 0, 0));
    expect(h).toBe(7);
  });

  test('domingo completo (2026-08-23) no suma nada dentro de un tramo mixto', () => {
    // Sábado 8:00-15:00 (7h) + domingo (0h) + lunes 8:00-20:00 (12h) = 19h —
    // si el domingo sumara algo, el total no daría este número.
    const h = horasHabilesEntre(mx(2026, 7, 22, 8, 0), mx(2026, 7, 24, 20, 0));
    expect(h).toBe(19);
  });

  test('viernes 19:00 → lunes 10:00: 1h viernes + 7h sábado + 0h domingo + 2h lunes = 10h', () => {
    const h = horasHabilesEntre(mx(2026, 7, 21, 19, 0), mx(2026, 7, 24, 10, 0));
    expect(h).toBe(10);
  });

  test('span de más de una semana: lunes 8:00 al lunes siguiente 8:00 = 67h hábiles', () => {
    // 17(lun) 18(mar) 19(mié) 20(jue) 21(vie) = 5 días × 12h = 60h; 22(sáb) = 7h (8:00-15:00);
    // 23(dom) = 0h; 24(lun) aporta 0 porque el tramo termina justo a las 8:00, sin
    // adelantarse a la ventana. Total: 60 + 7 = 67h.
    const h = horasHabilesEntre(mx(2026, 7, 17, 8, 0), mx(2026, 7, 24, 8, 0));
    expect(h).toBe(67);
  });

  test('borde exacto: un día completo 8:00-20:00 da exactamente 12h, sin off-by-one', () => {
    const h = horasHabilesEntre(mx(2026, 7, 17, 8, 0), mx(2026, 7, 17, 20, 0));
    expect(h).toBe(12);
  });

  test('sábado después de las 15:00 se clampea al fin de su ventana (2026-08-22)', () => {
    const h = horasHabilesEntre(mx(2026, 7, 22, 14, 0), mx(2026, 7, 22, 19, 0));
    expect(h).toBe(1); // 14:00-15:00, no 14:00-19:00
  });

  test('sábado completo 8:00-15:00 da exactamente 7h, sin off-by-one', () => {
    const h = horasHabilesEntre(mx(2026, 7, 22, 8, 0), mx(2026, 7, 22, 15, 0));
    expect(h).toBe(7);
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
    // Viernes 21 19:00 → lunes 24 10:00 = 10h hábiles (ver test de horasHabilesEntre) —
    // en tiempo de reloj serían ~63h. Si el service todavía calculara en reloj, este test fallaría.
    mockIdentificados([
      { createdAt: mx(2026, 7, 21, 19, 0), primeraIdentificacionAt: mx(2026, 7, 24, 10, 0) },
    ]);
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({});

    expect(result.promedioHoras).toBe(10);
    expect(result.medianaHoras).toBe(10);
    expect(result.totalIdentificadosConDato).toBe(1);
  });

  test('la mediana resiste un outlier que sí infla el promedio', async () => {
    // 4 movimientos de 2h hábiles + 1 de 67h hábiles (lunes a lunes, ver test de arriba).
    const rapido = () => ({ createdAt: mx(2026, 7, 17, 8, 0), primeraIdentificacionAt: mx(2026, 7, 17, 10, 0) }); // 2h
    const lento  = { createdAt: mx(2026, 7, 17, 8, 0), primeraIdentificacionAt: mx(2026, 7, 24, 8, 0) }; // 67h
    mockIdentificados([rapido(), rapido(), rapido(), rapido(), lento]);
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({});

    expect(result.promedioHoras).toBe(15);  // (2+2+2+2+67)/5
    expect(result.medianaHoras).toBe(2);    // valor central de [2,2,2,2,67]
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

describe('_calificaPorSaldoRestanteBajo — tolerancia <= 20% (2026-09-21, decisión explícita del usuario)', () => {
  test('califica cuando el saldo restante es exactamente 20% del total (borde inclusivo)', () => {
    expect(_calificaPorSaldoRestanteBajo([{ saldoActual: 100, total: 500 }])).toBe(true);
  });

  test('no califica cuando el saldo restante supera el 20%', () => {
    expect(_calificaPorSaldoRestanteBajo([{ saldoActual: 150, total: 500 }])).toBe(false);
  });

  test('CxC de 500 cobrando 400 (100 de saldo restante) califica — caso real del usuario', () => {
    expect(_calificaPorSaldoRestanteBajo([{ saldoActual: 100, total: 500 }])).toBe(true);
  });

  test('suma TODAS las erpLinks antes de sacar el %, no CxC por CxC (decisión explícita del usuario)', () => {
    // Individualmente el 2do link tiene 40% restante (no calificaría solo), pero el conjunto
    // (120/1000 = 12%) sí califica.
    const links = [{ saldoActual: 20, total: 500 }, { saldoActual: 100, total: 500 }];
    expect(_calificaPorSaldoRestanteBajo(links)).toBe(true);
  });

  test('no califica si a cualquier link le falta `total` (dato incompleto, no se adivina)', () => {
    const links = [{ saldoActual: 10, total: 500 }, { saldoActual: 5, total: null }];
    expect(_calificaPorSaldoRestanteBajo(links)).toBe(false);
  });

  test('no califica si `total` es 0 o negativo en cualquier link', () => {
    expect(_calificaPorSaldoRestanteBajo([{ saldoActual: 0, total: 0 }])).toBe(false);
  });

  test('sin erpLinks (vacío o no-array): no califica', () => {
    expect(_calificaPorSaldoRestanteBajo([])).toBe(false);
    expect(_calificaPorSaldoRestanteBajo(null)).toBe(false);
    expect(_calificaPorSaldoRestanteBajo(undefined)).toBe(false);
  });

  test('SALDO_RESTANTE_TOLERANCIA está fijada en 0.20', () => {
    expect(SALDO_RESTANTE_TOLERANCIA).toBe(0.20);
  });
});

describe('_masRecienteIdentificadoPor — fecha/usuario proxy', () => {
  test('elige la entrada con fechaId más reciente entre varias', () => {
    const entry = _masRecienteIdentificadoPor([
      { userId: 'user-1', nombre: 'Ana',  fechaId: new Date('2026-09-01T10:00:00Z') },
      { userId: 'user-2', nombre: 'Luis', fechaId: new Date('2026-09-05T10:00:00Z') },
      { userId: 'user-3', nombre: 'Eva',  fechaId: new Date('2026-09-03T10:00:00Z') },
    ]);
    expect(entry).toEqual({ userId: 'user-2', nombre: 'Luis', fechaId: new Date('2026-09-05T10:00:00Z') });
  });

  test('ignora entradas sin fechaId al elegir', () => {
    const entry = _masRecienteIdentificadoPor([
      { userId: 'user-1', nombre: 'Ana', fechaId: null },
      { userId: 'user-2', nombre: 'Luis', fechaId: new Date('2026-09-05T10:00:00Z') },
    ]);
    expect(entry.userId).toBe('user-2');
  });

  test('devuelve null si ninguna entrada tiene fechaId (se deja fuera, no se inventa fecha)', () => {
    expect(_masRecienteIdentificadoPor([{ userId: 'user-1', fechaId: null }])).toBeNull();
    expect(_masRecienteIdentificadoPor([])).toBeNull();
    expect(_masRecienteIdentificadoPor(undefined)).toBeNull();
  });
});

describe('_proxyFechaEnRango — mismo criterio de precedencia que primeraIdentificacionAtMatch', () => {
  test('rango explícito: dentro de fechaInicio/fechaFin', () => {
    const proxy = mx(2026, 8, 11, 12, 0); // 2026-09-11 mediodía MX
    expect(_proxyFechaEnRango({ fechaInicio: '2026-09-10', fechaFin: '2026-09-12' }, proxy)).toBe(true);
  });

  test('rango explícito: fuera de fechaInicio/fechaFin', () => {
    const proxy = mx(2026, 8, 20, 12, 0);
    expect(_proxyFechaEnRango({ fechaInicio: '2026-09-10', fechaFin: '2026-09-12' }, proxy)).toBe(false);
  });

  test('year sin rango explícito: cualquier fecha vale (mismo criterio que $ne:null)', () => {
    expect(_proxyFechaEnRango({ year: '2020' }, mx(2026, 8, 18, 12, 0))).toBe(true);
  });

  test('default (sin year, sin rango explícito): solo cuenta si la fecha proxy es HOY en México', () => {
    jest.useFakeTimers({ doNotFake: ['nextTick'] });
    jest.setSystemTime(mx(2026, 8, 18, 15, 30));
    try {
      expect(_proxyFechaEnRango({}, mx(2026, 8, 18, 9, 0))).toBe(true);
      expect(_proxyFechaEnRango({}, mx(2026, 8, 17, 9, 0))).toBe(false);
    } finally {
      jest.useRealTimers();
    }
  });
});

describe('_enScopeUserId — equivalente en JS de _matchScopeUserId', () => {
  test('sin scope (null/undefined): siempre true', () => {
    expect(_enScopeUserId(null, 'user-1')).toBe(true);
    expect(_enScopeUserId(undefined, 'user-1')).toBe(true);
  });

  test('array vacío: siempre true (nunca "nadie")', () => {
    expect(_enScopeUserId([], 'user-1')).toBe(true);
  });

  test('array con elementos: debe estar incluido', () => {
    expect(_enScopeUserId(['user-1', 'user-2'], 'user-1')).toBe(true);
    expect(_enScopeUserId(['user-1', 'user-2'], 'user-3')).toBe(false);
  });

  test('escalar: igualdad exacta', () => {
    expect(_enScopeUserId('user-1', 'user-1')).toBe(true);
    expect(_enScopeUserId('user-1', 'user-2')).toBe(false);
  });
});

describe('_resolverCasiIdentificadosPorSaldoBajo — integra calificación + fecha proxy + scope', () => {
  test('resuelve _id/createdAt/proxyFecha/userId/nombre de un candidato que califica', () => {
    const fechaId = new Date('2026-09-18T12:00:00Z');
    const candidatos = [{
      _id: 'mov-1', createdAt: new Date('2026-09-15T08:00:00Z'),
      erpLinks: [{ saldoActual: 100, total: 500 }],
      identificadoPor: [{ userId: 'user-1', nombre: 'Ana', fechaId }],
    }];

    const resultado = _resolverCasiIdentificadosPorSaldoBajo(candidatos, { year: '2026' });

    expect(resultado).toEqual([
      { _id: 'mov-1', createdAt: candidatos[0].createdAt, proxyFecha: fechaId, userId: 'user-1', nombre: 'Ana' },
    ]);
  });

  test('descarta un candidato que no califica por saldo restante', () => {
    const candidatos = [{
      _id: 'mov-1', createdAt: new Date(),
      erpLinks: [{ saldoActual: 300, total: 500 }],
      identificadoPor: [{ userId: 'user-1', fechaId: new Date() }],
    }];
    expect(_resolverCasiIdentificadosPorSaldoBajo(candidatos, { year: '2026' })).toEqual([]);
  });

  test('descarta un candidato sin ningún identificadoPor con fecha', () => {
    const candidatos = [{
      _id: 'mov-1', createdAt: new Date(),
      erpLinks: [{ saldoActual: 50, total: 500 }],
      identificadoPor: [{ userId: 'user-1', fechaId: null }],
    }];
    expect(_resolverCasiIdentificadosPorSaldoBajo(candidatos, { year: '2026' })).toEqual([]);
  });

  test('descarta un candidato fuera del rango de fecha resuelto', () => {
    const candidatos = [{
      _id: 'mov-1', createdAt: new Date('2026-09-15T08:00:00Z'),
      erpLinks: [{ saldoActual: 50, total: 500 }],
      identificadoPor: [{ userId: 'user-1', fechaId: new Date('2026-09-15T08:00:00Z') }],
    }];
    const resultado = _resolverCasiIdentificadosPorSaldoBajo(
      candidatos, { fechaInicio: '2026-09-20', fechaFin: '2026-09-21' },
    );
    expect(resultado).toEqual([]);
  });

  test('descarta un candidato fuera del scopeUserId', () => {
    const candidatos = [{
      _id: 'mov-1', createdAt: new Date(),
      erpLinks: [{ saldoActual: 50, total: 500 }],
      identificadoPor: [{ userId: 'user-1', fechaId: new Date() }],
    }];
    expect(_resolverCasiIdentificadosPorSaldoBajo(candidatos, { year: '2026', scopeUserId: 'user-2' })).toEqual([]);
  });
});

describe('getIndicadoresIdentificacion — saldo restante bajo (2026-09-21, SOLO para este dashboard)', () => {
  test('un candidato que califica se suma a promedio/mediana usando la fecha proxy, y sale del backlog', async () => {
    const createdAt   = mx(2026, 7, 17, 8, 0);
    const fechaProxy  = mx(2026, 7, 17, 12, 0); // 4h hábiles después
    mockIdentificadosYCandidatos(
      [], // sin identificados reales
      [{
        _id: 'mov-casi-1', createdAt,
        erpLinks: [{ saldoActual: 100, total: 500 }], // 20% restante, califica
        identificadoPor: [{ userId: 'user-1', nombre: 'Ana', fechaId: fechaProxy }],
      }],
    );
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({ year: '2026' });

    expect(result.promedioHoras).toBe(4);
    expect(result.totalIdentificadosConDato).toBe(1);
    expect(result.porUsuario).toEqual([{ userId: 'user-1', nombre: 'Ana', promedioHoras: 4, count: 1 }]);

    // El backlog debe excluir explícitamente ese _id — ya no debe contarse como pendiente.
    const backlogCall = BankMovement.aggregate.mock.calls[0];
    expect(backlogCall[0][0].$match._id).toEqual({ $nin: ['mov-casi-1'] });
  });

  test('un candidato que NO califica (saldo restante > 20%) no se suma y sigue en backlog (sin excluir)', async () => {
    mockIdentificadosYCandidatos(
      [],
      [{
        _id: 'mov-no-1', createdAt: mx(2026, 7, 17, 8, 0),
        erpLinks: [{ saldoActual: 300, total: 500 }], // 60% restante, no califica
        identificadoPor: [{ userId: 'user-1', nombre: 'Ana', fechaId: mx(2026, 7, 17, 12, 0) }],
      }],
    );
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({ year: '2026' });

    expect(result.totalIdentificadosConDato).toBe(0);
    const backlogCall = BankMovement.aggregate.mock.calls[0];
    expect(backlogCall[0][0].$match._id).toEqual({ $nin: [] });
  });

  test('scopeUserId también filtra a los candidatos por saldo restante bajo', async () => {
    mockIdentificadosYCandidatos(
      [],
      [{
        _id: 'mov-casi-2', createdAt: mx(2026, 7, 17, 8, 0),
        erpLinks: [{ saldoActual: 0, total: 500 }],
        identificadoPor: [{ userId: 'user-1', nombre: 'Ana', fechaId: mx(2026, 7, 17, 12, 0) }],
      }],
    );
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({ year: '2026', scopeUserId: 'user-2' });

    expect(result.totalIdentificadosConDato).toBe(0);
  });

  test('sin candidatos crudos de erpLinks no vacío: comportamiento idéntico al de antes (sin regresión)', async () => {
    mockIdentificadosYCandidatos(
      [{ createdAt: mx(2026, 7, 17, 8, 0), primeraIdentificacionAt: mx(2026, 7, 17, 10, 0) }],
      [],
    );
    mockBacklog([]);

    const result = await getIndicadoresIdentificacion({ year: '2026' });

    expect(result.promedioHoras).toBe(2);
    expect(result.totalIdentificadosConDato).toBe(1);
    const backlogCall = BankMovement.aggregate.mock.calls[0];
    expect(backlogCall[0][0].$match._id).toEqual({ $nin: [] });
  });

  test('el segundo find() pide status pendiente + erpLinks no vacío, mismo banco/categoria/cutoff que el backlog', async () => {
    mockIdentificadosYCandidatos([], []);
    mockBacklog([]);

    await getIndicadoresIdentificacion({ banco: 'BBVA', categoria: 'Renta' });

    const candidatosMatch = BankMovement.find.mock.calls[1][0];
    expect(candidatosMatch.status).toEqual({ $in: ['no_identificado', 'reclasificado'] });
    expect(candidatosMatch['erpLinks.0']).toEqual({ $exists: true });
    expect(candidatosMatch.banco).toBe('BBVA');
    expect(candidatosMatch.categoria).toBe('Renta');
    expect(candidatosMatch.createdAt).toEqual({ $gte: mx(2026, 7, 17, 0, 0) });
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
