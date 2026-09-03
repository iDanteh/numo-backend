'use strict';

// collection-request-indicadores.service.test.js — getIndicadoresSolicitudesCobro():
// total (reloj real, createdAt→resueltoAt) + fase1Banco (reloj real,
// createdAt→BankMovement.createdAt del primer movimiento) + fase2Contador (horas
// hábiles, BankMovement.createdAt→resueltoAt). Mock de query encadenada
// find().select().populate().populate().lean() con el mismo criterio "populate real"
// que collection-request-build-report.test.js — un path solo se resuelve si
// .populate() fue invocado con ESE path exacto, para no simular de más.
jest.mock('./CollectionRequest.model');

const CollectionRequest = require('./CollectionRequest.model');
const {
  getIndicadoresSolicitudesCobro, horasReloj, inicioFaseContador, distribucionPorMinutos, INDICADORES_CR_DESDE,
  getDistribucionSolicitudesCobro,
} = require('./collection-request-indicadores.service');

// Mock de query encadenada find().select().lean() — getDistribucionSolicitudesCobro()
// no popula nada (solo necesita createdAt/resueltoAt), a diferencia de
// mockPopulatingQuery() de arriba (usada por getIndicadoresSolicitudesCobro()).
function mockSimpleQuery(docs) {
  return { select: jest.fn().mockReturnThis(), lean: jest.fn(() => Promise.resolve(docs)) };
}

function mockPopulatingQuery(docs, movimientosPorId) {
  const populatedPaths = new Set();
  const query = {
    select:   jest.fn().mockReturnThis(),
    populate: jest.fn((path) => { populatedPaths.add(path); return query; }),
    lean:     jest.fn(() => Promise.resolve(docs.map(doc => _aplicarPopulate(doc, populatedPaths, movimientosPorId)))),
  };
  return query;
}

function _aplicarPopulate(doc, populatedPaths, movimientosPorId) {
  const clon = { ...doc, formasPago: (doc.formasPago || []).map(f => ({ ...f })) };
  if (populatedPaths.has('bankMovementId') && clon.bankMovementId != null) {
    clon.bankMovementId = movimientosPorId[clon.bankMovementId] ?? clon.bankMovementId;
  }
  if (populatedPaths.has('formasPago.bankMovementId')) {
    clon.formasPago = clon.formasPago.map(f => (
      f.bankMovementId != null ? { ...f, bankMovementId: movimientosPorId[f.bankMovementId] ?? f.bankMovementId } : f
    ));
  }
  return clon;
}

function mov(id, createdAt) {
  return { _id: id, createdAt: new Date(createdAt) };
}

function cr(overrides = {}) {
  return {
    _id: 'cr-1',
    status: 'identificada',
    createdAt: new Date('2026-08-10T09:00:00Z'),
    resueltoAt: new Date('2026-08-10T15:00:00Z'),
    resueltoPorUserId: 'user-1',
    resueltoPorNombre: 'Ana',
    bankMovementId: null,
    formasPago: [{ _id: 'f1', bankMovementId: 'mov-1' }],
    ...overrides,
  };
}

describe('horasReloj()', () => {
  test('diferencia simple en horas', () => {
    expect(horasReloj(new Date('2026-08-10T09:00:00Z'), new Date('2026-08-10T15:00:00Z'))).toBe(6);
  });

  test('fin <= inicio devuelve 0 (no negativos)', () => {
    expect(horasReloj(new Date('2026-08-10T15:00:00Z'), new Date('2026-08-10T09:00:00Z'))).toBe(0);
    expect(horasReloj(new Date('2026-08-10T09:00:00Z'), new Date('2026-08-10T09:00:00Z'))).toBe(0);
  });

  test('cuenta fin de semana/noche en reloj real (no son horas hábiles)', () => {
    // Sábado 22:00 -> lunes 08:00 = 34 horas de reloj real, aunque casi todo
    // caiga fuera de horario laboral.
    expect(horasReloj(new Date('2026-08-08T22:00:00Z'), new Date('2026-08-10T08:00:00Z'))).toBe(34);
  });
});

describe('inicioFaseContador() — fix real 2026-08-20: el reloj del contador nunca arranca antes de que la solicitud exista', () => {
  test('el depósito llegó DESPUÉS de crear la solicitud: arranca en el depósito (caso "normal")', () => {
    const crCreatedAt  = new Date('2026-08-20T09:00:00Z');
    const movCreatedAt = new Date('2026-08-20T15:00:00Z');
    expect(inicioFaseContador(crCreatedAt, movCreatedAt)).toEqual(movCreatedAt);
  });

  test('el depósito YA EXISTÍA antes de crear la solicitud (caso real más común): arranca en la solicitud, NO en el depósito viejo', () => {
    const movCreatedAt = new Date('2026-08-10T08:00:00Z'); // depósito importado hace días
    const crCreatedAt  = new Date('2026-08-20T09:00:00Z'); // solicitud creada después
    expect(inicioFaseContador(crCreatedAt, movCreatedAt)).toEqual(crCreatedAt);
  });

  test('mismas fechas exactas: cualquiera de las dos, el resultado es ese instante', () => {
    const fecha = new Date('2026-08-20T09:00:00Z');
    expect(inicioFaseContador(fecha, fecha)).toEqual(fecha);
  });
});

describe('distribucionPorMinutos() — pedido 2026-08-28: franjas de 30min calibradas contra el ciclo real de carga bancaria', () => {
  test('bucketing básico con datos conocidos, cortes default [30,60,120] (4 franjas, corrección /frontend-design del mismo día: se sacó el paso de 90min por no representar ningún límite operativo real)', () => {
    // 5 valores: 10min, 29min, 30min (borde, entra al SIGUIENTE bucket), 75min, 200min.
    const horasArr = [10 / 60, 29 / 60, 30 / 60, 75 / 60, 200 / 60];

    const res = distribucionPorMinutos(horasArr);

    expect(res).toEqual([
      { desdeMin: 0,   hastaMin: 30,   count: 2, porcentaje: 40 },  // 10min, 29min
      { desdeMin: 30,  hastaMin: 60,   count: 1, porcentaje: 20 },  // 30min (borde inclusivo abajo)
      { desdeMin: 60,  hastaMin: 120,  count: 1, porcentaje: 20 },  // 75min
      { desdeMin: 120, hastaMin: null, count: 1, porcentaje: 20 }, // 200min, bucket abierto
    ]);
  });

  test('los porcentajes suman ~100 (redondeo independiente por bucket, tolerancia chica esperada)', () => {
    const horasArr = [5, 20, 45, 50, 65, 100, 130].map(m => m / 60);

    const res = distribucionPorMinutos(horasArr);
    const sumaPorcentajes = res.reduce((acc, b) => acc + b.porcentaje, 0);
    const sumaCounts = res.reduce((acc, b) => acc + b.count, 0);

    expect(sumaCounts).toBe(horasArr.length);
    // Cada bucket redondea a 1 decimal de forma independiente (no ajuste tipo "largest
    // remainder") — la suma puede desviarse de 100 por hasta ~0.05 * cantidad de buckets,
    // nunca más. No usar toBeCloseTo con precisión alta acá, es matemáticamente esperable
    // que no dé exactamente 100.
    expect(Math.abs(sumaPorcentajes - 100)).toBeLessThan(0.5);
  });

  test('array vacío: todos los buckets en 0, nunca NaN', () => {
    const res = distribucionPorMinutos([]);

    expect(res).toHaveLength(4);
    res.forEach(b => {
      expect(b.count).toBe(0);
      expect(b.porcentaje).toBe(0);
      expect(Number.isNaN(b.porcentaje)).toBe(false);
    });
  });

  test('cortes custom', () => {
    const horasArr = [15 / 60, 45 / 60];

    const res = distribucionPorMinutos(horasArr, [30]);

    expect(res).toEqual([
      { desdeMin: 0, hastaMin: 30, count: 1, porcentaje: 50 },
      { desdeMin: 30, hastaMin: null, count: 1, porcentaje: 50 },
    ]);
  });
});

describe('getIndicadoresSolicitudesCobro()', () => {
  test('N=1: total (reloj real) + fase1Banco (reloj real) + fase2Contador (horas hábiles), populando ambos paths', async () => {
    const doc = cr({
      createdAt:  new Date('2026-08-10T09:00:00Z'), // solicitud creada
      resueltoAt: new Date('2026-08-11T10:00:00Z'), // identificada
    });
    const movimientosPorId = {
      'mov-1': mov('mov-1', '2026-08-10T15:00:00Z'), // depósito cargado en Numo
    };
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery([doc], movimientosPorId));

    const res = await getIndicadoresSolicitudesCobro({});

    // 2026-08-20: sin year/month, el corte de frescura (INDICADORES_CR_DESDE) SIEMPRE
    // se aplica — el mock no filtra por fecha (por eso las fixtures de este archivo usan
    // fechas de antes del corte sin problema), pero el query real que se manda sí debe
    // llevarlo.
    expect(CollectionRequest.find).toHaveBeenCalledWith({
      status: 'identificada', resueltoAt: { $ne: null }, createdAt: { $gte: INDICADORES_CR_DESDE },
    });
    expect(res.totalSolicitudesResueltas).toBe(1);
    expect(res.sinMovimientoVinculado).toBe(0);
    // total: 09:00 10/08 -> 10:00 11/08 = 25h de reloj real.
    expect(res.total.promedioHoras).toBeCloseTo(25, 5);
    // fase1Banco: 09:00 10/08 -> 15:00 10/08 = 6h de reloj real (depósito tardó en verse).
    expect(res.fase1Banco.promedioHoras).toBeCloseTo(6, 5);
    // fase2Contador: 15:00 10/08 -> 10:00 11/08, horas HÁBILES (8-20h, L-S) — no 19h de reloj.
    expect(res.fase2Contador.promedioHoras).toBeLessThan(19);
    expect(res.fase2Contador.promedioHoras).toBeGreaterThan(0);
  });

  test('sin movimiento vinculado: cuenta para el total pero NO para fase1/fase2', async () => {
    const doc = cr({ formasPago: [] }); // movimientosDe(cr) devuelve [] (sin fallback root)
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery([doc], {}));

    const res = await getIndicadoresSolicitudesCobro({});

    expect(res.sinMovimientoVinculado).toBe(1);
    expect(res.total.count).toBe(1);
    expect(res.fase1Banco.count).toBe(0);
    expect(res.fase2Contador.count).toBe(0);
  });

  test('fallback al campo raíz bankMovementId cuando formasPago viene vacío (documentos pre-backfill)', async () => {
    const doc = cr({ formasPago: [], bankMovementId: 'mov-legacy' });
    const movimientosPorId = { 'mov-legacy': mov('mov-legacy', '2026-08-10T12:00:00Z') };
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery([doc], movimientosPorId));

    const res = await getIndicadoresSolicitudesCobro({});

    expect(res.sinMovimientoVinculado).toBe(0);
    expect(res.fase1Banco.count).toBe(1);
  });

  test('filtro year/month posterior al corte de frescura: se usa el rango pedido tal cual', async () => {
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery([], {}));

    await getIndicadoresSolicitudesCobro({ year: '2027', month: '3' });

    expect(CollectionRequest.find).toHaveBeenCalledWith({
      status: 'identificada',
      resueltoAt: { $ne: null },
      createdAt: { $gte: new Date(2027, 2, 1), $lt: new Date(2027, 3, 1) },
    });
  });

  test('filtro year/month ANTERIOR al corte de frescura: el corte gana, nunca se esquiva', async () => {
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery([], {}));

    // Pedir explícitamente agosto 2026 (que arranca antes del corte, 2026-08-20) no debe
    // volver a mezclar datos de antes del corte — $gte queda en el corte, no en el 1ro.
    await getIndicadoresSolicitudesCobro({ year: '2026', month: '8' });

    expect(CollectionRequest.find).toHaveBeenCalledWith({
      status: 'identificada',
      resueltoAt: { $ne: null },
      createdAt: { $gte: INDICADORES_CR_DESDE, $lt: new Date(2026, 8, 1) },
    });
  });

  test('regresión del bug real 2026-08-20: depósito importado ANTES de crear la solicitud no infla fase2Contador', async () => {
    // Escenario real reportado por el usuario: el BankMovement ya existía en Numo desde
    // hace días cuando la tienda creó la solicitud — sin el fix, fase2Contador arrancaba
    // en la fecha vieja del depósito y sumaba días de horas hábiles que nunca fueron una
    // espera real para el contador.
    // horasHabilesEntre() lee horas de calendario LOCALES (new Date().getHours()/setHours()),
    // igual que el resto del dashboard — se construyen estas fechas en hora LOCAL (sin 'Z')
    // para que el ejemplo sea determinístico sin importar el timezone de quien corra el test.
    const doc = cr({
      createdAt:  new Date(2026, 7, 20, 9, 0, 0), // solicitud creada, jueves 09:00 local
      resueltoAt: new Date(2026, 7, 20, 9, 5, 0), // identificada 5 minutos después
    });
    const movimientosPorId = {
      'mov-1': mov('mov-1', new Date(2026, 7, 10, 8, 0, 0)), // depósito importado 10 días antes
    };
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery([doc], movimientosPorId));

    const res = await getIndicadoresSolicitudesCobro({});

    // fase1Banco: el depósito es ANTERIOR a la solicitud -> horasReloj() devuelve 0 (no
    // negativos), nunca "el depósito tardó -10 días".
    expect(res.fase1Banco.promedioHoras).toBe(0);
    // fase2Contador: debe medir SOLO los 5 minutos reales entre solicitud e identificación
    // (arranca en cr.createdAt, no en el depósito de 10 días atrás) — antes del fix esto
    // habría sido ~10 días de horas hábiles.
    expect(res.fase2Contador.promedioHoras).toBeCloseTo(5 / 60, 3);
  });

  test('expone distribucionTotal calculada sobre totalHorasArr (25h -> bucket abierto >120min)', async () => {
    const doc = cr({
      createdAt:  new Date('2026-08-10T09:00:00Z'),
      resueltoAt: new Date('2026-08-11T10:00:00Z'), // 25h de total
    });
    const movimientosPorId = { 'mov-1': mov('mov-1', '2026-08-10T15:00:00Z') };
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery([doc], movimientosPorId));

    const res = await getIndicadoresSolicitudesCobro({});

    expect(res.distribucionTotal).toEqual(distribucionPorMinutos([25]));
    expect(res.distribucionTotal.find(b => b.hastaMin === null)).toMatchObject({ count: 1, porcentaje: 100 });
  });

  test('scopeUserId ausente: trae todo el equipo (comportamiento actual, sin cambios) — match sin resueltoPorUserId', async () => {
    const docs = [
      cr({ _id: 'cr-1', resueltoPorUserId: 'u1' }),
      cr({ _id: 'cr-2', resueltoPorUserId: 'u2' }),
    ];
    const movimientosPorId = { 'mov-1': mov('mov-1', '2026-08-10T15:00:00Z') };
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery(docs, movimientosPorId));

    const res = await getIndicadoresSolicitudesCobro({});

    expect(CollectionRequest.find).toHaveBeenCalledWith({
      status: 'identificada', resueltoAt: { $ne: null }, createdAt: { $gte: INDICADORES_CR_DESDE },
    });
    expect(res.totalSolicitudesResueltas).toBe(2);
  });

  test('scopeUserId presente: acota el match a resueltoPorUserId, para que la query solo traiga lo de ese usuario', async () => {
    // El filtrado real por usuario ocurre en el $match de Mongo, no en JS post-fetch —
    // el mock de CollectionRequest.find() no filtra los docs devueltos (no simula Mongo),
    // así que lo que se verifica acá es que el match pedido a la query incluya
    // resueltoPorUserId con el scope pasado. Un segundo caso con 2 usuarios distintos
    // en el fixture confirma que, si el filtro SÍ se aplicara (como en Mongo real), solo
    // pasarían las de 'u1'.
    const docs = [
      cr({ _id: 'cr-1', resueltoPorUserId: 'u1' }),
      cr({ _id: 'cr-2', resueltoPorUserId: 'u2' }),
    ];
    const movimientosPorId = { 'mov-1': mov('mov-1', '2026-08-10T15:00:00Z') };
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery(docs, movimientosPorId));

    await getIndicadoresSolicitudesCobro({ scopeUserId: 'u1' });

    expect(CollectionRequest.find).toHaveBeenCalledWith({
      status: 'identificada',
      resueltoAt: { $ne: null },
      resueltoPorUserId: 'u1',
      createdAt: { $gte: INDICADORES_CR_DESDE },
    });
  });

  test('scopeUserId se combina con year/month, no lo reemplaza', async () => {
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery([], {}));

    await getIndicadoresSolicitudesCobro({ year: '2027', month: '3', scopeUserId: 'u1' });

    expect(CollectionRequest.find).toHaveBeenCalledWith({
      status: 'identificada',
      resueltoAt: { $ne: null },
      resueltoPorUserId: 'u1',
      createdAt: { $gte: new Date(2027, 2, 1), $lt: new Date(2027, 3, 1) },
    });
  });
});

describe('getDistribucionSolicitudesCobro() — pedido 2026-09-03: distribución acotada al día actual (o al rango elegido), independiente del año/mes del panel general', () => {
  afterEach(() => jest.useRealTimers());

  test('sin desde/hasta usa el día de hoy (hora de México)', async () => {
    // 18:00 UTC = 12:00 hora de México (UTC-6) del MISMO día calendario — sin
    // riesgo de cruzar medianoche MX al fijar la hora del sistema.
    jest.useFakeTimers({ now: new Date('2026-09-03T18:00:00.000Z') });
    CollectionRequest.find.mockReturnValue(mockSimpleQuery([]));

    const res = await getDistribucionSolicitudesCobro({});

    expect(CollectionRequest.find).toHaveBeenCalledWith({
      status: 'identificada',
      resueltoAt: { $ne: null },
      createdAt: { $gte: new Date('2026-09-03T06:00:00.000Z'), $lt: new Date('2026-09-04T06:00:00.000Z') },
    });
    expect(res.desde).toBe('2026-09-03');
    expect(res.hasta).toBe('2026-09-03');
    expect(res.total).toBe(0);
  });

  test('con desde/hasta explícitos filtra por ese rango', async () => {
    CollectionRequest.find.mockReturnValue(mockSimpleQuery([
      { createdAt: new Date('2026-08-26T10:00:00Z'), resueltoAt: new Date('2026-08-26T12:00:00Z') },
    ]));

    const res = await getDistribucionSolicitudesCobro({ desde: '2026-08-25', hasta: '2026-08-27' });

    expect(CollectionRequest.find).toHaveBeenCalledWith({
      status: 'identificada',
      resueltoAt: { $ne: null },
      createdAt: { $gte: new Date('2026-08-25T06:00:00.000Z'), $lt: new Date('2026-08-28T06:00:00.000Z') },
    });
    expect(res.desde).toBe('2026-08-25');
    expect(res.hasta).toBe('2026-08-27');
    expect(res.total).toBe(1);
    expect(res.distribucionTotal).toEqual(distribucionPorMinutos([2]));
  });

  test('desde/hasta fuera del corte INDICADORES_CR_DESDE no lo esquiva: el corte gana', async () => {
    CollectionRequest.find.mockReturnValue(mockSimpleQuery([]));

    await getDistribucionSolicitudesCobro({ desde: '2025-01-01', hasta: '2025-01-02' });

    expect(CollectionRequest.find).toHaveBeenCalledWith({
      status: 'identificada',
      resueltoAt: { $ne: null },
      createdAt: { $gte: INDICADORES_CR_DESDE, $lt: new Date('2025-01-03T06:00:00.000Z') },
    });
  });

  test('scopeUserId ausente: match sin resueltoPorUserId (comportamiento actual, todo el equipo)', async () => {
    CollectionRequest.find.mockReturnValue(mockSimpleQuery([]));

    await getDistribucionSolicitudesCobro({ desde: '2026-08-25', hasta: '2026-08-27' });

    expect(CollectionRequest.find).toHaveBeenCalledWith({
      status: 'identificada',
      resueltoAt: { $ne: null },
      createdAt: { $gte: new Date('2026-08-25T06:00:00.000Z'), $lt: new Date('2026-08-28T06:00:00.000Z') },
    });
  });

  test('scopeUserId presente: se agrega al match para acotar la distribución a ese usuario', async () => {
    CollectionRequest.find.mockReturnValue(mockSimpleQuery([]));

    await getDistribucionSolicitudesCobro({ desde: '2026-08-25', hasta: '2026-08-27', scopeUserId: 'u1' });

    expect(CollectionRequest.find).toHaveBeenCalledWith({
      status: 'identificada',
      resueltoAt: { $ne: null },
      resueltoPorUserId: 'u1',
      createdAt: { $gte: new Date('2026-08-25T06:00:00.000Z'), $lt: new Date('2026-08-28T06:00:00.000Z') },
    });
  });
});
