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
  getIndicadoresSolicitudesCobro, horasReloj, inicioFaseContador, INDICADORES_CR_DESDE,
} = require('./collection-request-indicadores.service');

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
    // 2026-08-20 (fix real): porUsuario usa la FASE CONTADOR (horas hábiles), no el
    // total (reloj real) — con N=1 debe coincidir EXACTO con fase2Contador.promedioHoras
    // (25h de total vs. este valor, mucho menor, es justo el bug real que reportó el
    // usuario: cada contador parecía "rápido" en total pero el bucket de fase contador
    // mostraba horas — porque medían cosas distintas).
    expect(res.porUsuario).toEqual([{ userId: 'user-1', nombre: 'Ana', promedioHoras: res.fase2Contador.promedioHoras, count: 1 }]);
    expect(res.porUsuario[0].promedioHoras).not.toBeCloseTo(res.total.promedioHoras, 1);
  });

  test('sin movimiento vinculado: cuenta para el total pero NO para fase1/fase2', async () => {
    const doc = cr({ formasPago: [] }); // movimientosDe(cr) devuelve [] (sin fallback root)
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery([doc], {}));

    const res = await getIndicadoresSolicitudesCobro({});

    expect(res.sinMovimientoVinculado).toBe(1);
    expect(res.total.count).toBe(1);
    expect(res.fase1Banco.count).toBe(0);
    expect(res.fase2Contador.count).toBe(0);
    // El contador que "resolvió" esta solicitud no debe aparecer en porUsuario — no hay
    // fase2 que promediar para él a partir de este único registro sin movimiento.
    expect(res.porUsuario).toEqual([]);
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
    expect(res.porUsuario[0].promedioHoras).toBeCloseTo(5 / 60, 3);
  });

  test('porUsuario agrupa por resueltoPorUserId y ordena por count desc', async () => {
    const docs = [
      cr({ _id: 'cr-1', resueltoPorUserId: 'u1', resueltoPorNombre: 'Ana' }),
      cr({ _id: 'cr-2', resueltoPorUserId: 'u2', resueltoPorNombre: 'Beto' }),
      cr({ _id: 'cr-3', resueltoPorUserId: 'u1', resueltoPorNombre: 'Ana' }),
    ];
    const movimientosPorId = { 'mov-1': mov('mov-1', '2026-08-10T15:00:00Z') };
    CollectionRequest.find.mockReturnValue(mockPopulatingQuery(docs, movimientosPorId));

    const res = await getIndicadoresSolicitudesCobro({});

    expect(res.porUsuario).toHaveLength(2);
    expect(res.porUsuario[0]).toMatchObject({ userId: 'u1', nombre: 'Ana', count: 2 });
    expect(res.porUsuario[1]).toMatchObject({ userId: 'u2', nombre: 'Beto', count: 1 });
  });
});
