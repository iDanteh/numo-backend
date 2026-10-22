'use strict';

// bank-indicadores.service.corte.test.js — getCorteConciliacion() (2026-10-02, control
// periódico de conciliación: rezagados / nuevos del periodo / identificados en el periodo).
// Mismas dependencias mockeadas que bank-indicadores.service.test.js (requiere bank.service.js
// para reusar _rangoAnioMesMexico/_inicioDiaMx/_finDiaMx).
jest.mock('./BankMovement.model');
jest.mock('../../shared/socket');
jest.mock('./drive-fichas.service');
jest.mock('../../../shared/services/global-config.service');
// Scope por rol (2026-10-09): getCorteConciliacion consulta Postgres solo cuando se pasa
// `rolEquipo` — mismo patrón/mock que bank.routes.test.js.
jest.mock('../../../shared/models/postgres', () => ({
  User: { findAll: jest.fn() },
}));

const ExcelJS = require('exceljs');
const BankMovement = require('./BankMovement.model');
const { User: PgUser } = require('../../../shared/models/postgres');
const globalConfigService = require('../../../shared/services/global-config.service');
const { BadRequestError } = require('../../shared/errors/AppError');
const {
  getCorteConciliacion, getCorteTendencia, buildReporteCorte, getPeriodoCortePorRol,
  _folioFechaCodificada, _mesesDeAtraso, _calcularCxcRetrasadas,
} = require('./bank-indicadores.service');

// "Ahora" fijo para que los tests sean deterministas: miércoles 2026-10-07, 15:00 México
// (21:00 UTC). Semana en curso → lunes 2026-10-05 00:00 México. Mes en curso → 2026-10-01
// 00:00 México.
const AHORA_MX = new Date(Date.UTC(2026, 9, 7, 21, 0, 0));
const INICIO_SEMANA_ESPERADO = new Date(Date.UTC(2026, 9, 5, 6, 0, 0));
const INICIO_MES_ESPERADO    = new Date(Date.UTC(2026, 9, 1, 6, 0, 0));

beforeEach(() => {
  jest.clearAllMocks();
  // Solo se mockea Date — dejar los timers reales (setTimeout/setImmediate/nextTick) sin
  // fakear: ExcelJS (buildReporteCorte) los usa internamente para escribir el .xlsx, y con
  // fake timers completos esa promesa nunca resuelve (timeout del test, no un bug real).
  jest.useFakeTimers({
    doNotFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'setImmediate', 'clearImmediate', 'nextTick', 'hrtime', 'queueMicrotask', 'performance'],
  }).setSystemTime(AHORA_MX);
});

afterEach(() => {
  jest.useRealTimers();
});

// `nuevosIdentificadoCount` (2026-10-09): getCorteConciliacion ahora saca el 'identificado' de
// `nuevos` vía un `countDocuments()` APARTE (acotable por rol), no del aggregate por status.
// Sin pasarlo explícito, default = el mismo count que ya viene en `nuevos` (_id:'identificado')
// — mantiene el comportamiento de siempre para los tests que no les importa el scope por rol.
function mockAggregates({ rezagados = [], nuevos = [], identificados = [], nuevosIdentificadoCount = null, pendientesConErp = [] } = {}) {
  BankMovement.aggregate
    .mockResolvedValueOnce(rezagados)
    .mockResolvedValueOnce(nuevos)
    .mockResolvedValueOnce(identificados);
  const defaultCount = nuevos.find((r) => r._id === 'identificado')?.count ?? 0;
  BankMovement.countDocuments = jest.fn().mockResolvedValue(nuevosIdentificadoCount ?? defaultCount);
  // CxC retrasadas (2026-10-22): el modo tiempo real hace UN find().select().lean() más, para
  // los pendientes con erpLinks — encolado con mockReturnValueOnce (no mockReturnValue) para
  // no pisar los find() propios de buildReporteCorte()/mockFinds(), que se encolan DESPUÉS de
  // este en los tests que combinan ambos helpers.
  const leanErp   = jest.fn().mockResolvedValue(pendientesConErp);
  const selectErp = jest.fn().mockReturnValue({ lean: leanErp });
  BankMovement.find.mockReturnValueOnce({ select: selectErp });
}

// buildReporteCorte() llama primero getCorteConciliacion() (3 aggregate) y DESPUÉS
// BankMovement.find().select().lean() dos veces (antesDelCorte, nuevos) — los mocks de
// find deben encolarse en ESE orden.
function mockFinds(antesDelCorteDocs, nuevosDocs) {
  const leanAntes   = jest.fn().mockResolvedValue(antesDelCorteDocs);
  const selectAntes = jest.fn().mockReturnValue({ lean: leanAntes });
  const leanNuevos   = jest.fn().mockResolvedValue(nuevosDocs);
  const selectNuevos = jest.fn().mockReturnValue({ lean: leanNuevos });
  BankMovement.find
    .mockReturnValueOnce({ select: selectAntes })
    .mockReturnValueOnce({ select: selectNuevos });
}

// Modo histórico (_getCorteHistorico): Promise.all([find(rezagados), find(nuevos),
// aggregate(identificadosPorOrigen)]) — los 2 find().select().lean() deben encolarse en ESE
// orden, el aggregate se mockea aparte con mockResolvedValueOnce.
function mockFindsHistorico(rezagadosDocs, nuevosDocs) {
  const leanRez   = jest.fn().mockResolvedValue(rezagadosDocs);
  const selectRez = jest.fn().mockReturnValue({ lean: leanRez });
  const leanNue   = jest.fn().mockResolvedValue(nuevosDocs);
  const selectNue = jest.fn().mockReturnValue({ lean: leanNue });
  BankMovement.find
    .mockReturnValueOnce({ select: selectRez })
    .mockReturnValueOnce({ select: selectNue });
}

async function leerWorkbook(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  return wb;
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

  // 2026-10-09 — pedido explícito del usuario: el corte mezclaba identificaciones de
  // CUALQUIER rol (si el corte era de cobranza, igual contaba lo identificado por
  // contabilidad). `rolEquipo` acota 'identificado' al rol dado — total/otros/pendientes NO
  // cambian (siguen siendo el real, sin scope).
  describe('scope por rol (rolEquipo)', () => {
    test('sin rolEquipo: no consulta Postgres, comportamiento idéntico a siempre', async () => {
      mockAggregates({ nuevos: [{ _id: 'identificado', count: 30 }] });
      const { nuevos } = await getCorteConciliacion({});

      expect(PgUser.findAll).not.toHaveBeenCalled();
      expect(nuevos.identificado).toBe(30);
    });

    test('con rolEquipo: nuevos.identificado se acota al count scoped, total/otros usan el real sin acotar', async () => {
      PgUser.findAll.mockResolvedValue([{ auth0Sub: 'cobranza-1' }]);
      mockAggregates({
        nuevos: [
          { _id: 'no_identificado', count: 10 },
          { _id: 'identificado',    count: 30 }, // real, de CUALQUIER rol
        ],
        nuevosIdentificadoCount: 18, // scoped: solo lo que identificó el equipo 'cobranza'
      });

      const { nuevos } = await getCorteConciliacion({ rolEquipo: 'cobranza' });

      expect(PgUser.findAll).toHaveBeenCalledWith({ where: { role: 'cobranza' }, attributes: ['auth0Sub'], raw: true });
      expect(nuevos.identificado).toBe(18);   // acotado
      expect(nuevos.no_identificado).toBe(10); // sin cambios
      expect(nuevos.total).toBe(40);           // sigue siendo el real (10 + 30), no 10+18
      expect(nuevos.pendientes).toBe(10);
    });

    test('con rolEquipo: el $match de identificadosEnPeriodo incluye primeraIdentificacionPor.userId del equipo', async () => {
      PgUser.findAll.mockResolvedValue([{ auth0Sub: 'cobranza-1' }, { auth0Sub: 'cobranza-2' }]);
      mockAggregates({});

      await getCorteConciliacion({ rolEquipo: 'cobranza' });

      const identificadosCall = BankMovement.aggregate.mock.calls[2]; // 3er aggregate: identificadosPorOrigen
      expect(identificadosCall[0][0].$match['primeraIdentificacionPor.userId']).toEqual({ $in: ['cobranza-1', 'cobranza-2'] });
    });

    test('con rolEquipo y equipo vacío: usa un sentinel que no matchea nada, NUNCA deja el filtro vacío (evita "ver todo")', async () => {
      PgUser.findAll.mockResolvedValue([]);
      mockAggregates({});

      await getCorteConciliacion({ rolEquipo: 'cobranza' });

      const identificadosCall = BankMovement.aggregate.mock.calls[2];
      const filtro = identificadosCall[0][0].$match['primeraIdentificacionPor.userId'].$in;
      expect(Array.isArray(filtro)).toBe(true);
      expect(filtro.length).toBeGreaterThan(0);
      expect(filtro).not.toEqual(['cobranza-1']); // no es un id real, es el sentinel
    });
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

describe('getCorteConciliacion — corte histórico (fechaInicio/fechaFin)', () => {
  // Rango histórico fijo para todos los casos: semana lunes 2026-09-14 a domingo 2026-09-20
  // (hora de México) — bien antes de AHORA_MX (miércoles 2026-10-07), así que no se confunde
  // con el periodo en curso.
  const FECHA_INICIO = '2026-09-14';
  const FECHA_FIN    = '2026-09-20';
  const INICIO_ESPERADO = new Date(Date.UTC(2026, 8, 14, 6, 0, 0));
  const FIN_ESPERADO    = new Date(Date.UTC(2026, 8, 21, 5, 59, 59, 999));

  test('(a) identificado antes de fin, sin reversión: cuenta como identificado', async () => {
    mockFindsHistorico([], [{
      _id: 'm1', status: 'identificado',
      primeraIdentificacionAt: new Date(Date.UTC(2026, 8, 16, 12, 0, 0)),
      ultimoCambioStatusAt: null,
      historialVinculacion: [],
    }]);
    BankMovement.aggregate.mockResolvedValueOnce([]); // identificadosPorOrigen

    const result = await getCorteConciliacion({ fechaInicio: FECHA_INICIO, fechaFin: FECHA_FIN });

    expect(result.historico).toBe(true);
    expect(result.inicio).toEqual(INICIO_ESPERADO);
    expect(result.fin).toEqual(FIN_ESPERADO);
    expect(result.nuevos.identificado).toBe(1);
    expect(result.nuevos.no_identificado).toBe(0);
    expect(result.advertencia).toBeUndefined();
  });

  test('(b) identificado pero con historialVinculacion "desvinculado" antes del cierre: cuenta como pendiente, NO identificado', async () => {
    const pid = new Date(Date.UTC(2026, 8, 16, 12, 0, 0));
    mockFindsHistorico([], [{
      _id: 'm2', status: 'no_identificado', // ya refleja la reversión real
      primeraIdentificacionAt: pid,
      ultimoCambioStatusAt: null,
      historialVinculacion: [
        { at: new Date(Date.UTC(2026, 8, 17, 9, 0, 0)), accion: 'desvinculado', erpId: 'CXC-1', origen: 'manual' },
      ],
    }]);
    BankMovement.aggregate.mockResolvedValueOnce([]);

    const result = await getCorteConciliacion({ fechaInicio: FECHA_INICIO, fechaFin: FECHA_FIN });

    expect(result.nuevos.identificado).toBe(0);
    expect(result.nuevos.no_identificado).toBe(1);
    expect(result.nuevos.pendientes).toBe(1);
  });

  test('(c) ultimoCambioStatusAt posterior a fin: aparece en advertencia.registrosConCambioPosteriorAlCierre', async () => {
    mockFindsHistorico([{
      _id: 'm3', status: 'no_identificado',
      primeraIdentificacionAt: null,
      ultimoCambioStatusAt: new Date(Date.UTC(2026, 9, 1, 0, 0, 0)), // después de FIN_ESPERADO
      historialVinculacion: [],
    }], []);
    BankMovement.aggregate.mockResolvedValueOnce([]);

    const result = await getCorteConciliacion({ fechaInicio: FECHA_INICIO, fechaFin: FECHA_FIN });

    expect(result.advertencia).toEqual({ registrosConCambioPosteriorAlCierre: 1 });
  });

  // 2026-10-09: mismo criterio de scope que la rama live, acá reconstruido en el loop JS
  // (vía primeraIdentificacionPor.userId) en vez de un $match de Mongo.
  test('(f) con rolEquipo: identificado por OTRO rol cuenta en total pero NO en el bucket identificado', async () => {
    PgUser.findAll.mockResolvedValue([{ auth0Sub: 'cobranza-1' }]);
    mockFindsHistorico([], [
      { _id: 'm1', status: 'identificado', primeraIdentificacionAt: new Date(Date.UTC(2026, 8, 16, 12, 0, 0)),
        primeraIdentificacionPor: { userId: 'cobranza-1' }, ultimoCambioStatusAt: null, historialVinculacion: [] },
      { _id: 'm2', status: 'identificado', primeraIdentificacionAt: new Date(Date.UTC(2026, 8, 16, 12, 0, 0)),
        primeraIdentificacionPor: { userId: 'contabilidad-1' }, ultimoCambioStatusAt: null, historialVinculacion: [] },
    ]);
    BankMovement.aggregate.mockResolvedValueOnce([]); // identificadosPorOrigen

    const result = await getCorteConciliacion({ fechaInicio: FECHA_INICIO, fechaFin: FECHA_FIN, rolEquipo: 'cobranza' });

    expect(result.nuevos.identificado).toBe(1);        // solo el de cobranza-1
    expect(result.nuevos.total).toBe(2);                // ambos siguen contando en el total real
    expect(result.nuevos.no_identificado).toBe(0);      // el de contabilidad-1 NO cae a pendiente
  });

  test('(d) fechaFin anterior a fechaInicio: BadRequestError', async () => {
    await expect(getCorteConciliacion({ fechaInicio: '2026-09-20', fechaFin: '2026-09-14' }))
      .rejects.toThrow(BadRequestError);
  });

  test('(e) sin fechaInicio/fechaFin: sigue devolviendo historico:false y el comportamiento de siempre', async () => {
    mockAggregates({});
    const result = await getCorteConciliacion({});

    expect(result.historico).toBe(false);
    expect(result.fin).toBeUndefined();
    // Desde 2026-10-22 el modo tiempo real SÍ llama a find() una vez — para los pendientes
    // con erpLinks que alimentan cxcRetrasadas (ver mockAggregates) — a diferencia de antes,
    // cuando este find() no existía todavía.
    expect(BankMovement.find).toHaveBeenCalledTimes(1);
  });
});

describe('buildReporteCorte', () => {
  test('hoja "Resumen": refleja exactamente los mismos números que getCorteConciliacion()', async () => {
    mockAggregates({
      rezagados: [{ _id: 'no_identificado', count: 2 }],
      nuevos:    [{ _id: 'identificado', count: 7 }],
      identificados: [{ _id: 'nuevo', count: 7 }],
    });
    mockFinds([], []);

    const buffer = await buildReporteCorte({ periodo: 'semanal' });
    const wb = await leerWorkbook(buffer);
    const resumen = wb.getWorksheet('Resumen');

    const filas = [];
    resumen.eachRow((row) => filas.push([row.getCell(1).value, row.getCell(2).value]));

    expect(filas).toEqual(expect.arrayContaining([
      ['No identificados iniciales — total', 2],
      ['Nuevos del periodo — total', 7],
      ['Identificados en el periodo — total', 7],
    ]));
  });

  test('hoja "Detalle": una fila por movimiento, con Origen correcto (Rezagado/Nuevo)', async () => {
    mockAggregates({});
    mockFinds(
      [{ banco: 'BBVA', fecha: new Date('2026-09-20'), concepto: 'Viejo', deposito: 100, categoria: null, status: 'no_identificado' }],
      [{ banco: 'BBVA', fecha: new Date('2026-10-06'), concepto: 'Nuevo', deposito: 200, categoria: null, status: 'no_identificado' }],
    );

    const buffer = await buildReporteCorte({});
    const wb = await leerWorkbook(buffer);
    const detalle = wb.getWorksheet('Detalle');

    expect(detalle.getRow(2).getCell(1).value).toBe('Rezagado'); // fila 1 = header
    expect(detalle.getRow(2).getCell(4).value).toBe('Viejo');
    expect(detalle.getRow(3).getCell(1).value).toBe('Nuevo');
    expect(detalle.getRow(3).getCell(4).value).toBe('Nuevo');
  });

  test('"Identificado en el periodo" = Sí solo si status=identificado Y primeraIdentificacionAt >= inicio', async () => {
    mockAggregates({});
    mockFinds([], [
      // Identificado DENTRO del periodo → Sí
      { banco: 'BBVA', fecha: new Date('2026-10-06'), concepto: 'A', deposito: 1, status: 'identificado', primeraIdentificacionAt: new Date('2026-10-06T12:00:00Z') },
      // Status identificado pero sin fecha de identificación (dato inconsistente) → No, no explota
      { banco: 'BBVA', fecha: new Date('2026-10-06'), concepto: 'B', deposito: 1, status: 'identificado', primeraIdentificacionAt: null },
      // Todavía no identificado → No
      { banco: 'BBVA', fecha: new Date('2026-10-06'), concepto: 'C', deposito: 1, status: 'no_identificado' },
    ]);

    const buffer = await buildReporteCorte({});
    const wb = await leerWorkbook(buffer);
    const detalle = wb.getWorksheet('Detalle');

    expect(detalle.getRow(2).getCell(8).value).toBe('Sí');
    expect(detalle.getRow(3).getCell(8).value).toBe('No');
    expect(detalle.getRow(4).getCell(8).value).toBe('No');
  });

  // 2026-10-09: invariante documentado en el JSDoc de buildReporteCorte — la suma de "Sí" debe
  // coincidir con identificadosEnPeriodo.total, así que la columna respeta el mismo rolEquipo.
  test('"Identificado en el periodo" respeta rolEquipo: No para quien identificó alguien de otro rol', async () => {
    PgUser.findAll.mockResolvedValue([{ auth0Sub: 'cobranza-1' }]);
    mockAggregates({});
    mockFinds([], [
      { banco: 'BBVA', fecha: new Date('2026-10-06'), concepto: 'A', deposito: 1, status: 'identificado',
        primeraIdentificacionAt: new Date('2026-10-06T12:00:00Z'), primeraIdentificacionPor: { userId: 'cobranza-1' } },
      { banco: 'BBVA', fecha: new Date('2026-10-06'), concepto: 'B', deposito: 1, status: 'identificado',
        primeraIdentificacionAt: new Date('2026-10-06T12:00:00Z'), primeraIdentificacionPor: { userId: 'contabilidad-1' } },
    ]);

    const buffer = await buildReporteCorte({ rolEquipo: 'cobranza' });
    const wb = await leerWorkbook(buffer);
    const detalle = wb.getWorksheet('Detalle');

    expect(detalle.getRow(2).getCell(8).value).toBe('Sí'); // cobranza-1
    expect(detalle.getRow(3).getCell(8).value).toBe('No'); // contabilidad-1
  });
});

describe('getPeriodoCortePorRol', () => {
  test('cobranza: lee CORTE_PERIODO_COBRANZA de Configuraciones Globales, no puede alternar', async () => {
    globalConfigService.getValue.mockResolvedValue('semanal');
    const result = await getPeriodoCortePorRol('cobranza');

    expect(result).toEqual({ periodo: 'semanal', puedeAlternar: false });
    expect(globalConfigService.getValue).toHaveBeenCalledWith('bancos', 'CORTE_PERIODO_COBRANZA');
  });

  test('contabilidad: lee CORTE_PERIODO_CONTABILIDAD, no puede alternar', async () => {
    globalConfigService.getValue.mockResolvedValue('mensual');
    const result = await getPeriodoCortePorRol('contabilidad');

    expect(result).toEqual({ periodo: 'mensual', puedeAlternar: false });
    expect(globalConfigService.getValue).toHaveBeenCalledWith('bancos', 'CORTE_PERIODO_CONTABILIDAD');
  });

  test('config todavía no sembrada: cae al default anterior (cobranza=semanal)', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error("No existe la configuración 'bancos.CORTE_PERIODO_COBRANZA' — cargala desde Configuraciones Globales."));
    const result = await getPeriodoCortePorRol('cobranza');

    expect(result).toEqual({ periodo: 'semanal', puedeAlternar: false });
  });

  test('config todavía no sembrada: cae al default anterior (contabilidad=mensual)', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error("No existe la configuración 'bancos.CORTE_PERIODO_CONTABILIDAD' — cargala desde Configuraciones Globales."));
    const result = await getPeriodoCortePorRol('contabilidad');

    expect(result).toEqual({ periodo: 'mensual', puedeAlternar: false });
  });

  test('error real (no de "no existe") se propaga, no se confunde con config faltante', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error('CONFIG_MASTER_KEY no está definida'));
    await expect(getPeriodoCortePorRol('cobranza')).rejects.toThrow('CONFIG_MASTER_KEY no está definida');
  });

  test('otro rol (ej. admin): semanal por default, puede alternar, sin leer configuración', async () => {
    const result = await getPeriodoCortePorRol('admin');

    expect(result).toEqual({ periodo: 'semanal', puedeAlternar: true });
    expect(globalConfigService.getValue).not.toHaveBeenCalled();
  });
});

// "CxC retrasadas" (2026-10-22) — folio AAMMNNNNNN, CxC vinculada a un depósito pendiente cuyo
// folio es de un mes calendario anterior al de referencia.
describe('_folioFechaCodificada', () => {
  test('folio válido: decodifica año (20 + AA) y mes', () => {
    expect(_folioFechaCodificada('260901542')).toEqual({ anio: 2026, mes: 9 });
  });

  test('mes inválido (00 o 13+): null', () => {
    expect(_folioFechaCodificada('260013000')).toBeNull();
    expect(_folioFechaCodificada('261300000')).toBeNull();
  });

  test('formato no numérico o muy corto: null (ej. anticipos OPA)', () => {
    expect(_folioFechaCodificada('OPA-123')).toBeNull();
    expect(_folioFechaCodificada('1234')).toBeNull();
    expect(_folioFechaCodificada(null)).toBeNull();
  });
});

describe('_mesesDeAtraso', () => {
  test('misma anio/mes: 0', () => {
    expect(_mesesDeAtraso({ anio: 2026, mes: 10 }, { anio: 2026, mes: 10 })).toBe(0);
  });

  test('folio de un mes antes: 1', () => {
    expect(_mesesDeAtraso({ anio: 2026, mes: 9 }, { anio: 2026, mes: 10 })).toBe(1);
  });

  test('cruza el límite de año: cuenta meses calendario, no solo la diferencia de "mes"', () => {
    expect(_mesesDeAtraso({ anio: 2025, mes: 12 }, { anio: 2026, mes: 2 })).toBe(2);
  });
});

describe('_calcularCxcRetrasadas', () => {
  const REFERENCIA = { anio: 2026, mes: 10 };

  test('folio de mes anterior y depósito pendiente: cuenta como retrasada', () => {
    const { count, detalle } = _calcularCxcRetrasadas([
      { _id: 'm1', banco: 'BBVA', fecha: new Date('2026-09-15'), erpLinks: [{ serie: 'A0', folioExterno: '260901542' }] },
    ], REFERENCIA);

    expect(count).toBe(1);
    expect(detalle[0]).toMatchObject({ movimientoId: 'm1', serie: 'A0', folioExterno: '260901542', mesFolio: '2026-09', mesesAtraso: 1 });
  });

  test('folio del mismo mes de referencia: NO cuenta (mesesAtraso <= 0 se descarta)', () => {
    const { count } = _calcularCxcRetrasadas([
      { _id: 'm1', erpLinks: [{ serie: 'A0', folioExterno: '261000266' }] },
    ], REFERENCIA);
    expect(count).toBe(0);
  });

  test('folio sin formato reconocible: se ignora, no explota', () => {
    const { count } = _calcularCxcRetrasadas([
      { _id: 'm1', erpLinks: [{ serie: 'OPA', folioExterno: 'OPA-1' }] },
    ], REFERENCIA);
    expect(count).toBe(0);
  });

  test('incluye folio Numo, importe y fecha del depósito (no datos de la ficha)', () => {
    const { detalle } = _calcularCxcRetrasadas([
      {
        _id: 'm1', fecha: new Date('2026-03-04'), folio: '020231', deposito: 170.97,
        erpLinks: [{ serie: 'E0', folioExterno: '260203714' }],
      },
    ], REFERENCIA);
    expect(detalle[0]).toMatchObject({ folioNumo: '020231', importe: 170.97, fechaDeposito: new Date('2026-03-04') });
    expect(detalle[0].ficha).toBeUndefined();
  });

  test('sin folio/depósito en el movimiento: quedan null, no explota', () => {
    const { detalle } = _calcularCxcRetrasadas([
      { _id: 'm1', erpLinks: [{ serie: 'A0', folioExterno: '260901542' }] },
    ], REFERENCIA);
    expect(detalle[0]).toMatchObject({ folioNumo: null, importe: null, fechaDeposito: null });
  });

  test('count siempre refleja el total real aunque el detalle se acote a 100', () => {
    const movimientos = Array.from({ length: 120 }, (_, i) => ({
      _id: `m${i}`, erpLinks: [{ serie: 'A0', folioExterno: '260101000' }], // 2026-01, 9 meses de atraso
    }));
    const { count, detalle } = _calcularCxcRetrasadas(movimientos, REFERENCIA);
    expect(count).toBe(120);
    expect(detalle).toHaveLength(100);
  });
});

describe('getCorteConciliacion — cxcRetrasadas', () => {
  test('modo tiempo real: referencia es el mes/año de HOY (mockeado a 2026-10-07)', async () => {
    mockAggregates({
      pendientesConErp: [
        { _id: 'm1', banco: 'BBVA', fecha: new Date('2026-09-15'), erpLinks: [{ serie: 'A0', folioExterno: '260901542' }] },
        { _id: 'm2', banco: 'BBVA', fecha: new Date('2026-10-05'), erpLinks: [{ serie: 'A0', folioExterno: '261000266' }] },
      ],
    });

    const { cxcRetrasadas } = await getCorteConciliacion({});

    expect(cxcRetrasadas.count).toBe(1);
    expect(cxcRetrasadas.detalle[0].movimientoId).toBe('m1');
  });

  test('modo histórico: referencia es el mes/año del cierre (fechaFin), no el de hoy', async () => {
    mockFindsHistorico([], [
      // Pendiente a cierre (2026-09-20): folio de agosto 2026 → 1 mes de atraso respecto al
      // cierre, aunque respecto a HOY (mockeado a octubre) sería 2.
      {
        _id: 'm1', status: 'no_identificado', primeraIdentificacionAt: null, ultimoCambioStatusAt: null,
        historialVinculacion: [], erpLinks: [{ serie: 'A0', folioExterno: '260801000' }],
      },
    ]);
    BankMovement.aggregate.mockResolvedValueOnce([]); // identificadosPorOrigen

    const { cxcRetrasadas } = await getCorteConciliacion({ fechaInicio: '2026-09-14', fechaFin: '2026-09-20' });

    expect(cxcRetrasadas.count).toBe(1);
    expect(cxcRetrasadas.detalle[0].mesesAtraso).toBe(1); // sept(09) - ago(08), no oct(10) - ago(08)
  });
});

describe('getCorteTendencia', () => {
  test('puntos:1 — un solo punto, SIEMPRE el periodo en curso (esActual:true)', async () => {
    mockAggregates({
      rezagados: [{ _id: 'no_identificado', count: 5 }],
      nuevos:    [{ _id: 'no_identificado', count: 3 }],
      pendientesConErp: [
        { _id: 'm1', erpLinks: [{ serie: 'A0', folioExterno: '260901542' }] },
      ],
    });

    const result = await getCorteTendencia({ periodo: 'semanal', puntos: 1 });

    expect(result.periodo).toBe('semanal');
    expect(result.puntos).toHaveLength(1);
    expect(result.puntos[0]).toMatchObject({
      esActual: true,
      rezagadosTotal: 5,
      rezagadosNoIdentificado: 5,
      rezagadosReclasificado: 0,
      nuevosPendientes: 3,
      nuevosNoIdentificado: 3,
      nuevosReclasificado: 0,
      nuevosIdentificado: 0,
      cxcRetrasadasCount: 1,
    });
  });

  // Desglose por status (2026-10-22, switch "Ver por estatus" del gráfico de tendencia):
  // además de los totales, cada punto expone no_identificado/reclasificado/identificado por
  // separado — y la suma debe cuadrar EXACTO con los totales de siempre (rezagadosTotal,
  // nuevosPendientes), nunca inflarse ni divergir.
  test('desglose por status — suma exacta de rezagados y de nuevos.pendientes', async () => {
    mockAggregates({
      rezagados: [
        { _id: 'no_identificado', count: 7 },
        { _id: 'reclasificado',   count: 2 },
      ],
      nuevos: [
        { _id: 'no_identificado', count: 4 },
        { _id: 'reclasificado',   count: 1 },
        { _id: 'identificado',    count: 9 },
      ],
    });

    const result = await getCorteTendencia({ periodo: 'semanal', puntos: 1 });
    const punto = result.puntos[0];

    expect(punto.rezagadosNoIdentificado).toBe(7);
    expect(punto.rezagadosReclasificado).toBe(2);
    expect(punto.rezagadosTotal).toBe(9);
    expect(punto.rezagadosNoIdentificado + punto.rezagadosReclasificado).toBe(punto.rezagadosTotal);

    expect(punto.nuevosNoIdentificado).toBe(4);
    expect(punto.nuevosReclasificado).toBe(1);
    expect(punto.nuevosPendientes).toBe(5);
    expect(punto.nuevosNoIdentificado + punto.nuevosReclasificado).toBe(punto.nuevosPendientes);

    // `nuevosIdentificado` es informativo aparte — NO debe sumarse a nuevosPendientes.
    expect(punto.nuevosIdentificado).toBe(9);
    expect(punto.nuevosNoIdentificado + punto.nuevosReclasificado + punto.nuevosIdentificado).not.toBe(punto.nuevosPendientes);
  });

  test('periodo inválido cae a "semanal" (mismo criterio que getCorteConciliacion)', async () => {
    mockAggregates({});
    const result = await getCorteTendencia({ periodo: 'quincenal', puntos: 1 });
    expect(result.periodo).toBe('semanal');
  });
});

// Reconciliación panel/Excel (2026-10-22, pedido explícito del usuario: "revisa que las sumas
// cuadren tanto en la vista como en el reporte que se descarga en excel") — buildReporteCorte()
// reusa getCorteConciliacion() para su hoja "Resumen" (ver JSDoc de buildReporteCorte), así que
// panel y Excel comparten la MISMA fuente de datos; este test lo deja explícito en vez de
// asumirlo, llamando a ambas funciones sobre el mismo mock y comparando los números.
describe('getCorteConciliacion / buildReporteCorte — misma fuente, mismos números', () => {
  test('el resumen del Excel coincide exacto con el corte que ve el panel', async () => {
    mockAggregates({
      rezagados: [
        { _id: 'no_identificado', count: 7 },
        { _id: 'reclasificado',   count: 2 },
      ],
      nuevos: [
        { _id: 'no_identificado', count: 4 },
        { _id: 'reclasificado',   count: 1 },
        { _id: 'identificado',    count: 9 },
      ],
      identificados: [{ _id: 'rezagado', count: 2 }, { _id: 'nuevo', count: 3 }],
    });
    const corte = await getCorteConciliacion({ periodo: 'semanal' });

    mockAggregates({
      rezagados: [
        { _id: 'no_identificado', count: 7 },
        { _id: 'reclasificado',   count: 2 },
      ],
      nuevos: [
        { _id: 'no_identificado', count: 4 },
        { _id: 'reclasificado',   count: 1 },
        { _id: 'identificado',    count: 9 },
      ],
      identificados: [{ _id: 'rezagado', count: 2 }, { _id: 'nuevo', count: 3 }],
    });
    mockFinds([], []);
    const buffer = await buildReporteCorte({ periodo: 'semanal' });
    const wb = await leerWorkbook(buffer);
    const resumen = wb.getWorksheet('Resumen');
    const valorDe = (etiqueta) => resumen.getRows(1, resumen.rowCount)
      .find(r => r.getCell('A').value === etiqueta)?.getCell('B').value;

    expect(valorDe('No identificados iniciales — total')).toBe(corte.rezagados.total);
    expect(valorDe('Nuevos del periodo — total')).toBe(corte.nuevos.total);
    expect(valorDe('Identificados en el periodo — total')).toBe(corte.identificadosEnPeriodo.total);
  });
});
