'use strict';

// bank-indicadores.service.corte.test.js — getCorteConciliacion() (2026-10-02, control
// periódico de conciliación: rezagados / nuevos del periodo / identificados en el periodo).
// Mismas dependencias mockeadas que bank-indicadores.service.test.js (requiere bank.service.js
// para reusar _rangoAnioMesMexico/_inicioDiaMx/_finDiaMx).
jest.mock('./BankMovement.model');
jest.mock('./CorteDescarga.model');
jest.mock('../../shared/socket');
jest.mock('./drive-fichas.service');
jest.mock('./drive-cortes.service');
jest.mock('../../../shared/services/global-config.service');
// Scope por rol (2026-10-09): getCorteConciliacion consulta Postgres solo cuando se pasa
// `rolEquipo` — mismo patrón/mock que bank.routes.test.js.
jest.mock('../../../shared/models/postgres', () => ({
  User: { findAll: jest.fn() },
}));

const ExcelJS = require('exceljs');
const BankMovement = require('./BankMovement.model');
const CorteDescarga = require('./CorteDescarga.model');
const { subirReporteCorte, descargarReporteCorte } = require('./drive-cortes.service');
const { User: PgUser } = require('../../../shared/models/postgres');
const globalConfigService = require('../../../shared/services/global-config.service');
const { BadRequestError, NotFoundError } = require('../../shared/errors/AppError');
const {
  getCorteConciliacion, buildReporteCorte, getPeriodoCortePorRol,
  getProgresoResolucionPorCorte, getLedgerDiario,
  _folioFechaCodificada, _mesesDeAtraso, _calcularCxcRetrasadas,
  _enumerarCortesSemanales, _calcularLedgerBacklog, _enumerarDiasEnRango,
  _resolverMesOrigen, _primeraIdentificacionEfectiva,
  getLedgerMensual, _enumerarMesesParaLedger, getLedgerPorHora, _enumerarHorasDelDia,
  getUltimosMovimientos,
  getAntiguedadDepositos, getAntiguedadDepositosPorAnio, _rangoAntiguedad,
  buildReporteCortes, listarDescargasCorte, descargarReporteCorteHistorico,
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

// `nuevosIdentificadoCount` (2026-10-09, actualizado 2026-10-22): getCorteConciliacion saca el
// 'identificado' de `nuevos` vía un aggregate APARTE con `$group:null` (acotable por rol) — ya
// no es un `countDocuments()` simple, porque ahora también suma `importeTotal` (ver
// `nuevosIdentificadoImporte`). Sin pasarlo explícito, default = el mismo count que ya viene en
// `nuevos` (_id:'identificado') — mantiene el comportamiento de siempre para los tests que no
// les importa el scope por rol.
function mockAggregates({
  rezagados = [], nuevos = [], identificados = [],
  nuevosIdentificadoCount = null, nuevosIdentificadoImporte = 0,
  pendientesConErp = [],
} = {}) {
  const defaultCount = nuevos.find((r) => r._id === 'identificado')?.count ?? 0;
  const count = nuevosIdentificadoCount ?? defaultCount;
  BankMovement.aggregate
    .mockResolvedValueOnce(rezagados)
    .mockResolvedValueOnce(nuevos)
    // nuevosIdentificadoEquipo — $group:null, una sola fila (o ninguna si count es 0, igual
    // que Mongo real: un $group sin documentos que matcheen no emite fila).
    .mockResolvedValueOnce(count > 0 ? [{ _id: null, count, importeTotal: nuevosIdentificadoImporte }] : [])
    .mockResolvedValueOnce(identificados);
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
        { _id: 'no_identificado', count: 12, importeTotal: 120000 },
        { _id: 'reclasificado',    count: 3,  importeTotal: 9000   },
      ],
    });
    const { rezagados } = await getCorteConciliacion({ periodo: 'semanal' });

    expect(rezagados).toEqual({
      no_identificado: 12, reclasificado: 3, total: 15,
      importeTotal: { no_identificado: 120000, reclasificado: 9000, total: 129000 },
    });
  });

  test('rezagados en cero (sin backlog) no explota — defaults a 0', async () => {
    mockAggregates({ rezagados: [] });
    const { rezagados } = await getCorteConciliacion({});
    expect(rezagados).toEqual({
      no_identificado: 0, reclasificado: 0, total: 0,
      importeTotal: { no_identificado: 0, reclasificado: 0, total: 0 },
    });
  });

  test('nuevos: desglosa por estatus del periodo, incluye pendientes/otros/total (conteo E importe)', async () => {
    mockAggregates({
      nuevos: [
        { _id: 'no_identificado', count: 20, importeTotal: 200000 },
        { _id: 'reclasificado',    count: 2,  importeTotal: 15000  },
        { _id: 'identificado',     count: 30, importeTotal: 400000 },
        { _id: 'otros',            count: 1,  importeTotal: 500    },
      ],
      // nuevosIdentificadoEquipo (sin rolEquipo acá, así que es el mismo 400000 sin acotar).
      nuevosIdentificadoImporte: 400000,
    });
    const { nuevos } = await getCorteConciliacion({});

    expect(nuevos).toEqual({
      no_identificado: 20,
      reclasificado:   2,
      identificado:    30,
      otros:           1,
      pendientes:      22, // no_identificado + reclasificado
      total:           53,
      importeTotal: {
        no_identificado: 200000,
        reclasificado:   15000,
        identificado:    400000,
        otros:           500,
        total:           615500, // 200000+15000+400000+500 — suma real, no inventada
      },
    });
  });

  test('identificadosEnPeriodo: separa origen rezagado vs nuevo, con total', async () => {
    mockAggregates({
      identificados: [
        { _id: 'rezagado', count: 5,  importeTotal: 50000 },
        { _id: 'nuevo',    count: 30, importeTotal: 310000 },
      ],
    });
    const { identificadosEnPeriodo } = await getCorteConciliacion({});

    expect(identificadosEnPeriodo).toEqual({
      deRezagados: 5, deNuevos: 30, total: 35,
      importeTotal: { deRezagados: 50000, deNuevos: 310000, total: 360000 },
    });
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

// Última semana YA CERRADA bajo AHORA_MX (miércoles 2026-10-07): semana en curso arranca el
// lunes 2026-10-05 (INICIO_SEMANA_ESPERADO) → el corte anterior arrancó el lunes 2026-09-28 y
// cerró el domingo 2026-10-04 a las 23:59:59.999 México.
const CIERRE_ESPERADO = new Date(INICIO_SEMANA_ESPERADO.getTime() - 1);
const INICIO_CORTE_ESPERADO = new Date(CIERRE_ESPERADO.getTime() - 7 * 86400000 + 1);

describe('_calcularLedgerBacklog (2026-10-30: ledger genérico, misma fórmula para día o semana)', () => {
  test('iniciales/nuevos/identificados/total — un movimiento ya resuelto ANTES de empezar el periodo queda totalmente excluido', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      // A: iniciales, nunca identificado — sigue pendiente.
      { deposito: 100, createdAt: new Date('2026-09-01T12:00:00.000Z'), primeraIdentificacionAt: null, historialVinculacion: [] },
      // B: iniciales, se identificó DENTRO del periodo.
      { deposito: 200, createdAt: new Date('2026-09-01T12:00:00.000Z'), primeraIdentificacionAt: new Date(INICIO_CORTE_ESPERADO.getTime() + 2 * 86400000), historialVinculacion: [] },
      // C: ya estaba identificado ANTES de empezar el periodo — excluido del todo (ni iniciales ni identificados).
      { deposito: 300, createdAt: new Date('2026-09-01T12:00:00.000Z'), primeraIdentificacionAt: new Date('2026-08-01T12:00:00.000Z'), historialVinculacion: [] },
      // D: nuevo del periodo, sigue pendiente.
      { deposito: 400, createdAt: new Date(INICIO_CORTE_ESPERADO.getTime() + 3 * 86400000), primeraIdentificacionAt: null, historialVinculacion: [] },
      // E: nuevo del periodo, se identificó dentro del mismo periodo.
      { deposito: 500, createdAt: new Date(INICIO_CORTE_ESPERADO.getTime() + 1 * 86400000), primeraIdentificacionAt: new Date(INICIO_CORTE_ESPERADO.getTime() + 4 * 86400000), historialVinculacion: [] },
    ]) }) });

    const ledger = await _calcularLedgerBacklog({ baseMatch: {}, periodoInicio: INICIO_CORTE_ESPERADO, periodoFin: CIERRE_ESPERADO });

    expect(ledger.iniciales).toEqual({ count: 2, saldo: 300 });     // A + B
    expect(ledger.nuevos).toEqual({ count: 2, saldo: 900 });        // D + E
    expect(ledger.identificados).toEqual({ count: 2, saldo: 700 }); // B + E
    expect(ledger.total).toEqual({ count: 2, saldo: 500 });         // A (100) + D (400), lo que sigue pendiente
    // Partido por origen (2026-10-30): B viene de iniciales, E viene de nuevos — nunca mezclados.
    expect(ledger.identificadosDeIniciales).toEqual({ count: 1, saldo: 200 }); // B
    expect(ledger.identificadosDeNuevos).toEqual({ count: 1, saldo: 500 });    // E
  });

  test('una identificación revertida ANTES del fin del periodo no cuenta como identificados — sigue pendiente', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      {
        deposito: 500, createdAt: new Date('2026-09-01T12:00:00.000Z'),
        primeraIdentificacionAt: new Date(INICIO_CORTE_ESPERADO.getTime() + 1 * 86400000),
        historialVinculacion: [{ accion: 'desvinculado', at: new Date(INICIO_CORTE_ESPERADO.getTime() + 3 * 86400000) }],
      },
    ]) }) });

    const ledger = await _calcularLedgerBacklog({ baseMatch: {}, periodoInicio: INICIO_CORTE_ESPERADO, periodoFin: CIERRE_ESPERADO });
    expect(ledger.iniciales).toEqual({ count: 1, saldo: 500 });
    expect(ledger.identificados).toEqual({ count: 0, saldo: 0 });
    expect(ledger.total).toEqual({ count: 1, saldo: 500 });
  });

  test('solo consulta status no_identificado/identificado — reclasificado/otros quedan fuera del query', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([]) }) });
    await _calcularLedgerBacklog({ baseMatch: {}, periodoInicio: INICIO_CORTE_ESPERADO, periodoFin: CIERRE_ESPERADO });
    const [filtro] = BankMovement.find.mock.calls[0];
    expect(filtro.status).toEqual({ $in: ['no_identificado', 'identificado'] });
    expect(filtro.createdAt).toEqual({ $lte: CIERRE_ESPERADO });
  });

  test('mesOrigenInicio/mesOrigenFin acota createdAt a ese mes exacto, combinado con el $lte de periodoFin', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([]) }) });
    const mesOrigenInicio = new Date(Date.UTC(2026, 7, 1, 6, 0, 0));
    const mesOrigenFin = new Date(Date.UTC(2026, 8, 1, 6, 0, 0));
    await _calcularLedgerBacklog({ baseMatch: {}, periodoInicio: INICIO_CORTE_ESPERADO, periodoFin: CIERRE_ESPERADO, mesOrigenInicio, mesOrigenFin });
    const [filtro] = BankMovement.find.mock.calls[0];
    expect(filtro.createdAt).toEqual({ $lte: CIERRE_ESPERADO, $gte: mesOrigenInicio, $lt: mesOrigenFin });
  });

  // Bug real 2026-11-06 (verificado contra una copia de producción): `primeraIdentificacionAt`
  // se agregó al schema el 2026-08-14 sin backfill — un movimiento identificado ANTES de esa
  // fecha nunca tiene el campo seteado, y sin el fallback de `identificadoPor[]` quedaba
  // "atrapado" como backlog pendiente para siempre, sin importar qué tan viejo fuera el
  // movimiento o cuántos periodos pasaran.
  test('bug real: movimiento identificado antes de que existiera primeraIdentificacionAt (null) se resuelve via identificadoPor[]', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      // Iniciales (createdAt de un mes anterior al periodo), primeraIdentificacionAt NUNCA se
      // seteó (identificado antes del 2026-08-14), pero identificadoPor[] sí tiene el historial
      // real: se identificó DENTRO del periodo consultado.
      {
        deposito: 700, createdAt: new Date('2026-05-01T12:00:00.000Z'),
        primeraIdentificacionAt: null, historialVinculacion: [],
        identificadoPor: [{ nombre: 'cristiangarcia', fechaId: new Date(INICIO_CORTE_ESPERADO.getTime() + 2 * 86400000) }],
      },
    ]) }) });

    const ledger = await _calcularLedgerBacklog({ baseMatch: {}, periodoInicio: INICIO_CORTE_ESPERADO, periodoFin: CIERRE_ESPERADO });

    expect(ledger.iniciales).toEqual({ count: 1, saldo: 700 });
    expect(ledger.identificados).toEqual({ count: 1, saldo: 700 }); // antes del fix: 0, atrapado para siempre
    expect(ledger.total).toEqual({ count: 0, saldo: 0 });
  });

  // Bug real 2026-11-10 (confirmado con 6 movimientos reales de producción, ver nota REAPARECIDOS
  // en el JSDoc de _calcularLedgerBacklog): un depósito YA resuelto antes de periodoInicio que se
  // DESVINCULA dentro del periodo y sigue sin resolver al periodoFin, antes de este fix quedaba
  // totalmente invisible para el `total` de este periodo (el mismo caso que el test C de arriba,
  // "ya estaba identificado ANTES de empezar el periodo"), rompiendo la garantía documentada
  // "total(A) == iniciales(B)": reaparecía de la nada como `iniciales` del periodo siguiente.
  test('reaparecido: resuelto antes del periodo, se revierte DENTRO del periodo y sigue sin resolver al cierre -> cuenta como nuevo, no se pierde', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      {
        deposito: 600, createdAt: new Date('2026-08-01T12:00:00.000Z'),
        primeraIdentificacionAt: new Date('2026-08-05T12:00:00.000Z'),
        identificadoPor: [],
        historialVinculacion: [
          { accion: 'desvinculado', at: new Date(INICIO_CORTE_ESPERADO.getTime() + 2 * 86400000) },
        ],
      },
    ]) }) });

    const ledger = await _calcularLedgerBacklog({ baseMatch: {}, periodoInicio: INICIO_CORTE_ESPERADO, periodoFin: CIERRE_ESPERADO });

    expect(ledger.iniciales).toEqual({ count: 0, saldo: 0 });        // nunca estuvo en el backlog al ARRANCAR el periodo
    expect(ledger.nuevos).toEqual({ count: 1, saldo: 600 });         // entra como "nuevo" (reaparecido) de este periodo
    expect(ledger.identificados).toEqual({ count: 0, saldo: 0 });    // sigue sin resolver al cierre
    expect(ledger.total).toEqual({ count: 1, saldo: 600 });          // antes del fix: 0 — invisible, reaparecía en el periodo siguiente
  });

  // Conservación end-to-end (la garantía documentada "total(A) == iniciales(B)"): el MISMO
  // movimiento reaparecido del test anterior sigue sin resolver al cierre del periodo — así que,
  // en el periodo SIGUIENTE, ya NO está "resuelto al arrancar" (identificadoAlInicio=false): entra
  // normal por la rama de `iniciales`, exactamente como cualquier otro backlog real. Antes del fix
  // no lo contaba NINGÚN periodo (invisible en el primero, pero el segundo igual lo levantaba
  // porque esa rama nunca cambió) — eso era justo la asimetría: aparecía de la nada. Ahora
  // total(periodo1)=1 y iniciales(periodo2)=1: EXACTAMENTE uno, nunca cero ni dos veces.
  test('conservación: el mismo movimiento reaparecido es el `total` de un periodo Y el `iniciales` del siguiente', async () => {
    const mov = {
      deposito: 600, createdAt: new Date('2026-08-01T12:00:00.000Z'),
      primeraIdentificacionAt: new Date('2026-08-05T12:00:00.000Z'),
      identificadoPor: [],
      historialVinculacion: [
        { accion: 'desvinculado', at: new Date(INICIO_CORTE_ESPERADO.getTime() + 2 * 86400000) },
      ],
    };
    const siguientePeriodoInicio = new Date(CIERRE_ESPERADO.getTime() + 1);
    const siguientePeriodoFin = new Date(siguientePeriodoInicio.getTime() + 7 * 86400000 - 1);

    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([mov]) }) });
    const ledgerSiguiente = await _calcularLedgerBacklog({ baseMatch: {}, periodoInicio: siguientePeriodoInicio, periodoFin: siguientePeriodoFin });

    expect(ledgerSiguiente.iniciales).toEqual({ count: 1, saldo: 600 }); // == total del periodo anterior (test de arriba)
    expect(ledgerSiguiente.nuevos).toEqual({ count: 0, saldo: 0 });
    expect(ledgerSiguiente.total).toEqual({ count: 1, saldo: 600 });
  });
});

describe('_primeraIdentificacionEfectiva (2026-11-06, bug real — fallback de primeraIdentificacionAt)', () => {
  test('si primeraIdentificacionAt existe, se usa tal cual (ignora identificadoPor)', () => {
    const fecha = new Date('2026-08-20T10:00:00.000Z');
    const mov = {
      primeraIdentificacionAt: fecha,
      identificadoPor: [{ nombre: 'katia', fechaId: new Date('2026-05-01T00:00:00.000Z') }],
    };
    expect(_primeraIdentificacionEfectiva(mov)).toBe(fecha);
  });

  test('sin primeraIdentificacionAt, usa el MÍNIMO de identificadoPor[].fechaId (incluye Motor ERP)', () => {
    const mov = {
      primeraIdentificacionAt: null,
      identificadoPor: [
        { nombre: 'Motor ERP', fechaId: new Date('2026-05-22T19:37:04.523Z') },
        { nombre: 'cristiangarcia', fechaId: new Date('2026-05-06T15:56:38.305Z') },
        { nombre: 'luisfelipe', fechaId: new Date('2026-05-18T16:44:09.703Z') },
      ],
    };
    expect(_primeraIdentificacionEfectiva(mov)).toEqual(new Date('2026-05-06T15:56:38.305Z'));
  });

  test('sin primeraIdentificacionAt ni identificadoPor (o vacío) -> null', () => {
    expect(_primeraIdentificacionEfectiva({ primeraIdentificacionAt: null, identificadoPor: [] })).toBeNull();
    expect(_primeraIdentificacionEfectiva({ primeraIdentificacionAt: null })).toBeNull();
  });

  // Bug real 2026-11-10: el fallback a `fichaAt` que ya tenía `_primeraResueltaEfectiva` (desde
  // 2026-11-06, para Antigüedad) nunca se había portado acá, así que el ledger de Mensual/Diario
  // seguía subestimando identificaciones hechas por comprobante físico.
  test('sin primeraIdentificacionAt ni identificadoPor, usa fichaAt', () => {
    const fichaAt = new Date('2026-02-01T00:00:00.000Z');
    expect(_primeraIdentificacionEfectiva({ identificadoPor: [], fichaAt })).toBe(fichaAt);
    expect(_primeraIdentificacionEfectiva({ fichaAt })).toBe(fichaAt);
  });

  test('fichaAt nunca gana sobre identificadoPor, aunque fichaAt sea más antigua', () => {
    const mov = {
      identificadoPor: [{ nombre: 'katia', fechaId: new Date('2026-05-06T15:56:38.305Z') }],
      fichaAt: new Date('2026-01-01T00:00:00.000Z'),
    };
    expect(_primeraIdentificacionEfectiva(mov)).toEqual(new Date('2026-05-06T15:56:38.305Z'));
  });

  test('sin ningún campo (ni fichaAt) -> null', () => {
    expect(_primeraIdentificacionEfectiva({})).toBeNull();
  });
});

describe('Consistencia día↔semana (garantía central del diseño del ledger, no una suposición)', () => {
  test('sumar nuevos/identificados de los 7 días de una semana da EXACTO lo mismo que calcular la semana directo', async () => {
    const candidatos = [
      // Iniciales de la semana: ya existía antes del lunes, nunca se resuelve.
      { deposito: 1000, createdAt: new Date('2026-08-01T12:00:00.000Z'), primeraIdentificacionAt: null, historialVinculacion: [] },
      // Iniciales de la semana, se identifica el miércoles (día 3).
      { deposito: 2000, createdAt: new Date('2026-08-01T12:00:00.000Z'), primeraIdentificacionAt: new Date(Date.UTC(2026, 8, 30, 15, 0, 0)), historialVinculacion: [] },
      // Nuevo del lunes (día 1), se identifica el mismo lunes.
      { deposito: 3000, createdAt: new Date(Date.UTC(2026, 8, 28, 10, 0, 0)), primeraIdentificacionAt: new Date(Date.UTC(2026, 8, 28, 18, 0, 0)), historialVinculacion: [] },
      // Nuevo del viernes (día 5), sigue pendiente al cierre de la semana.
      { deposito: 4000, createdAt: new Date(Date.UTC(2026, 9, 2, 10, 0, 0)), primeraIdentificacionAt: null, historialVinculacion: [] },
      // Nuevo del domingo (día 7), se identifica el mismo domingo.
      { deposito: 5000, createdAt: new Date(Date.UTC(2026, 9, 4, 10, 0, 0)), primeraIdentificacionAt: new Date(Date.UTC(2026, 9, 4, 20, 0, 0)), historialVinculacion: [] },
    ];
    // mockImplementation (no Once): simula el filtro real de Mongo por `createdAt` en cada
    // llamada — sin esto, un ítem creado el viernes aparecería como candidato también en el
    // cálculo del lunes (cuyo periodoFin es antes de que el ítem exista), lo que ninguna query
    // real de Mongo haría. Las 8 llamadas de este test (7 días + 1 semana) reusan el mismo mock.
    BankMovement.find.mockImplementation((filtro) => ({
      select: () => ({ lean: () => Promise.resolve(candidatos.filter((c) => {
        if (c.createdAt > filtro.createdAt.$lte) return false;
        if (filtro.createdAt.$gte && c.createdAt < filtro.createdAt.$gte) return false;
        if (filtro.createdAt.$lt && c.createdAt >= filtro.createdAt.$lt) return false;
        return true;
      })) }),
    }));

    const semanaDirecta = await _calcularLedgerBacklog({ baseMatch: {}, periodoInicio: INICIO_CORTE_ESPERADO, periodoFin: CIERRE_ESPERADO });

    const dias = _enumerarDiasEnRango({ desde: '2026-09-28', hasta: '2026-10-04' });
    expect(dias).toHaveLength(7);

    let nuevosSumaCount = 0, nuevosSumaSaldo = 0, identificadosSumaCount = 0, identificadosSumaSaldo = 0;
    let idDeInicialesSumaCount = 0, idDeInicialesSumaSaldo = 0, idDeNuevosSumaCount = 0, idDeNuevosSumaSaldo = 0;
    for (const { inicio, fin } of dias) {
      const ledgerDia = await _calcularLedgerBacklog({ baseMatch: {}, periodoInicio: inicio, periodoFin: fin });
      nuevosSumaCount += ledgerDia.nuevos.count;
      nuevosSumaSaldo += ledgerDia.nuevos.saldo;
      identificadosSumaCount += ledgerDia.identificados.count;
      identificadosSumaSaldo += ledgerDia.identificados.saldo;
      idDeInicialesSumaCount += ledgerDia.identificadosDeIniciales.count;
      idDeInicialesSumaSaldo += ledgerDia.identificadosDeIniciales.saldo;
      idDeNuevosSumaCount += ledgerDia.identificadosDeNuevos.count;
      idDeNuevosSumaSaldo += ledgerDia.identificadosDeNuevos.saldo;
    }

    // Verificación no trivial: confirma que el dataset realmente ejercita ambas cosas antes de
    // comparar (si diera 0 de los dos lados, el test pasaría sin probar nada real).
    expect(semanaDirecta.nuevos.count).toBeGreaterThan(0);
    expect(semanaDirecta.identificados.count).toBeGreaterThan(0);
    expect(semanaDirecta.identificadosDeIniciales.count).toBeGreaterThan(0);
    expect(semanaDirecta.identificadosDeNuevos.count).toBeGreaterThan(0);

    expect(nuevosSumaCount).toBe(semanaDirecta.nuevos.count);
    expect(nuevosSumaSaldo).toBe(semanaDirecta.nuevos.saldo);
    expect(identificadosSumaCount).toBe(semanaDirecta.identificados.count);
    expect(identificadosSumaSaldo).toBe(semanaDirecta.identificados.saldo);
    // El invariante también vale partido por origen (2026-10-30), no solo en el total combinado.
    expect(idDeInicialesSumaCount).toBe(semanaDirecta.identificadosDeIniciales.count);
    expect(idDeInicialesSumaSaldo).toBe(semanaDirecta.identificadosDeIniciales.saldo);
    expect(idDeNuevosSumaCount).toBe(semanaDirecta.identificadosDeNuevos.count);
    expect(idDeNuevosSumaSaldo).toBe(semanaDirecta.identificadosDeNuevos.saldo);
  });
});

describe('getProgresoResolucionPorCorte (2026-10-30: vista semanal del ledger genérico)', () => {
  test('arma {fechaInicio, fechaCierre, iniciales, nuevos, identificados, total} por corte, reusando _calcularLedgerBacklog', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      { deposito: 100, createdAt: new Date('2026-09-01T12:00:00.000Z'), primeraIdentificacionAt: null, historialVinculacion: [] },
      { deposito: 200, createdAt: new Date('2026-09-01T12:00:00.000Z'), primeraIdentificacionAt: new Date(CIERRE_ESPERADO.getTime() - 2 * 86400000), historialVinculacion: [] },
    ]) }) });

    const { cortes } = await getProgresoResolucionPorCorte({ desde: '2026-10-04', hasta: '2026-10-04' });

    expect(cortes).toHaveLength(1);
    expect(cortes[0]).toEqual({
      fechaInicio: INICIO_CORTE_ESPERADO,
      fechaCierre: CIERRE_ESPERADO,
      iniciales:                { count: 2, saldo: 300 },
      nuevos:                   { count: 0, saldo: 0 },
      identificados:            { count: 1, saldo: 200 },
      identificadosDeIniciales: { count: 1, saldo: 200 },
      identificadosDeNuevos:    { count: 0, saldo: 0 },
      total:                    { count: 1, saldo: 100 },
    });
  });

  test('mesOrigen inválido tira BadRequestError', async () => {
    await expect(getProgresoResolucionPorCorte({ mesOrigen: '2026-13' })).rejects.toThrow(BadRequestError);
  });

  test('sin desde/hasta, enumera los últimos 8 cortes semanales cerrados (la semana en curso no se incluye)', () => {
    const cortes = _enumerarCortesSemanales({});
    expect(cortes).toHaveLength(8);
    expect(cortes[cortes.length - 1]).toEqual({ inicio: INICIO_CORTE_ESPERADO, fin: CIERRE_ESPERADO });
    expect(cortes[cortes.length - 2].fin).toEqual(new Date(CIERRE_ESPERADO.getTime() - 7 * 86400000));
  });
});

describe('getLedgerDiario (2026-10-30: vista diaria del ledger genérico, HOY incluido en vivo)', () => {
  test('arma {fecha, iniciales, nuevos, identificados, total} por día, reusando _calcularLedgerBacklog', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      { deposito: 700, createdAt: AHORA_MX, primeraIdentificacionAt: null, historialVinculacion: [] },
    ]) }) });

    const { dias } = await getLedgerDiario({ desde: '2026-10-07', hasta: '2026-10-07' });

    expect(dias).toHaveLength(1);
    expect(dias[0].fecha).toEqual(new Date(Date.UTC(2026, 9, 7, 6, 0, 0))); // inicio del día de hoy
    expect(dias[0].nuevos).toEqual({ count: 1, saldo: 700 });
  });

  test('el día de HOY usa "ahora" real como fin (no medianoche) — un día pasado usa medianoche', () => {
    const dias = _enumerarDiasEnRango({ desde: '2026-10-06', hasta: '2026-10-07' });
    expect(dias).toHaveLength(2);
    expect(dias[0].fin).toEqual(new Date(Date.UTC(2026, 9, 7, 5, 59, 59, 999))); // ayer, cerrado
    expect(dias[1].fin).toEqual(AHORA_MX); // hoy, en vivo
  });
});

describe('_resolverMesOrigen', () => {
  test('resuelve YYYY-MM a los límites $gte/$lt del mes', () => {
    expect(_resolverMesOrigen('2026-08')).toEqual({
      mesOrigenInicio: new Date(Date.UTC(2026, 7, 1, 6, 0, 0)),
      mesOrigenFin: new Date(Date.UTC(2026, 8, 1, 6, 0, 0)),
    });
  });

  test('sin mesOrigen, devuelve ambos null', () => {
    expect(_resolverMesOrigen(null)).toEqual({ mesOrigenInicio: null, mesOrigenFin: null });
  });

  test('mesOrigen con formato inválido tira BadRequestError', () => {
    expect(() => _resolverMesOrigen('agosto-2026')).toThrow(BadRequestError);
  });
});

describe('_enumerarMesesParaLedger (2026-10-30: límites mensuales para getLedgerMensual, mes en curso incluido en vivo)', () => {
  test('sin desde/hasta, enumera los últimos `cantidad` meses, el mes en curso con fin en vivo', () => {
    const meses = _enumerarMesesParaLedger({ cantidad: 3 });
    expect(meses).toHaveLength(3);
    expect(meses.map(m => m.mes)).toEqual(['2026-08', '2026-09', '2026-10']);
    expect(meses[0].inicio).toEqual(new Date(Date.UTC(2026, 7, 1, 6, 0, 0)));
    // Agosto y septiembre ya cerraron: fin = último instante del mes, no AHORA_MX.
    expect(meses[0].fin).toEqual(new Date(Date.UTC(2026, 8, 1, 6, 0, 0) - 1));
    // Octubre es el mes en curso bajo AHORA_MX (miércoles 2026-10-07 15:00 México): fin EN VIVO.
    expect(meses[2].fin).toEqual(AHORA_MX);
  });

  test('con `hasta` explícito, ningún mes del rango se trata como "en curso" (fin siempre cierre de mes)', () => {
    const meses = _enumerarMesesParaLedger({ desde: '2026-06-01', hasta: '2026-07-31' });
    expect(meses.map(m => m.mes)).toEqual(['2026-06', '2026-07']);
    expect(meses[1].fin).toEqual(new Date(Date.UTC(2026, 7, 1, 6, 0, 0) - 1));
  });

  test('cruza el límite de año correctamente', () => {
    const meses = _enumerarMesesParaLedger({ desde: '2025-11-01', hasta: '2026-02-01' });
    expect(meses.map(m => m.mes)).toEqual(['2025-11', '2025-12', '2026-01', '2026-02']);
  });
});

describe('getLedgerMensual (2026-10-30: vista mensual del ledger genérico — reemplaza el corte semanal para este dashboard)', () => {
  test('arma {mes, fechaInicio, fechaFin, iniciales, nuevos, identificados, total} por mes, reusando _calcularLedgerBacklog', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      // Cargado en septiembre, antes del inicio de octubre — es "iniciales" del mes de octubre.
      { deposito: 300, createdAt: new Date('2026-09-15T12:00:00.000Z'), primeraIdentificacionAt: null, historialVinculacion: [] },
    ]) }) });

    const { meses } = await getLedgerMensual({ desde: '2026-10-01', hasta: '2026-10-31' });

    expect(meses).toHaveLength(1);
    expect(meses[0].mes).toBe('2026-10');
    expect(meses[0].iniciales).toEqual({ count: 1, saldo: 300 });
  });

  test('mesOrigen inválido tira BadRequestError', async () => {
    await expect(getLedgerMensual({ mesOrigen: '2026-13' })).rejects.toThrow(BadRequestError);
  });
});

describe('_enumerarHorasDelDia (2026-10-30: límites horarios de un día para getLedgerPorHora — hoy en vivo, otro día cerrado)', () => {
  test('sin fecha (hoy), bajo AHORA_MX (miércoles 2026-10-07 15:00 México), enumera de 00:00 a 15:00, la hora en curso con fin en vivo', () => {
    const horas = _enumerarHorasDelDia();
    expect(horas).toHaveLength(16); // 00:00 .. 15:00 inclusive
    expect(horas[0].hora).toBe('00:00');
    expect(horas[0].inicio).toEqual(new Date(Date.UTC(2026, 9, 7, 6, 0, 0))); // medianoche de hoy, hora México
    expect(horas[horas.length - 1].hora).toBe('15:00');
    // La hora en curso (15:00) tiene fin EN VIVO (AHORA_MX), no el cierre de esa hora.
    expect(horas[horas.length - 1].fin).toEqual(AHORA_MX);
    // Una hora ya cerrada (ej. 14:00, que va de las 14:00 a las 14:59:59.999 México =
    // 20:00 a 20:59:59.999 UTC) tiene fin = último instante de esa hora, no medianoche.
    const hora14 = horas.find(h => h.hora === '14:00');
    expect(hora14.fin).toEqual(new Date(Date.UTC(2026, 9, 7, 21, 0, 0) - 1));
  });

  test('con fecha de un día pasado (ayer), enumera las 24 horas completas, TODAS cerradas — nunca "ahora", ni la última', () => {
    const horas = _enumerarHorasDelDia('2026-10-06');
    expect(horas).toHaveLength(24);
    expect(horas[0].hora).toBe('00:00');
    expect(horas[0].inicio).toEqual(new Date(Date.UTC(2026, 9, 6, 6, 0, 0)));
    expect(horas[horas.length - 1].hora).toBe('23:00');
    // La hora 23:00 de un día YA PASADO tiene fin fijo (cierre de esa hora), nunca AHORA_MX.
    expect(horas[horas.length - 1].fin).not.toEqual(AHORA_MX);
    expect(horas[horas.length - 1].fin).toEqual(new Date(Date.UTC(2026, 9, 7, 6, 0, 0) - 1));
  });
});

describe('getLedgerPorHora (2026-10-30: vista por hora del ledger genérico, SOLO hoy, en vivo)', () => {
  test('arma {hora, fechaInicio, fechaFin, iniciales, nuevos, identificados, total} por cada hora transcurrida de hoy', async () => {
    BankMovement.find.mockReturnValue({ select: () => ({ lean: () => Promise.resolve([]) }) });
    const { horas } = await getLedgerPorHora({});
    expect(horas.length).toBeGreaterThan(0);
    expect(horas[horas.length - 1].hora).toBe('15:00');
    expect(horas[0]).toHaveProperty('iniciales');
    expect(horas[0]).toHaveProperty('nuevos');
    expect(horas[0]).toHaveProperty('identificados');
    expect(horas[0]).toHaveProperty('total');
  });
});

describe('getUltimosMovimientos (2026-10-30: feed de actividad reciente — cargados e identificados, combinados y deduplicados)', () => {
  test('clasifica tipo por createdAt (nuevo = hoy, inicial = antes de hoy) y arma mesOrigen por hora México', async () => {
    BankMovement.find
      .mockReturnValueOnce({ sort: () => ({ limit: () => ({ select: () => ({ lean: () => Promise.resolve([
        { _id: 'a', folio: 1, deposito: 500, banco: 'BBVA', status: 'no_identificado', createdAt: AHORA_MX, primeraIdentificacionAt: null },
      ]) }) }) }) })
      .mockReturnValueOnce({ sort: () => ({ limit: () => ({ select: () => ({ lean: () => Promise.resolve([
        { _id: 'b', folio: 2, deposito: 700, banco: 'Santander', status: 'identificado', createdAt: new Date('2026-08-15T12:00:00.000Z'), primeraIdentificacionAt: AHORA_MX },
      ]) }) }) }) });

    const { movimientos } = await getUltimosMovimientos({});

    expect(movimientos).toHaveLength(2);
    const nuevo = movimientos.find(m => m.folio === 1);
    expect(nuevo.tipo).toBe('nuevo');
    const inicial = movimientos.find(m => m.folio === 2);
    expect(inicial.tipo).toBe('inicial');
    expect(inicial.mesOrigen).toBe('2026-08');
    expect(inicial.fechaEvento).toEqual(AHORA_MX); // evento relevante = cuándo se identificó, no cuándo se cargó
  });

  test('el mismo movimiento en ambas queries (cargado Y identificado) no se duplica — se queda con la fecha de evento más reciente', async () => {
    const mismoMov = { _id: 'x', folio: 9, deposito: 900, banco: 'BBVA', status: 'identificado', createdAt: AHORA_MX, primeraIdentificacionAt: AHORA_MX };
    BankMovement.find
      .mockReturnValueOnce({ sort: () => ({ limit: () => ({ select: () => ({ lean: () => Promise.resolve([mismoMov]) }) }) }) })
      .mockReturnValueOnce({ sort: () => ({ limit: () => ({ select: () => ({ lean: () => Promise.resolve([mismoMov]) }) }) }) });

    const { movimientos } = await getUltimosMovimientos({ limit: 10 });
    expect(movimientos).toHaveLength(1);
  });

  test('respeta `limit` tras combinar y deduplicar', async () => {
    const movs = Array.from({ length: 5 }, (_, i) => ({
      _id: `m${i}`, folio: i, deposito: 100, banco: 'BBVA', status: 'no_identificado',
      createdAt: new Date(AHORA_MX.getTime() - i * 60000), primeraIdentificacionAt: null,
    }));
    BankMovement.find
      .mockReturnValueOnce({ sort: () => ({ limit: () => ({ select: () => ({ lean: () => Promise.resolve(movs) }) }) }) })
      .mockReturnValueOnce({ sort: () => ({ limit: () => ({ select: () => ({ lean: () => Promise.resolve([]) }) }) }) });

    const { movimientos } = await getUltimosMovimientos({ limit: 2 });
    expect(movimientos).toHaveLength(2);
    expect(movimientos[0].folio).toBe(0); // el más reciente primero
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

describe('_rangoAntiguedad (2026-11-06) — límites exactos de cada bucket', () => {
  test.each([
    [0, 0], [30, 0], [31, 1], [60, 1], [61, 2], [90, 2], [91, 3], [500, 3],
  ])('%i días de antigüedad -> bucket %i', (dias, esperado) => {
    expect(_rangoAntiguedad(dias)).toBe(esperado);
  });
});

// getAntiguedadDepositos (2026-11-06, pedido explícito del usuario tras revisar un mockup de
// referencia): retoma la idea de rangos de días que se había descartado el 30-oct, con 2
// correcciones sobre esa primera versión — agrupa por `fecha` del depósito (no `createdAt`), y
// exige el depósito completamente vacío (status no_identificado Y sin ninguna CxC vinculada).
// Bajo AHORA_MX (miércoles 2026-10-07 15:00 México), "hoy" = 2026-10-07, corte = medianoche de
// ese día (2026-10-07T06:00:00Z).
describe('getAntiguedadDepositos', () => {
  test('agrupa en los 4 rangos fijos según días desde `fecha` (no createdAt) hasta hoy', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      // 10 días de antigüedad (2026-09-27 -> 2026-10-07) -> "1 a 30 días".
      { deposito: 100, fecha: new Date('2026-09-27T12:00:00.000Z'), createdAt: new Date('2020-01-01') },
      // 40 días -> "31 a 60 días".
      { deposito: 200, fecha: new Date('2026-08-28T12:00:00.000Z') },
      // 70 días -> "61 a 90 días".
      { deposito: 300, fecha: new Date('2026-07-29T12:00:00.000Z') },
      // 100 días -> "Más de 90 días".
      { deposito: 400, fecha: new Date('2026-06-29T12:00:00.000Z') },
    ]) }) });

    const { rangos, total } = await getAntiguedadDepositos({});

    expect(rangos).toEqual([
      { rango: '1 a 30 días', count: 1, saldo: 100 },
      { rango: '31 a 60 días', count: 1, saldo: 200 },
      { rango: '61 a 90 días', count: 1, saldo: 300 },
      { rango: 'Más de 90 días', count: 1, saldo: 400 },
    ]);
    expect(total).toEqual({ count: 4, saldo: 1000 });
  });

  test('varios depósitos en el mismo rango se acumulan (count y saldo)', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      { deposito: 100, fecha: new Date('2026-10-05T12:00:00.000Z') }, // 2 días
      { deposito: 50,  fecha: new Date('2026-10-01T12:00:00.000Z') }, // 6 días
    ]) }) });

    const { rangos, total } = await getAntiguedadDepositos({});

    expect(rangos[0]).toEqual({ rango: '1 a 30 días', count: 2, saldo: 150 });
    expect(total).toEqual({ count: 2, saldo: 150 });
  });

  // 2026-11-06, cuarta ronda: "hoy" (sin `fecha`, o `fecha`=hoy) usa `status` directo en Mongo,
  // sin reconstrucción — más simple y más correcto que reconstruir, porque `status` YA es la
  // fuente de verdad del presente sin importar cómo se resolvió el depósito (ficha, CxC, edición
  // manual, match automático).
  test('sin `fecha` (hoy), filtra status:"no_identificado" + erpIds vacío DIRECTO en Mongo — sin traer "identificado"', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([]) }) });
    await getAntiguedadDepositos({});

    const [filtro] = BankMovement.find.mock.calls[0];
    expect(filtro.status).toBe('no_identificado');
    expect(filtro.erpIds).toEqual({ $size: 0 });
  });

  // Bug real corregido 2026-11-06 (tercera ronda): un depósito que estaba vacío en un mes pasado
  // pero se vinculó después no debe quedar invisible para TODOS los cortes — un corte pasado
  // anterior a su primera resolución sí debe contarlo como backlog de ese momento. Esto solo
  // aplica al camino histórico (`fecha` pasada); el camino "hoy" ya lo excluye a nivel de Mongo
  // (ver test de arriba), no hace falta re-probarlo acá con un mock que no respeta el filtro real.
  test('con `fecha` pasada, consulta status no_identificado/identificado (nunca otros/reclasificado) SIN filtrar por erpIds — y un depósito ya resuelto hoy SÍ cuenta como backlog en un corte anterior a su resolución', async () => {
    const mockDeposito = {
      deposito: 100,
      fecha: new Date('2026-03-01T12:00:00.000Z'),
      erpIds: ['cxc-1'], // ya vinculado hoy
      identificadoPor: [{ fechaId: new Date('2026-06-15T00:00:00.000Z') }],
    };
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([mockDeposito]) }) });

    const marzo = await getAntiguedadDepositos({ fecha: '2026-03-10' });
    expect(marzo.total).toEqual({ count: 1, saldo: 100 });

    const [filtro] = BankMovement.find.mock.calls[0];
    expect(filtro.status).toEqual({ $in: ['no_identificado', 'identificado'] });
    expect(filtro.erpIds).toBeUndefined();
  });

  // Bug real 2026-11-06 (cuarta ronda): de un residuo de 635 depósitos de enero 2026 que el chart
  // de Antigüedad mostraba como "pendientes" pese a `status:'identificado'`, el 96.5% tenía
  // `fichaAt` poblado sin `primeraIdentificacionAt` ni `identificadoPor` (ficha física, más vieja
  // que la adopción de `resolvePrimeraIdentificacion()`). `_primeraResueltaEfectiva` necesita el
  // fallback a `fichaAt` para reconstruir bien un corte pasado anterior/posterior a esa ficha.
  test('con `fecha` pasada, un depósito identificado por FICHA (sin identificadoPor ni primeraIdentificacionAt) cuenta como backlog antes de fichaAt y no después', async () => {
    const mockDeposito = {
      deposito: 250,
      fecha: new Date('2026-01-08T12:00:00.000Z'),
      erpIds: [],
      identificadoPor: [],
      primeraIdentificacionAt: null,
      fichaAt: new Date('2026-02-01T00:00:00.000Z'),
    };

    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([mockDeposito]) }) });
    const antes = await getAntiguedadDepositos({ fecha: '2026-01-31' });
    expect(antes.total).toEqual({ count: 1, saldo: 250 });

    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([mockDeposito]) }) });
    const despues = await getAntiguedadDepositos({ fecha: '2026-02-15' });
    expect(despues.total).toEqual({ count: 0, saldo: 0 });
  });

  test('banco: se pasa al $match (buildBaseMatch)', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([]) }) });
    await getAntiguedadDepositos({ banco: 'BBVA' });

    const [filtro] = BankMovement.find.mock.calls[0];
    expect(filtro.banco).toBe('BBVA');
    expect(filtro.deposito).toEqual({ $gt: 0 });
  });

  test('sin depósitos: los 4 rangos y el total quedan en cero — no explota', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([]) }) });
    const { rangos, total } = await getAntiguedadDepositos({});

    expect(rangos.every((r) => r.count === 0 && r.saldo === 0)).toBe(true);
    expect(rangos.map((r) => r.rango)).toEqual(['1 a 30 días', '31 a 60 días', '61 a 90 días', 'Más de 90 días']);
    expect(total).toEqual({ count: 0, saldo: 0 });
  });

  // Corte por fecha pasada (2026-11-06, segunda ronda — el usuario revisó de nuevo el mockup de
  // referencia y pidió poder elegir un corte histórico, no solo "hoy").
  test('con `fecha` pasada, recorta la antigüedad a ESE corte, no a hoy', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      // Al corte 2026-09-10: 9 días de antigüedad -> "1 a 30 días" (sería "31 a 60" si se
      // calculara contra hoy, 2026-10-07).
      { deposito: 100, fecha: new Date('2026-09-01T12:00:00.000Z') },
    ]) }) });

    const { rangos, total } = await getAntiguedadDepositos({ fecha: '2026-09-10' });

    expect(rangos[0]).toEqual({ rango: '1 a 30 días', count: 1, saldo: 100 });
    expect(total).toEqual({ count: 1, saldo: 100 });
  });

  test('un depósito con fecha POSTERIOR al corte no existía todavía — se excluye (nunca antigüedad negativa)', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      { deposito: 100, fecha: new Date('2026-09-15T12:00:00.000Z') },
    ]) }) });

    const { total } = await getAntiguedadDepositos({ fecha: '2026-09-10' });

    expect(total).toEqual({ count: 0, saldo: 0 });
  });
});

describe('getAntiguedadDepositosPorAnio', () => {
  test('agrupa por MES DE ORIGEN del depósito, no por un corte de fin de mes — un depósito de enero aparece SOLO en enero, con antigüedad contra HOY', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      { deposito: 100, fecha: new Date('2026-01-15T12:00:00.000Z') },
    ]) }) });

    const { meses } = await getAntiguedadDepositosPorAnio({ anio: '2026' });

    // Bajo AHORA_MX (2026-10-07), el mes en curso es octubre — no hay meses futuros.
    expect(meses.map((m) => m.periodo)).toEqual([
      '2026-01', '2026-02', '2026-03', '2026-04', '2026-05', '2026-06',
      '2026-07', '2026-08', '2026-09', '2026-10',
    ]);
    const enero = meses.find((m) => m.periodo === '2026-01');
    expect(enero.total).toEqual({ count: 1, saldo: 100 });
    expect(enero.rangos[3].count).toBe(1); // contra HOY (2026-10-07): ~265 días -> "Más de 90 días"
    // El depósito NO se repite en ningún otro mes — a diferencia del diseño anterior (un corte
    // histórico por mes), acá cada depósito vive solo en su mes de origen.
    for (const mes of meses.filter((m) => m.periodo !== '2026-01')) {
      expect(mes.total).toEqual({ count: 0, saldo: 0 });
    }
  });

  // 2026-11-06, cuarta ronda: ya no hay filtrado en JS acá — un depósito `identificado` (por CxC,
  // ficha, o lo que sea) nunca llega siquiera a este código porque la query a Mongo ya pide
  // `status:'no_identificado'` directo (ver `_antiguedadCandidatosHoy`). Este test confirma la
  // query real, no un post-filtrado — mockear un `identificado` acá y esperar que "desaparezca"
  // ya no prueba nada real (el mock no respeta el filtro, a diferencia de Mongo real).
  test('consulta status:"no_identificado" + erpIds vacío DIRECTO en Mongo — un depósito identificado nunca es candidato, sea cual sea su rastro de identificación', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([]) }) });
    await getAntiguedadDepositosPorAnio({ anio: '2026' });

    const [filtro] = BankMovement.find.mock.calls[0];
    expect(filtro.status).toBe('no_identificado');
    expect(filtro.erpIds).toEqual({ $size: 0 });
  });

  test('un depósito con fecha posterior a todos los cortes del año no aparece en ningún mes', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([
      { deposito: 100, fecha: new Date('2026-11-15T12:00:00.000Z') },
    ]) }) });

    const { meses } = await getAntiguedadDepositosPorAnio({ anio: '2026' });

    expect(meses.every((m) => m.total.count === 0)).toBe(true);
  });

  test.each(['abcd', '26', '', null, undefined])('anio inválido (%p) lanza BadRequestError', async (anio) => {
    await expect(getAntiguedadDepositosPorAnio({ anio })).rejects.toThrow(BadRequestError);
  });

  test('banco: se pasa al $match (buildBaseMatch)', async () => {
    BankMovement.find.mockReturnValueOnce({ select: () => ({ lean: () => Promise.resolve([]) }) });
    await getAntiguedadDepositosPorAnio({ anio: '2026', banco: 'BBVA' });

    const [filtro] = BankMovement.find.mock.calls[0];
    expect(filtro.banco).toBe('BBVA');
  });
});

// buildReporteCortes/listarDescargasCorte (2026-11-06, pedido explícito del usuario: un solo
// botón de exportar para todo Cortes + un historial de descargas con delta vs. la anterior).
// `BankMovement.find` se mockea con `mockReturnValue` (no `mockReturnValueOnce`) a propósito:
// getLedgerMensual/getLedgerDiario/getAntiguedadDepositos hacen VARIAS llamadas (una por
// mes/día + una de antigüedad), pero el filtrado por periodo real pasa en JS dentro de
// _calcularLedgerBacklog/_estadoAlCierre, no en el mock — devolver el MISMO set de documentos a
// cada llamada es suficiente para ejercitar la integración real sin encolar 20+ mocks.
describe('buildReporteCortes', () => {
  test('arma 4 hojas (Mensual/Diario/Antigüedad/Detalle de depósitos) y deja un registro en CorteDescarga con el backlog del día más reciente', async () => {
    BankMovement.find.mockReturnValue({ select: () => ({ lean: () => Promise.resolve([
      {
        folio: '012345', banco: 'BBVA', deposito: 500, fecha: AHORA_MX, createdAt: AHORA_MX,
        primeraIdentificacionAt: null, historialVinculacion: [], erpIds: [],
      },
    ]) }) });
    CorteDescarga.create.mockResolvedValue({});
    subirReporteCorte.mockResolvedValue('drive-id-1');

    const buffer = await buildReporteCortes({ usuario: { userId: 'u1', nombre: 'Ana' } });
    const wb = await leerWorkbook(buffer);

    expect(wb.getWorksheet('Mensual')).toBeDefined();
    expect(wb.getWorksheet('Diario')).toBeDefined();
    expect(wb.getWorksheet('Antigüedad')).toBeDefined();
    // "Detalle de depósitos" (2026-11-06, pedido explícito del usuario: "que al exportar esto me
    // diga qué depósitos son los que están siendo involucrados") — misma población que la hoja
    // "Antigüedad", pero depósito por depósito.
    const wsDetalle = wb.getWorksheet('Detalle de depósitos');
    expect(wsDetalle).toBeDefined();
    expect(wsDetalle.getRow(1).values.filter(Boolean)).toEqual([
      'Folio', 'Banco', 'Fecha del depósito', 'Importe', 'Mes de origen', 'Antigüedad (días)', 'Rango',
    ]);
    const filaDetalle = wsDetalle.getRow(2).values.filter((v) => v !== undefined && v !== null);
    // AHORA_MX = 2026-10-07 15:00 hora México (ver mock de tiempo del archivo) — mismo día que el
    // corte de "hoy", así que la antigüedad es 0 días ("1 a 30 días").
    expect(filaDetalle).toEqual(['012345', 'BBVA', '2026-10-07', 500, '2026-10', 0, '1 a 30 días']);
    // Encabezados de la hoja Mensual — mismo orden que la tabla "Resultados por mes" del frontend.
    const headerMensual = wb.getWorksheet('Mensual').getRow(1).values.filter(Boolean);
    expect(headerMensual).toEqual([
      'Mes', 'Saldo inicial', '+ Nuevos', '= Al cierre', '− Identificados',
      '= Depósitos por identificar', 'Dep. saldo inicial', '+ Dep. nuevos', '= Dep. al cierre',
      '− Dep. identificados', '= Dep. por identificar',
    ]);

    expect(CorteDescarga.create).toHaveBeenCalledTimes(1);
    const [doc] = CorteDescarga.create.mock.calls[0];
    expect(doc.usuario).toEqual({ userId: 'u1', nombre: 'Ana' });
    expect(doc.banco).toBeNull();
    // El depósito cargado HOY, no_identificado y sin historial, es "nuevo" y sigue pendiente al
    // cierre de hoy — el snapshot debe reflejarlo (1 depósito, $500).
    expect(doc.depositosNoIdentificados).toEqual({ count: 1, saldo: 500 });
    // Subida a Drive exitosa (2026-11-06) — el registro guarda el driveFileId real.
    expect(subirReporteCorte).toHaveBeenCalledTimes(1);
    expect(doc.driveFileId).toBe('drive-id-1');
  });

  test('pasa `banco` a las 3 fuentes y al registro de CorteDescarga', async () => {
    BankMovement.find.mockReturnValue({ select: () => ({ lean: () => Promise.resolve([]) }) });
    CorteDescarga.create.mockResolvedValue({});
    subirReporteCorte.mockResolvedValue('drive-id-2');

    await buildReporteCortes({ banco: 'BBVA', usuario: { userId: 'u1', nombre: 'Ana' } });

    expect(BankMovement.find.mock.calls.every(([filtro]) => filtro.banco === 'BBVA')).toBe(true);
    const [doc] = CorteDescarga.create.mock.calls[0];
    expect(doc.banco).toBe('BBVA');
    expect(doc.depositosNoIdentificados).toEqual({ count: 0, saldo: 0 });
  });

  // Drive best-effort (2026-11-06, pedido explícito del usuario: poder volver a bajar el Excel de
  // un punto pasado) — si la subida falla, la descarga EN CURSO del usuario nunca se bloquea.
  test('si falla la subida a Drive, igual devuelve el buffer y guarda driveFileId:null (nunca rompe la descarga en curso)', async () => {
    BankMovement.find.mockReturnValue({ select: () => ({ lean: () => Promise.resolve([]) }) });
    CorteDescarga.create.mockResolvedValue({});
    subirReporteCorte.mockRejectedValue(new Error('carpeta no configurada'));

    const buffer = await buildReporteCortes({ usuario: { userId: 'u1', nombre: 'Ana' } });

    expect(buffer).toBeInstanceOf(Buffer);
    const wb = await leerWorkbook(buffer);
    expect(wb.getWorksheet('Mensual')).toBeDefined();
    const [doc] = CorteDescarga.create.mock.calls[0];
    expect(doc.driveFileId).toBeNull();
  });
});

describe('descargarReporteCorteHistorico', () => {
  test('resuelve el registro por id y descarga el binario real de Drive', async () => {
    CorteDescarga.findById.mockReturnValue({ lean: () => Promise.resolve({
      _id: '1', fecha: new Date('2026-10-05T12:00:00.000Z'), driveFileId: 'drive-id-1',
    }) });
    descargarReporteCorte.mockResolvedValue(Buffer.from('xlsx-real'));

    const { buffer, filename } = await descargarReporteCorteHistorico('1');

    expect(descargarReporteCorte).toHaveBeenCalledWith('drive-id-1');
    expect(buffer.toString()).toBe('xlsx-real');
    expect(filename).toBe('Cortes-2026-10-05.xlsx');
  });

  test('id inexistente -> NotFoundError', async () => {
    CorteDescarga.findById.mockReturnValue({ lean: () => Promise.resolve(null) });

    await expect(descargarReporteCorteHistorico('x')).rejects.toThrow(NotFoundError);
    expect(descargarReporteCorte).not.toHaveBeenCalled();
  });

  test('descarga sin driveFileId (la subida falló en su momento) -> BadRequestError, nunca llama a Drive', async () => {
    CorteDescarga.findById.mockReturnValue({ lean: () => Promise.resolve({
      _id: '2', fecha: new Date('2026-10-05'), driveFileId: null,
    }) });

    await expect(descargarReporteCorteHistorico('2')).rejects.toThrow(BadRequestError);
    expect(descargarReporteCorte).not.toHaveBeenCalled();
  });
});

describe('listarDescargasCorte', () => {
  test('la descarga más reciente trae delta contra la inmediatamente anterior del MISMO banco', async () => {
    CorteDescarga.find.mockReturnValue({ sort: () => ({ lean: () => Promise.resolve([
      // Orden desc (más reciente primero) — mismo orden que devuelve el find real.
      { _id: '3', fecha: new Date('2026-10-07'), banco: null, depositosNoIdentificados: { count: 80, saldo: 8000 } },
      { _id: '2', fecha: new Date('2026-10-06'), banco: null, depositosNoIdentificados: { count: 100, saldo: 10000 } },
      { _id: '1', fecha: new Date('2026-10-05'), banco: null, depositosNoIdentificados: { count: 100, saldo: 10000 } },
    ]) }) });

    const descargas = await listarDescargasCorte({});

    // 80 vs 100 -> bajó 20%.
    expect(descargas[0].delta).toEqual({ countPct: -20, saldoPct: -20, direccion: 'bajo' });
    // 100 vs 100 -> igual.
    expect(descargas[1].delta).toEqual({ countPct: 0, saldoPct: 0, direccion: 'igual' });
    // La más vieja no tiene contra qué compararse.
    expect(descargas[2].delta).toBeNull();
  });

  test('nunca compara un banco contra otro — cada uno se compara solo contra su propio historial', async () => {
    CorteDescarga.find.mockReturnValue({ sort: () => ({ lean: () => Promise.resolve([
      { _id: '2', fecha: new Date('2026-10-07'), banco: 'BBVA', depositosNoIdentificados: { count: 50, saldo: 5000 } },
      { _id: '1', fecha: new Date('2026-10-06'), banco: 'Banamex', depositosNoIdentificados: { count: 999, saldo: 99900 } },
    ]) }) });

    const descargas = await listarDescargasCorte({});

    // La única descarga de BBVA no tiene una anterior CON EL MISMO banco — nunca compara contra
    // la de Banamex aunque sea la fila inmediatamente siguiente en la lista.
    expect(descargas[0].delta).toBeNull();
    expect(descargas[1].delta).toBeNull();
  });

  test('con `count` anterior en 0, el porcentaje de conteo es null (no divide por cero) pero `direccion` sigue siendo correcta', async () => {
    CorteDescarga.find.mockReturnValue({ sort: () => ({ lean: () => Promise.resolve([
      { _id: '2', fecha: new Date('2026-10-07'), banco: null, depositosNoIdentificados: { count: 5, saldo: 500 } },
      { _id: '1', fecha: new Date('2026-10-06'), banco: null, depositosNoIdentificados: { count: 0, saldo: 0 } },
    ]) }) });

    const descargas = await listarDescargasCorte({});

    expect(descargas[0].delta).toEqual({ countPct: null, saldoPct: null, direccion: 'subio' });
  });

  test('`banco` filtra el historial a ese banco exacto', async () => {
    const lean = jest.fn().mockResolvedValue([]);
    const sort = jest.fn().mockReturnValue({ lean });
    CorteDescarga.find.mockReturnValue({ sort });

    await listarDescargasCorte({ banco: 'BBVA' });

    expect(CorteDescarga.find).toHaveBeenCalledWith({ banco: 'BBVA' });
  });
});
