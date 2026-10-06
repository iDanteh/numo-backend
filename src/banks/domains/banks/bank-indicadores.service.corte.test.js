'use strict';

// bank-indicadores.service.corte.test.js — getCorteConciliacion() (2026-10-02, control
// periódico de conciliación: rezagados / nuevos del periodo / identificados en el periodo).
// Mismas dependencias mockeadas que bank-indicadores.service.test.js (requiere bank.service.js
// para reusar _rangoAnioMesMexico/_inicioDiaMx/_finDiaMx).
jest.mock('./BankMovement.model');
jest.mock('../../shared/socket');
jest.mock('./drive-fichas.service');
jest.mock('../../../shared/services/global-config.service');

const ExcelJS = require('exceljs');
const BankMovement = require('./BankMovement.model');
const globalConfigService = require('../../../shared/services/global-config.service');
const { BadRequestError } = require('../../shared/errors/AppError');
const { getCorteConciliacion, buildReporteCorte, getPeriodoCortePorRol } = require('./bank-indicadores.service');

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

function mockAggregates({ rezagados = [], nuevos = [], identificados = [] }) {
  BankMovement.aggregate
    .mockResolvedValueOnce(rezagados)
    .mockResolvedValueOnce(nuevos)
    .mockResolvedValueOnce(identificados);
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

  test('(d) fechaFin anterior a fechaInicio: BadRequestError', async () => {
    await expect(getCorteConciliacion({ fechaInicio: '2026-09-20', fechaFin: '2026-09-14' }))
      .rejects.toThrow(BadRequestError);
  });

  test('(e) sin fechaInicio/fechaFin: sigue devolviendo historico:false y el comportamiento de siempre', async () => {
    mockAggregates({});
    const result = await getCorteConciliacion({});

    expect(result.historico).toBe(false);
    expect(result.fin).toBeUndefined();
    expect(BankMovement.find).not.toHaveBeenCalled();
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
      ['Rezagados — total', 2],
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
