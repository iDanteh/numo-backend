'use strict';

// getSnapshot() es el único punto donde este módulo toca el mundo real (Mongo,
// Postgres, event loop, process) — se mockean sus 3 dependencias directas para
// probarlo sin tocar nada real. construirSnapshot()/calcularEstadoGeneral() son
// puras y se prueban directo, sin mocks.
jest.mock('../shared/utils/db-health', () => ({
  checkMongoOk: jest.fn(),
  checkPostgresOk: jest.fn(),
}));
jest.mock('./traffic-tracker.middleware', () => ({
  getEstadoCrudo: jest.fn(),
}));
jest.mock('./event-loop-lag.util', () => ({
  leerYReiniciarLagMs: jest.fn(),
}));

const { checkMongoOk, checkPostgresOk } = require('../shared/utils/db-health');
const tracker = require('./traffic-tracker.middleware');
const eventLoopLag = require('./event-loop-lag.util');
const { getSnapshot, construirSnapshot, calcularEstadoGeneral } = require('./system-monitor.service');

// ahoraMs fijo, alineado a un minuto exacto (evita ambigüedad de a qué "minuto en
// curso" pertenece cada bucket de prueba).
const AHORA_MS = Date.UTC(2026, 8, 24, 12, 30, 0);
const AHORA_MINUTO = Math.floor(AHORA_MS / 60000);

function minuto(offsetDesdeAhora, { total = 0, c5xx = 0, sumaDuracionMs = 0, muestrasDuracion = 0 } = {}) {
  return { ts: AHORA_MINUTO + offsetDesdeAhora, total, c2xx: total - c5xx, c3xx: 0, c4xx: 0, c5xx, sumaDuracionMs, muestrasDuracion };
}

const MEMORY_USAGE = { rss: 100 * 1048576, heapUsed: 40 * 1048576, heapTotal: 80 * 1048576 };

function baseInput(overrides = {}) {
  return {
    ahoraMs: AHORA_MS,
    enCurso: 0,
    segundosValidos: [],
    minutosValidos: [],
    erroresRecientes: [],
    mongoOk: true,
    pgOk: true,
    lagMs: 5,
    memoryUsage: MEMORY_USAGE,
    uptimeSegundos: 3600,
    ...overrides,
  };
}

describe('calcularEstadoGeneral', () => {
  test('normal: todo en orden', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0, muestrasTasaError: 20, lagMs: 5,
    })).toBe('normal');
  });

  test('caido: Mongo desconectado, sin importar lo demás', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: false, pgOk: true,
      tasaErrorFraccion: 0, muestrasTasaError: 20, lagMs: 5,
    })).toBe('caido');
  });

  test('caido: hubo tráfico en la hora pero silencio total reciente', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: true, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0, muestrasTasaError: 0, lagMs: 5,
    })).toBe('caido');
  });

  test('normal (NO caido): silencio reciente pero NUNCA hubo tráfico en la hora (server recién arrancado)', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: false, sinTraficoReciente: true, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0, muestrasTasaError: 0, lagMs: 5,
    })).toBe('normal');
  });

  test('degradado: Postgres desconectado', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: false,
      tasaErrorFraccion: 0, muestrasTasaError: 20, lagMs: 5,
    })).toBe('degradado');
  });

  test('degradado: tasa de error > 10% con muestras suficientes', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0.25, muestrasTasaError: 20, lagMs: 5,
    })).toBe('degradado');
  });

  test('NO degradado: tasa de error alta pero con muy pocas muestras (ruido)', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 1, muestrasTasaError: 1, lagMs: 5,
    })).toBe('normal');
  });

  test('degradado: event loop lag alto', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0, muestrasTasaError: 20, lagMs: 500,
    })).toBe('degradado');
  });

  test('Mongo caido gana sobre cualquier otra condición', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: true, mongoOk: false, pgOk: false,
      tasaErrorFraccion: 1, muestrasTasaError: 20, lagMs: 999,
    })).toBe('caido');
  });

  // ── Bordes exactos de los 3 umbrales (WARNING pedido en revisión) ────────────
  test('borde: tasa de error EXACTAMENTE 0.10 (10%) NO es degradado — el corte es estrictamente mayor', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0.10, muestrasTasaError: 20, lagMs: 5,
    })).toBe('normal');
  });

  test('borde: tasa de error apenas sobre 0.10 SÍ es degradado', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0.1001, muestrasTasaError: 20, lagMs: 5,
    })).toBe('degradado');
  });

  test('borde: EXACTAMENTE 5 muestras (MIN_MUESTRAS_TASA_ERROR) ya cuenta como "suficientes"', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0.5, muestrasTasaError: 5, lagMs: 5,
    })).toBe('degradado');
  });

  test('borde: 4 muestras (una menos que el mínimo) todavía NO alcanza', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0.5, muestrasTasaError: 4, lagMs: 5,
    })).toBe('normal');
  });

  test('borde: lag EXACTAMENTE 200ms NO es degradado — el corte es estrictamente mayor', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0, muestrasTasaError: 20, lagMs: 200,
    })).toBe('normal');
  });

  test('borde: lag apenas sobre 200ms SÍ es degradado', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0, muestrasTasaError: 20, lagMs: 200.1,
    })).toBe('degradado');
  });

  // ── Mejora #4: CPU real del host (load1 vs. cantidad de cores) ──────────────
  test('degradado: load1 (carga de 1 min) supera la cantidad de cores', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0, muestrasTasaError: 20, lagMs: 5, load1: 5, cpuCores: 4,
    })).toBe('degradado');
  });

  test('borde: load1 EXACTAMENTE igual a cpuCores NO es degradado', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0, muestrasTasaError: 20, lagMs: 5, load1: 4, cpuCores: 4,
    })).toBe('normal');
  });

  test('cpuCores 0 (Windows en dev, o dato no disponible) nunca dispara degradado por CPU', () => {
    expect(calcularEstadoGeneral({
      huboTraficoEnLaHora: true, sinTraficoReciente: false, mongoOk: true, pgOk: true,
      tasaErrorFraccion: 0, muestrasTasaError: 20, lagMs: 5, load1: 999, cpuCores: 0,
    })).toBe('normal');
  });
});

describe('construirSnapshot', () => {
  test('snapshot normal: sin tráfico, sin errores', () => {
    const snap = construirSnapshot(baseInput());

    expect(snap.estadoGeneral).toBe('normal');
    expect(snap.requestsPorMinuto).toBe(0);
    expect(snap.requestsEnCurso).toBe(0);
    expect(snap.tasaErrorPct).toBe(0);
    expect(snap.tiempoRespuestaPromedioMs).toBeNull();
    expect(snap.salud).toEqual({ mongo: 'conectado', postgres: 'conectado' });
    expect(snap.serieUltimaHora).toHaveLength(60);
    expect(snap.serieUltimaHora.every((p) => p.total === 0)).toBe(true);
    expect(snap.erroresRecientes).toEqual([]);
  });

  test('requestsPorMinuto suma los buckets de segundo, no los de minuto', () => {
    const snap = construirSnapshot(baseInput({
      segundosValidos: [
        { ts: 1, total: 3, c2xx: 3, c3xx: 0, c4xx: 0, c5xx: 0, sumaDuracionMs: 30, muestrasDuracion: 3 },
        { ts: 2, total: 2, c2xx: 2, c3xx: 0, c4xx: 0, c5xx: 0, sumaDuracionMs: 20, muestrasDuracion: 2 },
      ],
    }));

    expect(snap.requestsPorMinuto).toBe(5);
  });

  test('tasa de error y tiempo de respuesta se calculan sobre los últimos 5 minutos COMPLETOS, excluyendo el actual', () => {
    const snap = construirSnapshot(baseInput({
      minutosValidos: [
        minuto(0, { total: 999, c5xx: 999 }), // minuto EN CURSO — no debe contar
        minuto(-1, { total: 10, c5xx: 1, sumaDuracionMs: 500, muestrasDuracion: 10 }),
        minuto(-2, { total: 10, c5xx: 1, sumaDuracionMs: 500, muestrasDuracion: 10 }),
        minuto(-6, { total: 100, c5xx: 100 }), // fuera de la ventana de 5 min — no debe contar
      ],
    }));

    // 2 errores de 20 total = 10% exacto, y 1000ms/20 = 50ms promedio.
    expect(snap.tasaErrorPct).toBe(10);
    expect(snap.tiempoRespuestaPromedioMs).toBe(50);
  });

  test('serieUltimaHora rellena con 0 los minutos sin bucket y respeta el orden cronológico', () => {
    const snap = construirSnapshot(baseInput({
      minutosValidos: [minuto(-59, { total: 7, c5xx: 2 }), minuto(0, { total: 4, c5xx: 0 })],
    }));

    expect(snap.serieUltimaHora[0]).toMatchObject({ total: 7, errores: 2 }); // el más viejo, primero
    expect(snap.serieUltimaHora[59]).toMatchObject({ total: 4, errores: 0 }); // el actual, al final
    expect(snap.serieUltimaHora.slice(1, 59).every((p) => p.total === 0)).toBe(true);
  });

  test('memoria se reporta en MB redondeados', () => {
    const snap = construirSnapshot(baseInput());
    expect(snap.memoria).toEqual({ rssMb: 100, heapUsedMb: 40, heapTotalMb: 80 });
  });

  test('erroresRecientes se devuelve más-reciente-primero (orden invertido respecto al insumo)', () => {
    const snap = construirSnapshot(baseInput({
      erroresRecientes: [
        { ts: 1, metodo: 'GET', path: '/a', status: 500 },
        { ts: 2, metodo: 'GET', path: '/b', status: 502 },
      ],
    }));

    expect(snap.erroresRecientes.map((e) => e.path)).toEqual(['/b', '/a']);
  });

  test('estadoGeneral "caido" cuando Mongo está desconectado, propagado end-to-end', () => {
    const snap = construirSnapshot(baseInput({ mongoOk: false }));
    expect(snap.estadoGeneral).toBe('caido');
    expect(snap.salud.mongo).toBe('desconectado');
  });

  test('sin loadavg/memHost (compatibilidad hacia atrás): cpu/memoriaHost quedan en 0, sin romper', () => {
    const snap = construirSnapshot(baseInput());
    expect(snap.cpu).toEqual({ load1: 0, load5: 0, load15: 0, cores: 0 });
    expect(snap.memoriaHost).toEqual({ totalMb: 0, freeMb: 0, usadoPct: 0 });
  });

  test('cpu/memoriaHost se reportan correctamente cuando se pasan datos reales', () => {
    const snap = construirSnapshot(baseInput({
      loadavg: [2.5, 1.8, 1.2],
      cpuCores: 4,
      memHostTotalBytes: 16 * 1024 * 1024 * 1024, // 16 GB
      memHostFreeBytes: 4 * 1024 * 1024 * 1024,   // 4 GB libres → 75% usado
    }));

    expect(snap.cpu).toEqual({ load1: 2.5, load5: 1.8, load15: 1.2, cores: 4 });
    expect(snap.memoriaHost).toEqual({ totalMb: 16384, freeMb: 4096, usadoPct: 75 });
  });

  test('estadoGeneral "degradado" por CPU alta del host, propagado end-to-end', () => {
    const snap = construirSnapshot(baseInput({ loadavg: [8, 6, 5], cpuCores: 4 }));
    expect(snap.estadoGeneral).toBe('degradado');
  });
});

// _minutosRecientesSinTrafico() NO se exporta — se ejercita acá pasando por
// construirSnapshot() con buckets de minuto reales (no un booleano ya calculado a
// mano), cruzando el borde exacto 1 vs. 2 minutos de silencio (CRITICAL pedido en
// revisión de confiabilidad).
describe('detección real de "caido" por silencio (vía construirSnapshot, sin atajos)', () => {
  test('1 minuto de silencio (el anterior tuvo tráfico) — NO es caido', () => {
    const snap = construirSnapshot(baseInput({
      minutosValidos: [
        minuto(-2, { total: 5 }), // hubo tráfico hace 2 minutos
        // minuto(-1) ausente = silencio ese minuto — pero no alcanza 2 seguidos
      ],
    }));

    expect(snap.estadoGeneral).toBe('normal');
  });

  test('EXACTAMENTE 2 minutos de silencio consecutivos tras haber tenido tráfico — SÍ es caido', () => {
    const snap = construirSnapshot(baseInput({
      minutosValidos: [
        minuto(-3, { total: 5 }), // hubo tráfico en la hora, hace 3 minutos
        // minuto(-1) y minuto(-2) ausentes = 2 minutos consecutivos sin nada
      ],
    }));

    expect(snap.estadoGeneral).toBe('caido');
  });

  test('nunca hubo tráfico en la hora (server recién arrancado) — silencio NO se confunde con caida', () => {
    const snap = construirSnapshot(baseInput({ minutosValidos: [] }));
    expect(snap.estadoGeneral).toBe('normal');
  });
});

describe('getSnapshot (orquestador impuro — mongoose/sequelize/tracker/process mockeados)', () => {
  let memSpy;
  let uptimeSpy;

  beforeEach(() => {
    jest.clearAllMocks();
    memSpy = jest.spyOn(process, 'memoryUsage').mockReturnValue(MEMORY_USAGE);
    uptimeSpy = jest.spyOn(process, 'uptime').mockReturnValue(1234);
  });

  afterEach(() => {
    memSpy.mockRestore();
    uptimeSpy.mockRestore();
  });

  test('arma el snapshot combinando tracker + salud de las DBs + proceso', async () => {
    tracker.getEstadoCrudo.mockReturnValue({
      ahoraMs: AHORA_MS, enCurso: 2, segundos: [], minutos: [], erroresRecientes: [],
    });
    checkMongoOk.mockReturnValue(true);
    checkPostgresOk.mockResolvedValue(true);
    eventLoopLag.leerYReiniciarLagMs.mockReturnValue(3.4);

    const snap = await getSnapshot();

    expect(snap.requestsEnCurso).toBe(2);
    expect(snap.salud).toEqual({ mongo: 'conectado', postgres: 'conectado' });
    expect(snap.eventLoopLagMs).toBe(3.4);
    expect(snap.uptimeSegundos).toBe(1234);
    expect(snap.memoria).toEqual({ rssMb: 100, heapUsedMb: 40, heapTotalMb: 80 });
    expect(snap.estadoGeneral).toBe('normal');
  });

  test('Mongo desconectado (checkMongoOk=false) se refleja en salud y en estadoGeneral "caido"', async () => {
    tracker.getEstadoCrudo.mockReturnValue({
      ahoraMs: AHORA_MS, enCurso: 0, segundos: [], minutos: [], erroresRecientes: [],
    });
    checkMongoOk.mockReturnValue(false);
    checkPostgresOk.mockResolvedValue(true);
    eventLoopLag.leerYReiniciarLagMs.mockReturnValue(0);

    const snap = await getSnapshot();

    expect(snap.salud.mongo).toBe('desconectado');
    expect(snap.estadoGeneral).toBe('caido');
  });

  test('Postgres desconectado (checkPostgresOk=false) se refleja en salud y en estadoGeneral "degradado"', async () => {
    tracker.getEstadoCrudo.mockReturnValue({
      ahoraMs: AHORA_MS, enCurso: 0, segundos: [], minutos: [], erroresRecientes: [],
    });
    checkMongoOk.mockReturnValue(true);
    checkPostgresOk.mockResolvedValue(false);
    eventLoopLag.leerYReiniciarLagMs.mockReturnValue(0);

    const snap = await getSnapshot();

    expect(snap.salud.postgres).toBe('desconectado');
    expect(snap.estadoGeneral).toBe('degradado');
  });
});
