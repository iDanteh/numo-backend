'use strict';

const os = require('os');
const { checkMongoOk, checkPostgresOk } = require('../shared/utils/db-health');
const tracker = require('./traffic-tracker.middleware');
const eventLoopLag = require('./event-loop-lag.util');

// Ventana (minutos hacia atrás, SIN contar el minuto en curso — está incompleto y
// distorsiona la tasa) usada para tasa de error y tiempo de respuesta promedio.
const VENTANA_TASA_ERROR_MIN = 5;
// Con menos muestras que esto en la ventana, no se declara "degradado" por tasa de
// error — 1 error de 1 request es 100% pero no dice nada real del sistema.
const MIN_MUESTRAS_TASA_ERROR = 5;
const UMBRAL_TASA_ERROR_DEGRADADO = 0.10; // 10%
const UMBRAL_LAG_DEGRADADO_MS = 200;
// Minutos consecutivos SIN ninguna request (excluyendo el minuto en curso) que,
// habiendo habido tráfico en la última hora, se interpretan como una caída real —
// no un rato tranquilo cualquiera.
const MINUTOS_SIN_TRAFICO_CAIDO = 2;

function _sumar(buckets, campo) {
  return buckets.reduce((acc, b) => acc + b[campo], 0);
}

/** true si ninguno de los últimos `n` minutos COMPLETOS (excluye el actual, en
 *  curso) tuvo tráfico. */
function _minutosRecientesSinTrafico(minutosValidos, ahoraMinuto, n) {
  const porTs = new Map(minutosValidos.map((b) => [b.ts, b]));
  for (let i = 1; i <= n; i += 1) {
    if ((porTs.get(ahoraMinuto - i)?.total ?? 0) > 0) return false;
  }
  return true;
}

/**
 * Clasifica el estado general del sistema. Umbrales explícitos y deliberadamente
 * conservadores (pocos falsos positivos > detectar todo): Mongo caído es SIEMPRE
 * "caido" (el sistema no funciona sin él); silencio total tras haber tenido
 * tráfico es la señal de "se cayó" que pidió el usuario; Postgres caído o tasa de
 * error/lag altos son "degradado" (el sistema funciona pero mal).
 */
function calcularEstadoGeneral({ huboTraficoEnLaHora, sinTraficoReciente, mongoOk, pgOk, tasaErrorFraccion, muestrasTasaError, lagMs, load1, cpuCores }) {
  if (!mongoOk) return 'caido';
  if (huboTraficoEnLaHora && sinTraficoReciente) return 'caido';
  if (!pgOk) return 'degradado';
  if (muestrasTasaError >= MIN_MUESTRAS_TASA_ERROR && tasaErrorFraccion > UMBRAL_TASA_ERROR_DEGRADADO) return 'degradado';
  if (lagMs > UMBRAL_LAG_DEGRADADO_MS) return 'degradado';
  // load1 (carga promedio del último minuto, os.loadavg()[0]) YA es un promedio
  // sostenido de 1 minuto completo — a diferencia de un % de CPU instantáneo, un
  // pico de una sola muestra no puede disparar esto solo, no hace falta debounce
  // extra. cpuCores<=0 (entorno raro, o Windows en dev donde loadavg() siempre da
  // 0) nunca dispara esta condición.
  if (cpuCores > 0 && load1 > cpuCores) return 'degradado';
  return 'normal';
}

/**
 * Función PURA — arma el snapshot completo a partir de datos ya leídos (nunca lee
 * process/mongoose/sequelize directo), para poder testear cada escenario de
 * `estadoGeneral` con buckets construidos a mano, sin mockear el mundo real.
 */
function construirSnapshot({
  ahoraMs, enCurso, segundosValidos, minutosValidos, erroresRecientes, mongoOk, pgOk, lagMs, memoryUsage, uptimeSegundos,
  // Defaults por compatibilidad con los tests/llamadas previas a esta métrica —
  // ver mejora #4 (2026-09-24): CPU/memoria REALES del host, no del proceso Node.
  loadavg = [0, 0, 0], cpuCores = 0, memHostTotalBytes = 0, memHostFreeBytes = 0,
}) {
  const ahoraMinuto = Math.floor(ahoraMs / 60000);

  const requestsPorMinuto = _sumar(segundosValidos, 'total');

  // Ventana de tasa de error / tiempo de respuesta: minutos completos, excluye el actual.
  const ventana = minutosValidos.filter((b) => b.ts >= ahoraMinuto - VENTANA_TASA_ERROR_MIN && b.ts < ahoraMinuto);
  const totalVentana = _sumar(ventana, 'total');
  const erroresVentana = _sumar(ventana, 'c5xx');
  const sumaDuracionVentana = _sumar(ventana, 'sumaDuracionMs');
  const muestrasDuracionVentana = _sumar(ventana, 'muestrasDuracion');

  const tasaErrorFraccion = totalVentana > 0 ? erroresVentana / totalVentana : 0;
  const tiempoRespuestaPromedioMs = muestrasDuracionVentana > 0 ? sumaDuracionVentana / muestrasDuracionVentana : null;

  const huboTraficoEnLaHora = minutosValidos.some((b) => b.total > 0);
  const sinTraficoReciente = _minutosRecientesSinTrafico(minutosValidos, ahoraMinuto, MINUTOS_SIN_TRAFICO_CAIDO);

  const estadoGeneral = calcularEstadoGeneral({
    huboTraficoEnLaHora,
    sinTraficoReciente,
    mongoOk,
    pgOk,
    tasaErrorFraccion,
    muestrasTasaError: totalVentana,
    lagMs,
    load1: loadavg[0],
    cpuCores,
  });

  // 60 puntos (más viejo → más nuevo), rellenando con 0 los minutos sin tráfico.
  const porTs = new Map(minutosValidos.map((b) => [b.ts, b]));
  const serieUltimaHora = [];
  for (let i = 59; i >= 0; i -= 1) {
    const ts = ahoraMinuto - i;
    const b = porTs.get(ts);
    serieUltimaHora.push({ ts: ts * 60000, total: b?.total ?? 0, errores: b?.c5xx ?? 0 });
  }

  return {
    generadoEn: new Date(ahoraMs).toISOString(),
    estadoGeneral,
    requestsPorMinuto,
    requestsEnCurso: enCurso,
    tasaErrorPct: Math.round(tasaErrorFraccion * 10000) / 100,
    tiempoRespuestaPromedioMs: tiempoRespuestaPromedioMs != null ? Math.round(tiempoRespuestaPromedioMs) : null,
    uptimeSegundos: Math.round(uptimeSegundos),
    // memoria: SOLO del proceso Node (heap/rss) — cuánta RAM usa Numo puntualmente.
    // memoriaHost: la RAM del SERVIDOR completo (otros procesos, SO, caché de
    // disco, etc.) — son métricas DISTINTAS a propósito, no confundir una con
    // otra: un proceso Node liviano puede convivir con un host casi sin memoria
    // libre por otra causa.
    memoria: {
      rssMb: Math.round(memoryUsage.rss / 1048576),
      heapUsedMb: Math.round(memoryUsage.heapUsed / 1048576),
      heapTotalMb: Math.round(memoryUsage.heapTotal / 1048576),
    },
    memoriaHost: {
      totalMb: Math.round(memHostTotalBytes / 1048576),
      freeMb: Math.round(memHostFreeBytes / 1048576),
      usadoPct: memHostTotalBytes > 0
        ? Math.round(((memHostTotalBytes - memHostFreeBytes) / memHostTotalBytes) * 10000) / 100
        : 0,
    },
    // Carga promedio del SISTEMA OPERATIVO (1/5/15 min) — en Windows, Node
    // siempre devuelve [0,0,0] (no soportado por el SO), inofensivo: nunca da
    // falso positivo de degradación, solo no aporta señal en desarrollo local.
    cpu: { load1: loadavg[0], load5: loadavg[1], load15: loadavg[2], cores: cpuCores },
    eventLoopLagMs: Math.round(lagMs * 10) / 10,
    salud: {
      mongo: mongoOk ? 'conectado' : 'desconectado',
      postgres: pgOk ? 'conectado' : 'desconectado',
    },
    serieUltimaHora,
    // Más reciente primero — es como se lee una tabla de "errores recientes".
    erroresRecientes: erroresRecientes.slice().reverse(),
  };
}

/** Orquestador impuro: junta el estado crudo del tracker con el estado real de
 *  Mongo/Postgres/proceso y arma el snapshot. Es lo que consume la ruta HTTP. */
async function getSnapshot() {
  const raw = tracker.getEstadoCrudo();
  const mongoOk = checkMongoOk();
  const pgOk = await checkPostgresOk();
  const lagMs = eventLoopLag.leerYReiniciarLagMs();

  return construirSnapshot({
    ahoraMs: raw.ahoraMs,
    enCurso: raw.enCurso,
    segundosValidos: raw.segundos,
    minutosValidos: raw.minutos,
    erroresRecientes: raw.erroresRecientes,
    mongoOk,
    pgOk,
    lagMs,
    memoryUsage: process.memoryUsage(),
    uptimeSegundos: process.uptime(),
    loadavg: os.loadavg(),
    cpuCores: os.cpus().length,
    memHostTotalBytes: os.totalmem(),
    memHostFreeBytes: os.freemem(),
  });
}

module.exports = { getSnapshot, construirSnapshot, calcularEstadoGeneral };
