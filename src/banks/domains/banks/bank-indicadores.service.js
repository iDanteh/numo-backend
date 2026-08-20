'use strict';

const BankMovement = require('./BankMovement.model');

const MS_PER_HOUR = 3600000;

// Boundaries del $bucket de backlog: [0,24) < 24h, [24,72) 1-3d, [72,168) 3-7d, [168,∞) 7d+.
const BACKLOG_BOUNDARIES = [0, 24, 72, 168, Number.MAX_SAFE_INTEGER];

// Fecha de corte del dashboard completo (tiempo Y backlog): decisión explícita del usuario
// (2026-08-17) de medir SOLO desde que se implementa este indicador en adelante, para no
// ensuciar el promedio ni el backlog con historial viejo que nunca se pensó medir. Reemplaza
// al anterior split histórico/nuevo vía `backlogPreExistente` (ver BankMovement.model.js y
// scripts/migrate-backlog-preexistente.js, ahora sin uso). Mismo criterio de construcción
// (hora local del servidor) que applyDateRange() más abajo. Si el deploy real de este cambio
// ocurre en otra fecha, ACTUALIZAR este valor a mano antes de desplegar.
const INDICADORES_DESDE = new Date(2026, 7, 17);

// Estatus que cuentan como "pendiente" para el backlog: no_identificado (nunca se tocó) y
// reclasificado (se identificó mal y quedó otra vez esperando revisión) — ambos son trabajo
// real todavía sin cerrar. "otros" queda afuera a propósito: es un estatus terminal, no un
// pendiente disfrazado.
const BACKLOG_STATUSES = ['no_identificado', 'reclasificado'];
const BACKLOG_KEY_BY_BOUNDARY = { 0: 'menos24h', 24: 'de1a3d', 72: 'de3a7d', 168: 'mas7d' };
const BACKLOG_DEFAULT = Object.freeze({ menos24h: 0, de1a3d: 0, de3a7d: 0, mas7d: 0 });

// Horario laboral usado para "horas hábiles" del promedio/mediana de identificación —
// decisión explícita del usuario (2026-08-17): 8:00-20:00, lunes a SÁBADO (el sábado
// cuenta como día laboral completo — el usuario dijo "excluye noches y domingos", no
// "fines de semana"). Domingo completo = 0 horas hábiles sin importar el horario.
const HORA_INICIO_LABORAL = 8;
const HORA_FIN_LABORAL    = 20;
const DIA_DOMINGO         = 0; // Date#getDay()

/**
 * Filtro plano banco/categoria compartido por las 3 agregaciones. A diferencia de
 * getCards()/getStatusStats() (bank.service.js), que agrupan por banco+categoria para
 * armar el breakdown `porCategoria` de las cards, este endpoint no necesita ese desglose —
 * banco/categoria se aplican directo como campos del $match.
 *
 * `deposito: {$gt:0}` y `oculto: {$ne:true}` replican EXACTO el criterio de getCards()
 * (bank.service.js) — todo este dominio trata "identificado/no_identificado/reclasificado"
 * como estatus de DEPÓSITOS (el KPI de arriba se llama literalmente "Estatus de depósitos");
 * un retiro casi nunca se identifica y quedaba contando como backlog sin que nadie fuera a
 * actuar sobre él. Sin este filtro, el backlog mostraba miles de retiros mezclados con los
 * depósitos reales por identificar — un número técnicamente correcto pero inútil para
 * priorizar trabajo, y además inconsistente con el resto del dashboard.
 */
function buildBaseMatch({ banco, categoria } = {}) {
  const match = { isActive: true, oculto: { $ne: true }, deposito: { $gt: 0 } };
  if (banco) match.banco = banco;
  if (categoria) match.categoria = categoria;
  return match;
}

// Mismo criterio EXACTO de rango de fecha que getCards() (bank.service.js) — evita
// introducir una inconsistencia de zona horaria/rango distinta a la que ya existe ahí.
function applyDateRange(match, year, month) {
  if (!year) return match;
  const y = parseInt(year, 10);
  const m = month ? parseInt(month, 10) : null;
  match.fecha = (m && m >= 1 && m <= 12)
    ? { $gte: new Date(y, m - 1, 1), $lt: new Date(y, m, 1) }
    : { $gte: new Date(y, 0, 1), $lt: new Date(y + 1, 0, 1) };
  return match;
}

// Mapea el array {_id, count}[] que devuelve $bucket a las 4 llaves fijas del backlog.
// Mongo omite las llaves sin documentos — hay que default-earlas explícitamente a 0.
function mapBacklogBuckets(buckets) {
  const out = { ...BACKLOG_DEFAULT };
  for (const b of buckets) {
    const key = BACKLOG_KEY_BY_BOUNDARY[b._id];
    if (key) out[key] = b.count;
  }
  return out;
}

/**
 * Horas hábiles entre 2 timestamps: lunes-sábado, 8:00-20:00 (hora local del servidor,
 * mismo criterio que applyDateRange() arriba). Domingo completo y las horas fuera de
 * 8-20 en cualquier día NO cuentan. Recorre día por día (acotado: la cantidad de días
 * entre inicio/fin de un caso real de identificación es chica, nunca miles) y suma el
 * solape de cada día con la ventana [inicio, fin] — así una franja que cruza varios
 * días (ej. viernes a la noche → lunes) se reparte bien entre los días que sí cuentan.
 *
 * No calculado en el pipeline de Mongo a propósito: esta lógica de calendario (saltar
 * domingos, recortar cada día a su ventana laboral) sería un `$reduce` de agregación
 * ilegible e imposible de testear con confianza — se resuelve en JS, sobre los pocos
 * cientos/miles de documentos que trae getIndicadoresIdentificacion() con find().lean().
 */
function horasHabilesEntre(inicio, fin) {
  if (!(fin > inicio)) return 0;
  let totalMs = 0;
  let cursor = new Date(inicio.getFullYear(), inicio.getMonth(), inicio.getDate());
  while (cursor < fin) {
    if (cursor.getDay() !== DIA_DOMINGO) {
      const ventanaInicio = new Date(cursor); ventanaInicio.setHours(HORA_INICIO_LABORAL, 0, 0, 0);
      const ventanaFin    = new Date(cursor); ventanaFin.setHours(HORA_FIN_LABORAL, 0, 0, 0);
      const solapeInicio = ventanaInicio > inicio ? ventanaInicio : inicio;
      const solapeFin    = ventanaFin    < fin   ? ventanaFin    : fin;
      if (solapeFin > solapeInicio) totalMs += solapeFin.getTime() - solapeInicio.getTime();
    }
    cursor = new Date(cursor.getFullYear(), cursor.getMonth(), cursor.getDate() + 1);
  }
  return totalMs / MS_PER_HOUR;
}

function promedio(valores) {
  if (!valores.length) return null;
  return valores.reduce((a, b) => a + b, 0) / valores.length;
}

// Mediana: menos sensible que el promedio a outliers (ej. un puñado de movimientos que
// tardaron semanas por vacaciones/un banco raro) — da una lectura más honesta de "cuánto
// tarda normalmente el equipo" que un promedio que esos casos pueden inflar solos.
function mediana(valores) {
  if (!valores.length) return null;
  const ordenados = [...valores].sort((a, b) => a - b);
  const mid = Math.floor(ordenados.length / 2);
  return ordenados.length % 2 === 0
    ? (ordenados[mid - 1] + ordenados[mid]) / 2
    : ordenados[mid];
}

/**
 * Indicadores de tiempo de identificación de movimientos bancarios — cuánto tarda un
 * usuario en marcar un depósito como `identificado` desde que se cargó en Numo
 * (`primeraIdentificacionAt - createdAt`, en HORAS HÁBILES — ver horasHabilesEntre()).
 * Acotado a depósitos (deposito > 0, sin oculto), igual criterio que getCards() — ver
 * buildBaseMatch().
 *
 * @param {object} [opts]
 * @param {string} [opts.banco]
 * @param {string} [opts.categoria]
 * @param {string|number} [opts.year]  - limita el promedio/mediana general y el desglose
 *   por usuario a ese año (y mes, si también viene). El backlog NO se acota por year/month:
 *   la antigüedad de un pendiente se mide contra AHORA, no contra un periodo pasado (y
 *   sigue en tiempo de RELOJ, no horas hábiles — ver Pipeline 2 abajo).
 *   El backlog cuenta status no_identificado + reclasificado (BACKLOG_STATUSES) — ambos
 *   son trabajo real sin cerrar; "otros" queda afuera por ser un estatus terminal.
 *   Tanto el promedio como el backlog están acotados además a `createdAt >= INDICADORES_DESDE`
 *   (ver constante arriba) — el dashboard completo mide solo desde su propia implementación.
 * @param {string|number} [opts.month]
 */
async function getIndicadoresIdentificacion({ banco, categoria, year, month } = {}) {
  const matchConFecha = applyDateRange(buildBaseMatch({ banco, categoria }), year, month);
  const matchSoloBancoCategoria = { ...buildBaseMatch({ banco, categoria }), createdAt: { $gte: INDICADORES_DESDE } };
  const matchTiempo = {
    ...matchConFecha,
    status: 'identificado',
    primeraIdentificacionAt: { $ne: null },
    createdAt: { $gte: INDICADORES_DESDE },
  };

  const [identificados, backlogAgg] = await Promise.all([
    // Trae los documentos ya identificados (equipo completo, desde INDICADORES_DESDE) para
    // calcular horas hábiles en JS — ver horasHabilesEntre() arriba sobre por qué esto no
    // se hace dentro de la agregación de Mongo. Con los volúmenes actuales (cientos/pocos
    // miles desde el cutoff) traer los documentos a Node es perfectamente razonable; si el
    // volumen creciera mucho con los años, valdría la pena revisar el enfoque (ej. mover el
    // cálculo a un job que lo materialice), pero no hace falta resolverlo ahora.
    BankMovement.find(matchTiempo)
      .select('createdAt primeraIdentificacionAt primeraIdentificacionPor')
      .lean(),
    // Pipeline 2 — backlog de pendientes (no_identificado + reclasificado, BACKLOG_STATUSES)
    // por antigüedad (sin year/month; equipo completo), desde INDICADORES_DESDE. Sigue en
    // tiempo de RELOJ a propósito — el usuario pidió horas hábiles para los promedios, no
    // para la antigüedad del backlog (decisión de alcance explícita, no un olvido).
    BankMovement.aggregate([
      { $match: { ...matchSoloBancoCategoria, status: { $in: BACKLOG_STATUSES } } },
      { $project: { horas: { $divide: [{ $subtract: ['$$NOW', '$createdAt'] }, MS_PER_HOUR] } } },
      { $bucket: { groupBy: '$horas', boundaries: BACKLOG_BOUNDARIES, default: 'otro', output: { count: { $sum: 1 } } } },
    ], { allowDiskUse: true }),
  ]);

  const conHoras = identificados.map(d => ({
    horas:  horasHabilesEntre(d.createdAt, d.primeraIdentificacionAt),
    userId: d.primeraIdentificacionPor?.userId ?? null,
    nombre: d.primeraIdentificacionPor?.nombre ?? null,
  }));
  const todasLasHoras = conHoras.map(d => d.horas);

  // Desglose por usuario — agrupado en JS sobre las mismas horas hábiles ya calculadas
  // (misma definición de "horas" que el promedio/mediana del equipo, para que sea
  // comparable). Solo promedio por usuario, sin mediana — la tabla ya es compacta.
  const porUsuarioMap = new Map();
  for (const d of conHoras) {
    const key = d.userId ?? '__sin_usuario__';
    if (!porUsuarioMap.has(key)) {
      porUsuarioMap.set(key, { userId: d.userId, nombre: d.nombre, horas: [] });
    }
    porUsuarioMap.get(key).horas.push(d.horas);
  }
  const porUsuario = [...porUsuarioMap.values()]
    .map(u => ({
      userId: u.userId,
      nombre: u.nombre,
      promedioHoras: promedio(u.horas),
      count: u.horas.length,
    }))
    .sort((a, b) => b.count - a.count);

  return {
    promedioHoras: promedio(todasLasHoras),
    medianaHoras:  mediana(todasLasHoras),
    totalIdentificadosConDato: todasLasHoras.length,
    backlog: mapBacklogBuckets(backlogAgg ?? []),
    porUsuario,
  };
}

// promedio/mediana también se exportan para collection-request-indicadores.service.js
// (mismo dominio conceptual — tiempo de identificación — pero acotado a Solicitudes de
// Cobro, ver ese archivo).
module.exports = { getIndicadoresIdentificacion, horasHabilesEntre, promedio, mediana };
