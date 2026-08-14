'use strict';

const BankMovement = require('./BankMovement.model');
const { MOVEMENT_SCOPE } = require('../../../shared/config/rbac');

const MS_PER_HOUR = 3600000;

// Boundaries del $bucket de backlog: [0,24) < 24h, [24,72) 1-3d, [72,168) 3-7d, [168,∞) 7d+.
const BACKLOG_BOUNDARIES = [0, 24, 72, 168, Number.MAX_SAFE_INTEGER];

// Estatus que cuentan como "pendiente" para el backlog: no_identificado (nunca se tocó) y
// reclasificado (se identificó mal y quedó otra vez esperando revisión) — ambos son trabajo
// real todavía sin cerrar. "otros" queda afuera a propósito: es un estatus terminal, no un
// pendiente disfrazado.
const BACKLOG_STATUSES = ['no_identificado', 'reclasificado'];
const BACKLOG_KEY_BY_BOUNDARY = { 0: 'menos24h', 24: 'de1a3d', 72: 'de3a7d', 168: 'mas7d' };
const BACKLOG_DEFAULT = Object.freeze({ menos24h: 0, de1a3d: 0, de3a7d: 0, mas7d: 0 });

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
 * Indicadores de tiempo de identificación de movimientos bancarios — cuánto tarda un
 * usuario en marcar un depósito como `identificado` desde que se cargó en Numo
 * (`primeraIdentificacionAt - createdAt`). Acotado a depósitos (deposito > 0, sin oculto),
 * igual criterio que getCards() — ver buildBaseMatch().
 *
 * @param {object} [opts]
 * @param {string} [opts.banco]
 * @param {string} [opts.categoria]
 * @param {string|number} [opts.year]  - limita el promedio general y el desglose por
 *   usuario a ese año (y mes, si también viene). El backlog NO se acota por year/month:
 *   la antigüedad de un pendiente se mide contra AHORA, no contra un periodo pasado.
 *   El backlog cuenta status no_identificado + reclasificado (BACKLOG_STATUSES) — ambos
 *   son trabajo real sin cerrar; "otros" queda afuera por ser un estatus terminal.
 *   Viene partido en `backlog.historico` / `backlog.nuevo` según el flag INMUTABLE
 *   `backlogPreExistente` (estampado una sola vez por scripts/migrate-backlog-preexistente.js
 *   en el momento del deploy de este split) — nunca por comparar `createdAt` contra una
 *   fecha de corte dinámica, que se corrompería con reimportaciones tardías de Excels
 *   viejos y con movimientos revertidos después del corte.
 * @param {string|number} [opts.month]
 * @param {{scope: 'own'|'all', userId: string}|null} [opts.restrictions] - null = acceso
 *   completo (banks:config). Mismo criterio que getCards() para "Identificados": el
 *   promedio general y el backlog son siempre del equipo completo; solo el desglose "por
 *   usuario" se acota a `restrictions.userId` cuando scope === MOVEMENT_SCOPE.OWN.
 */
async function getIndicadoresIdentificacion({ banco, categoria, year, month, restrictions } = {}) {
  const matchConFecha = applyDateRange(buildBaseMatch({ banco, categoria }), year, month);
  const matchSoloBancoCategoria = buildBaseMatch({ banco, categoria });
  const ownUserId = restrictions?.scope === MOVEMENT_SCOPE.OWN ? restrictions.userId : null;

  const [tiempoAgg, backlogAgg, porUsuarioAgg] = await Promise.all([
    // Pipeline 1 — promedio de tiempo de identificación (equipo completo).
    BankMovement.aggregate([
      { $match: { ...matchConFecha, status: 'identificado', primeraIdentificacionAt: { $ne: null } } },
      { $project: { horas: { $divide: [{ $subtract: ['$primeraIdentificacionAt', '$createdAt'] }, MS_PER_HOUR] } } },
      { $group: { _id: null, promedioHoras: { $avg: '$horas' }, n: { $sum: 1 } } },
    ], { allowDiskUse: true }),
    // Pipeline 2 — backlog de pendientes (no_identificado + reclasificado, BACKLOG_STATUSES)
    // por antigüedad (sin year/month; equipo completo), partido en 2 grupos vía $facet (una
    // sola query, más barato que 2 aggregate() separados): "historico" = ya era backlog
    // antes del deploy de este split (backlogPreExistente:true, estampado UNA VEZ por
    // migrate-backlog-preexistente.js) y "nuevo" = apareció después (false/default). Evita
    // que un backlog histórico enorme se mezcle para siempre con el que el equipo genera
    // desde que se empezó a medir esto.
    BankMovement.aggregate([
      { $match: { ...matchSoloBancoCategoria, status: { $in: BACKLOG_STATUSES } } },
      { $project: { horas: { $divide: [{ $subtract: ['$$NOW', '$createdAt'] }, MS_PER_HOUR] }, backlogPreExistente: 1 } },
      {
        $facet: {
          historico: [
            { $match: { backlogPreExistente: true } },
            { $bucket: { groupBy: '$horas', boundaries: BACKLOG_BOUNDARIES, default: 'otro', output: { count: { $sum: 1 } } } },
          ],
          nuevo: [
            { $match: { backlogPreExistente: { $ne: true } } },
            { $bucket: { groupBy: '$horas', boundaries: BACKLOG_BOUNDARIES, default: 'otro', output: { count: { $sum: 1 } } } },
          ],
        },
      },
    ], { allowDiskUse: true }),
    // Pipeline 3 — desglose por usuario que identificó primero (acotado a userId si scope OWN).
    BankMovement.aggregate([
      {
        $match: {
          ...matchConFecha,
          status: 'identificado',
          primeraIdentificacionAt: { $ne: null },
          ...(ownUserId ? { 'primeraIdentificacionPor.userId': ownUserId } : {}),
        },
      },
      {
        $group: {
          _id: '$primeraIdentificacionPor.userId',
          nombre: { $first: '$primeraIdentificacionPor.nombre' },
          promedioHoras: { $avg: { $divide: [{ $subtract: ['$primeraIdentificacionAt', '$createdAt'] }, MS_PER_HOUR] } },
          count: { $sum: 1 },
        },
      },
      { $sort: { count: -1 } },
    ], { allowDiskUse: true }),
  ]);

  return {
    promedioHoras: tiempoAgg[0]?.promedioHoras ?? null,
    totalIdentificadosConDato: tiempoAgg[0]?.n ?? 0,
    backlog: {
      historico: mapBacklogBuckets(backlogAgg[0]?.historico ?? []),
      nuevo:     mapBacklogBuckets(backlogAgg[0]?.nuevo ?? []),
    },
    porUsuario: porUsuarioAgg.map(r => ({
      userId: r._id ?? null,
      nombre: r.nombre ?? null,
      promedioHoras: r.promedioHoras,
      count: r.count,
    })),
  };
}

module.exports = { getIndicadoresIdentificacion };
