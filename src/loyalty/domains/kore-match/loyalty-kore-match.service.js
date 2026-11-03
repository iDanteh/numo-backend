'use strict';

/**
 * loyalty/domains/kore-match/loyalty-kore-match.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — match de clientes entre LoyaltyCard (identidad =
 * teléfono) y Kore (ERP/monedero externo). Cruza por `tarjetaLegada` (número
 * de Tarjeta del CSV legado, ya normalizado y persistido en LoyaltyCard — ver
 * loyalty-import-apply.service.js) contra `monedero` de Kore (normalizado con
 * el mismo criterio, join exacto por número, ya verificado contra 12 pares
 * reales — no hace falta matching difuso por nombre).
 *
 * Idempotente — se puede llamar tantas veces como se quiera (ej. re-correrlo
 * tras cargar más clientes en Kore): solo UPDATEa `koreClienteId`/`koreMonedero`
 * en las tarjetas que matchean, nunca toca nada más. Pensado para exponerse
 * por `POST /loyalty/import/kore-match` (ver loyalty-import.routes.js), que el
 * frontend va a consumir desde "Importar clientes" en una tarea futura.
 *
 * BACKGROUND JOB (revisión 4-lens 2026-10-22, hallazgo CRÍTICO): `obtenerClientesConMonedero()`
 * pagina ~20 páginas remotas contra Kore (hasta 3 reintentos con backoff de hasta 60s cada uno) y
 * el cruce puede terminar actualizando hasta ~2,000 filas de Postgres de forma secuencial — correr
 * esto síncrono adentro de una request HTTP revienta cualquier timeout (proxy/cliente), igual que
 * el caso ya resuelto en `loyalty-import-apply.service.js` (bcrypt del NIP, ver su docstring).
 * Mismo patrón YA establecido en ESTE MISMO router (`POST /aplicar` → `iniciarAplicarJob` /
 * `_correrAplicarJob`): el endpoint responde 202+jobId de inmediato, el trabajo corre después sin
 * bloquear la respuesta, el progreso se emite por socket (`lealtad-import:kore-match:progress`) a
 * quien lo disparó, y el resultado final se recupera vía socket (`...:done`/`...:error`) o,
 * como fallback de recuperación tras un reload, vía `GET /kore-match-job/:jobId` →
 * `obtenerEstadoMatchKoreJob` (mismo criterio que `obtenerEstadoAplicarJob`: chequeo de dueño por
 * `userId`). A diferencia de `/aplicar`, acá no hace falta partir en LOTES/transacciones — ya no
 * hay nada transaccional que aislar entre filas (ver fix de `erroresActualizacion` abajo), así que
 * un solo job envuelve a `correrMatchKore()` completo, con progreso emitido periódicamente vía el
 * callback opcional `onProgress`.
 */

const { Op } = require('sequelize');
const { LoyaltyCard } = require('../../../shared/models/postgres');
const { normalizarTarjeta } = require('../import/loyalty-import.parser');
const { obtenerClientesConMonedero } = require('./loyalty-kore-monedero.client');
const { emitToUser } = require('../../../banks/shared/socket');
const { logger } = require('../../../shared/utils/logger');

// Listas de "sin match"/duplicados/errores en el resumen: truncadas para no devolver un JSON
// gigante si el universo real tiene miles de filas — el conteo total SIEMPRE es completo (via
// *_count), solo la lista de ejemplos se corta acá.
const MAX_EJEMPLOS_SIN_MATCH = 100;

// Granularidad del progreso emitido durante el loop principal — mismo orden de magnitud que
// BATCH_SIZE en loyalty-import-apply.service.js, para no spamear un evento de socket por cada
// cliente de Kore procesado (el universo real hoy es ~1,968).
const PROGRESS_EVERY = 200;

/**
 * Corre el match completo: trae todas las LoyaltyCard con `tarjetaLegada` no nulo, trae todos los
 * clientes de Kore con monedero, cruza por `tarjetaLegada === normalizarTarjeta(monedero)` y
 * persiste `koreClienteId`/`koreMonedero` en las que matchean.
 *
 * `onProgress({ procesados, total })` es opcional (usado por `_correrMatchKoreJob` para emitir
 * progreso por socket) — sin él, la función se comporta exactamente igual que antes (tests
 * existentes la siguen llamando sin argumentos).
 *
 * Fix reliability (revisión 4-lens 2026-10-22):
 *  - `monedero` duplicado en Kore (dos clientes DISTINTOS de Kore con el mismo `monedero`
 *    normalizado, dato externo sucio): antes, el segundo cliente procesado pisaba en silencio el
 *    match del primero (mismo `koreClienteId`/`koreMonedero` sobreescrito sin dejar rastro). Ahora
 *    se detecta vía `tarjetasMatcheadas` (ya existía para el resumen `sinMatchEnKore`, se reusa
 *    como "ya fue reclamado en esta corrida"): el cliente de Kore que llega DESPUÉS para un
 *    `monedero` ya reclamado se cuenta en `monederosDuplicadosEnKore` y NO se aplica — el primer
 *    match (orden de `obtenerClientesConMonedero()`) gana. El caso simétrico (2+ LoyaltyCard con la
 *    MISMA `tarjetaLegada`) ya estaba cubierto: ahí no hay ambigüedad real, un solo cliente de Kore
 *    legítimamente corresponde a 2+ tarjetas y se actualizan todas.
 *  - `card.update(...)` ya no puede abortar el resto de la corrida: una fila que falla (ej. error
 *    transitorio de DB) se registra en `erroresActualizacion` (con log individual) y el loop sigue
 *    con las demás — mismo criterio que `_correrAplicarJob` en loyalty-import-apply.service.js.
 */
async function correrMatchKore({ onProgress } = {}) {
  const [cards, clientesKore] = await Promise.all([
    LoyaltyCard.findAll({ where: { tarjetaLegada: { [Op.ne]: null } } }),
    obtenerClientesConMonedero(),
  ]);

  // Índice por tarjetaLegada ya normalizada — O(1) por cliente de Kore en vez de O(n) por tarjeta.
  // `tarjetaLegada` en LoyaltyCard ya se persiste normalizada (ver loyalty-import-apply.service.js),
  // así que no se vuelve a normalizar acá.
  const cardsPorTarjeta = new Map();
  for (const card of cards) {
    if (!card.tarjetaLegada) continue; // defensivo: el where ya filtra NULL, pero no cadena vacía
    if (!cardsPorTarjeta.has(card.tarjetaLegada)) cardsPorTarjeta.set(card.tarjetaLegada, []);
    cardsPorTarjeta.get(card.tarjetaLegada).push(card);
  }

  const tarjetasMatcheadas = new Set();
  const clientesSinMatchEnLealtad = [];
  const clientesConMonederoDuplicado = [];
  const erroresActualizacion = [];
  // Detalle COMPLETO de los matcheados, sin truncar a MAX_EJEMPLOS_SIN_MATCH (a diferencia de las
  // listas de error de abajo) — alimenta el Excel que el frontend genera automáticamente al
  // terminar el job (ver importar-clientes.component.ts), con monedero legado + monedero Kore de
  // TODOS los clientes, no solo una muestra. `sinMatchEnKoreDetalle` (declarado más abajo) sigue el
  // mismo criterio para la 2da pestaña del Excel (tarjetas sin match, pedido 2026-10-23).
  const matcheadosDetalle = [];
  let matcheados = 0;
  let procesados = 0;

  for (const cliente of clientesKore) {
    const monederoNormalizado = normalizarTarjeta(cliente.monedero);
    const candidatos = monederoNormalizado ? (cardsPorTarjeta.get(monederoNormalizado) ?? []) : [];

    if (candidatos.length === 0) {
      clientesSinMatchEnLealtad.push(cliente);
    } else if (tarjetasMatcheadas.has(monederoNormalizado)) {
      // `monedero` ya reclamado por un cliente de Kore ANTERIOR en esta misma corrida — dato
      // externo sucio (2 clientes de Kore distintos comparten el mismo monedero normalizado). No
      // se sobreescribe el match ya aplicado: se cuenta como conflictivo y se descarta.
      clientesConMonederoDuplicado.push(cliente);
      logger.warn(
        `[KoreMatch] monedero duplicado en Kore: cliente ${cliente.id} (monedero="${cliente.monedero}") `
        + `ignorado — ese monedero ya fue aplicado por otro cliente de Kore en esta misma corrida.`,
      );
    } else {
      tarjetasMatcheadas.add(monederoNormalizado);
      for (const card of candidatos) {
        try {
          // eslint-disable-next-line no-await-in-loop -- secuencial a propósito, universo acotado
          // (miles de filas, no millones) y cada UPDATE es independiente — no hace falta batchear.
          await card.update({ koreClienteId: cliente.id, koreMonedero: cliente.monedero });
          matcheados += 1;
          matcheadosDetalle.push({
            telefono: card.telefono,
            nombre: card.nombre,
            tarjetaLegada: card.tarjetaLegada,
            koreClienteId: cliente.id,
            koreMonedero: cliente.monedero,
          });
        } catch (err) {
          erroresActualizacion.push({ cardId: card.id, telefono: card.telefono, error: err.message });
          logger.error(
            `[KoreMatch] card ${card.id} (${card.telefono ?? 's/d'}) falló al actualizar match con `
            + `Kore cliente ${cliente.id}, se omite y se continúa: ${err.message}`,
          );
        }
      }
    }

    procesados += 1;
    if (onProgress && (procesados % PROGRESS_EVERY === 0 || procesados === clientesKore.length)) {
      onProgress({ procesados, total: clientesKore.length });
    }
  }

  const tarjetasSinMatchEnKore = cards.filter((card) => !tarjetasMatcheadas.has(card.tarjetaLegada));
  // Detalle COMPLETO (sin truncar a MAX_EJEMPLOS_SIN_MATCH) de las tarjetas de Lealtad que no
  // matchearon — a diferencia de `sinMatchEnKore` de abajo (acotada, pensada para el modal "ver
  // ejemplos" del frontend), esta alimenta la 2da pestaña del Excel automático (pedido explícito
  // 2026-10-23: rastreo manual de los clientes que quedaron sin monedero Kore).
  const sinMatchEnKoreDetalle = tarjetasSinMatchEnKore.map((card) => ({
    cardId: card.id, telefono: card.telefono, nombre: card.nombre, tarjetaLegada: card.tarjetaLegada,
  }));

  logger.info(
    `[KoreMatch] totalKore=${clientesKore.length} totalCardsConTarjetaLegada=${cards.length} `
    + `matcheados=${matcheados} sinMatchEnKore=${tarjetasSinMatchEnKore.length} `
    + `sinMatchEnLealtad=${clientesSinMatchEnLealtad.length} `
    + `monederosDuplicadosEnKore=${clientesConMonederoDuplicado.length} `
    + `erroresActualizacion=${erroresActualizacion.length}`,
  );

  return {
    totalKore: clientesKore.length,
    totalCardsConTarjetaLegada: cards.length,
    matcheados,
    matcheadosDetalle,
    sinMatchEnKoreCount: tarjetasSinMatchEnKore.length,
    sinMatchEnKore: sinMatchEnKoreDetalle.slice(0, MAX_EJEMPLOS_SIN_MATCH),
    sinMatchEnKoreDetalle,
    sinMatchEnLealtadCount: clientesSinMatchEnLealtad.length,
    sinMatchEnLealtad: clientesSinMatchEnLealtad.slice(0, MAX_EJEMPLOS_SIN_MATCH).map((cliente) => ({
      koreClienteId: cliente.id, nombre: cliente.nombre, razonSocial: cliente.razonSocial, monedero: cliente.monedero,
    })),
    monederosDuplicadosEnKoreCount: clientesConMonederoDuplicado.length,
    monederosDuplicadosEnKore: clientesConMonederoDuplicado.slice(0, MAX_EJEMPLOS_SIN_MATCH).map((cliente) => ({
      koreClienteId: cliente.id, nombre: cliente.nombre, razonSocial: cliente.razonSocial, monedero: cliente.monedero,
    })),
    erroresActualizacionCount: erroresActualizacion.length,
    erroresActualizacion: erroresActualizacion.slice(0, MAX_EJEMPLOS_SIN_MATCH),
  };
}

// ── Match de Kore en BACKGROUND (mismo patrón que loyalty-import-apply.service.js#iniciarAplicarJob
// / netpay-reporte.service.js#iniciarCargaReporteJob — ver docstring del archivo) ───────────────
const LEALTAD_KORE_MATCH_JOBS = new Map(); // jobId -> { status, userId, procesados, total, resultado?, error? }
const LEALTAD_KORE_MATCH_JOB_TTL = 2 * 60 * 60 * 1000; // 2h — mismo criterio que LEALTAD_APPLY_JOB_TTL

function _nuevoJobIdKoreMatch() {
  return `lealtad-kore-match-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Corre `correrMatchKore()` de principio a fin, actualizando el estado del job y emitiendo
 * progreso/resultado por socket a quien lo disparó (`emitToUser`, acción personal). Nunca deja el
 * job colgado en 'running': cualquier error (ej. `bancos.TOKEN` vacío, Kore caído tras
 * agotar los 3 reintentos) se captura y pasa el job a 'error', con el mensaje disponible tanto por
 * socket como por el fallback de polling.
 */
async function _correrMatchKoreJob(jobId, user) {
  try {
    const resultado = await correrMatchKore({
      onProgress: ({ procesados, total }) => {
        const job = LEALTAD_KORE_MATCH_JOBS.get(jobId);
        if (job) { job.procesados = procesados; job.total = total; }
        emitToUser(user?._id, 'lealtad-import:kore-match:progress', {
          jobId, procesados, total, pct: total > 0 ? Math.round((procesados / total) * 100) : 100,
        });
      },
    });

    LEALTAD_KORE_MATCH_JOBS.set(jobId, {
      status: 'done',
      userId: user?._id ?? null,
      procesados: resultado.totalKore,
      total: resultado.totalKore,
      resultado,
    });
    emitToUser(user?._id, 'lealtad-import:kore-match:done', { jobId, ...resultado });
  } catch (err) {
    const jobPrevio = LEALTAD_KORE_MATCH_JOBS.get(jobId);
    const mensaje = err.message;
    LEALTAD_KORE_MATCH_JOBS.set(jobId, {
      status: 'error',
      userId: user?._id ?? null,
      procesados: jobPrevio?.procesados ?? 0,
      total: jobPrevio?.total ?? 0,
      error: mensaje,
    });
    emitToUser(user?._id, 'lealtad-import:kore-match:error', { jobId, error: mensaje });
    logger.error(`[KoreMatch] job ${jobId} falló de forma inesperada: ${mensaje}`);
  } finally {
    setTimeout(() => LEALTAD_KORE_MATCH_JOBS.delete(jobId), LEALTAD_KORE_MATCH_JOB_TTL);
  }
}

/**
 * Arranca el match con Kore en background y devuelve `{ jobId }` de inmediato — ver
 * `POST /api/loyalty/import/kore-match` en loyalty-import.routes.js, que responde 202 con esto.
 */
function iniciarMatchKoreJob(user) {
  const jobId = _nuevoJobIdKoreMatch();
  LEALTAD_KORE_MATCH_JOBS.set(jobId, {
    status: 'running', userId: user?._id ?? null, procesados: 0, total: 0,
  });

  // Sin await a propósito — mismo patrón que loyalty-import-apply.service.js#_iniciarJobDesdeFilasClasificadas:
  // es justo lo que permite responder 202 de inmediato sin bloquear la request.
  _correrMatchKoreJob(jobId, user);

  return { jobId };
}

/**
 * Fallback de recuperación tras un reload de página — mismo criterio que
 * `obtenerEstadoAplicarJob`/`obtenerEstadoJobCarga`: el socket es la vía normal de enterarse del
 * progreso, esto es solo para cuando alguien recarga a mitad del job y pierde la suscripción en
 * memoria. SÍ hay chequeo de dueño (userId).
 */
function obtenerEstadoMatchKoreJob(jobId, userId) {
  const job = LEALTAD_KORE_MATCH_JOBS.get(jobId);
  if (!job || String(job.userId) !== String(userId)) return null;
  const { userId: _userId, ...estado } = job;
  return estado;
}

module.exports = {
  correrMatchKore,
  iniciarMatchKoreJob,
  obtenerEstadoMatchKoreJob,
  _correrMatchKoreJob, // exportado solo para tests (mismo criterio que _correrAplicarJob)
};
