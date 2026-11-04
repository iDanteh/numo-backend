'use strict';

/**
 * jobs/congelamiento-puntos.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — "Congelamiento de puntos" por inactividad (2026-11-03,
 * feature ya CONFIRMADA por el usuario). Corrida diaria (ver
 * congelamientoPuntosCron.js): toda tarjeta con saldo > 0 cuya ÚLTIMA
 * ACUMULACIÓN real tiene más de `CONGELAMIENTO_MESES` (configurable, ver
 * loyalty-config.service.js#obtenerCongelamientoMeses) se resetea a 0 puntos
 * vía un AJUSTE real (ledgerService.registrarAjuste) — auditable con el
 * historial existente del ledger, sin ninguna UI nueva.
 *
 * Reglas (confirmadas, no renegociables):
 *   - Tarjeta SIN ninguna ACUMULACIÓN registrada nunca se toca (no hay fecha
 *     contra qué comparar).
 *   - Tarjeta ya en saldo 0 no se toca de nuevo → idempotente (el WHERE
 *     saldoPuntos > 0 ya la excluye del batch; se repite el chequeo adentro
 *     del loop como defensivo).
 *   - Si el cliente vuelve a comprar, la próxima ACUMULACIÓN reinicia el
 *     contador solo: la fecha se recalcula vía MAX(created_at) en cada
 *     corrida, no hay ningún estado propio ("ultimaAcumulacionAt") que
 *     resetear a mano.
 *   - Última acumulación calculada con UNA query agregada (GROUP BY) sobre
 *     loyalty_points_ledger — nunca N+1, nunca denormalizada en LoyaltyCard
 *     (es un job diario en batch).
 *   - Cada tarjeta en su propio try/catch: una falla no debe frenar las
 *     demás (mismo criterio que banks/jobs/cajaTransferenciaSyncCron.js).
 */

const { Op, fn, col } = require('sequelize');
const { LoyaltyCard, LoyaltyPointsLedger, LoyaltyAdjustmentReason } = require('../../shared/models/postgres');
const configService = require('../domains/config/loyalty-config.service');
const ledgerService = require('../domains/ledger/loyalty-ledger.service');

const REASON_CLAVE = 'congelamiento_inactividad';

// Usuario sintético (pedido explícito del usuario): dbId null — no hay un usuario staff real
// detrás de esta corrida automática. `_userLabel()` de loyalty-ledger.service.js ya tolera
// dbId/nombre así: usuarioId queda null, usuarioNombre queda el label de sistema de abajo.
const USUARIO_SISTEMA = { dbId: null, nombre: 'Sistema (congelamiento por inactividad)' };

function _restarMeses(fecha, meses) {
  const d = new Date(fecha);
  d.setMonth(d.getMonth() - meses);
  return d;
}

// Inverso de _restarMeses — usado por tarjetasPorVencer() para proyectar la fecha límite de
// congelamiento hacia ADELANTE desde la última acumulación (en vez de hacia atrás desde hoy).
function _sumarMeses(fecha, meses) {
  const d = new Date(fecha);
  d.setMonth(d.getMonth() + meses);
  return d;
}

/**
 * Mapa cardId → fecha de la ACUMULACIÓN más reciente, en UNA sola query agregada (GROUP BY) —
 * nunca N+1 contra loyalty_points_ledger. Una tarjeta sin ninguna fila ACUMULACION simplemente no
 * aparece en el mapa (se detecta con `.has()`/valor undefined, nunca asumiendo que null signifique
 * "nunca acumuló").
 */
async function _ultimasAcumulacionesPorTarjeta(cardIds) {
  if (!cardIds.length) return new Map();

  const filas = await LoyaltyPointsLedger.findAll({
    attributes: ['cardId', [fn('MAX', col('created_at')), 'ultimaAcumulacion']],
    where:      { tipo: 'ACUMULACION', cardId: { [Op.in]: cardIds } },
    group:      ['cardId'],
    raw:        true,
  });

  const mapa = new Map();
  for (const fila of filas) {
    mapa.set(fila.cardId, fila.ultimaAcumulacion);
  }
  return mapa;
}

/**
 * Corrida completa del congelamiento. Devuelve un resumen (consumido por el cron solo para
 * loguear) — nunca tira si una tarjeta individual falla: esa falla queda contada en `errores` y el
 * resto del batch sigue.
 */
async function congelarTarjetasInactivas() {
  const meses = await configService.obtenerCongelamientoMeses();
  if (!meses) {
    console.log('[CongelamientoPuntos] Feature deshabilitada (CONGELAMIENTO_MESES sin configurar o en 0) — no se procesa nada.');
    return { deshabilitado: true, evaluadas: 0, congeladas: 0, errores: 0 };
  }

  const reason = await LoyaltyAdjustmentReason.findOne({ where: { clave: REASON_CLAVE } });
  if (!reason || !reason.activo) {
    console.error(`[CongelamientoPuntos] Motivo '${REASON_CLAVE}' no existe o está inactivo (correr seed-lealtad.js) — no se procesa nada.`);
    return { deshabilitado: true, evaluadas: 0, congeladas: 0, errores: 0 };
  }

  const tarjetas = await LoyaltyCard.findAll({ where: { saldoPuntos: { [Op.gt]: 0 } } });
  if (!tarjetas.length) {
    console.log('[CongelamientoPuntos] Ninguna tarjeta con saldo > 0 — nada que evaluar.');
    return { deshabilitado: false, evaluadas: 0, congeladas: 0, errores: 0 };
  }

  const ultimasAcumulaciones = await _ultimasAcumulacionesPorTarjeta(tarjetas.map((t) => t.id));
  const limite = _restarMeses(new Date(), meses);

  let congeladas = 0;
  let errores = 0;

  for (const tarjeta of tarjetas) {
    // Defensivo: el WHERE saldoPuntos > 0 de arriba ya debería excluirla, pero una tarjeta en 0
    // nunca debe generar un segundo ajuste (idempotencia explícita del prompt).
    if (!(Number(tarjeta.saldoPuntos) > 0)) continue;

    const ultimaAcumulacion = ultimasAcumulaciones.get(tarjeta.id);
    if (!ultimaAcumulacion) continue; // nunca tuvo una ACUMULACIÓN real → nunca se toca
    if (new Date(ultimaAcumulacion) > limite) continue; // dentro del umbral, sigue activa

    // eslint-disable-next-line no-await-in-loop -- batch diario, secuencial a propósito: cada
    // tarjeta en su propio try/catch, mismo criterio que cajaTransferenciaSyncCron.js.
    try {
      await ledgerService.registrarAjuste({
        cardId:        tarjeta.id,
        puntos:        -Number(tarjeta.saldoPuntos),
        reasonId:      reason.id,
        justificacion: `Congelamiento automático por ${meses} meses sin actividad`,
      }, USUARIO_SISTEMA);
      congeladas += 1;
    } catch (err) {
      errores += 1;
      console.error(`[CongelamientoPuntos] Error congelando tarjeta ${tarjeta.id}: ${err.message}`);
    }
  }

  console.log(`[CongelamientoPuntos] Evaluadas ${tarjetas.length}, congeladas ${congeladas}, errores ${errores}.`);
  return { deshabilitado: false, evaluadas: tarjetas.length, congeladas, errores };
}

/**
 * "Puntos por vencer" (2026-11-03, reporte de Lealtad) — tarjetas con saldo > 0 cuya fecha límite
 * de congelamiento (última ACUMULACIÓN real + `CONGELAMIENTO_MESES`) cae dentro de los próximos
 * `diasVentana` días. Reusa LA MISMA query agregada de última acumulación que
 * `congelarTarjetasInactivas()` — nunca se duplica esa lógica (pedido explícito del prompt).
 *
 * Mismo criterio que el job de congelamiento:
 *   - Sin `CONGELAMIENTO_MESES` configurado (o en 0) → sin umbral del cual contar, reporte vacío.
 *   - Tarjeta sin ninguna ACUMULACIÓN real nunca entra (no hay fecha contra qué proyectar).
 *   - Tarjeta cuya fecha límite ya pasó (debería estar congelada en la próxima corrida del job,
 *     o la corrida de hoy ya la tocó) NO es "por vencer" — ya venció o está por vencer HOY mismo
 *     en la corrida del cron, no es información accionable de anticipación.
 */
async function tarjetasPorVencer({ diasVentana = 30 } = {}) {
  const meses = await configService.obtenerCongelamientoMeses();
  if (!meses) return [];

  const tarjetas = await LoyaltyCard.findAll({ where: { saldoPuntos: { [Op.gt]: 0 } } });
  if (!tarjetas.length) return [];

  const ultimasAcumulaciones = await _ultimasAcumulacionesPorTarjeta(tarjetas.map((t) => t.id));
  const ahora = new Date();
  const limiteVentana = new Date(ahora.getTime() + diasVentana * 24 * 60 * 60 * 1000);
  const msPorDia = 24 * 60 * 60 * 1000;

  const resultado = [];
  for (const tarjeta of tarjetas) {
    const ultimaAcumulacion = ultimasAcumulaciones.get(tarjeta.id);
    if (!ultimaAcumulacion) continue; // nunca tuvo una ACUMULACIÓN real → mismo criterio que el job

    const fechaLimite = _sumarMeses(new Date(ultimaAcumulacion), meses);
    if (fechaLimite <= ahora) continue;          // ya venció (o vence hoy) — no es "por vencer"
    if (fechaLimite > limiteVentana) continue;   // fuera de la ventana pedida

    resultado.push({
      id:            tarjeta.id,
      telefono:      tarjeta.telefono,
      nombre:        tarjeta.nombre,
      nivel:         tarjeta.nivel,
      saldoPuntos:   Number(tarjeta.saldoPuntos),
      fechaLimite:   fechaLimite.toISOString(),
      diasRestantes: Math.ceil((fechaLimite.getTime() - ahora.getTime()) / msPorDia),
    });
  }

  return resultado;
}

module.exports = { congelarTarjetasInactivas, tarjetasPorVencer };
