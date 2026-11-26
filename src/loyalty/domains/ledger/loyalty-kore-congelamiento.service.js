'use strict';

/**
 * domains/ledger/loyalty-kore-congelamiento.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — webhook Kore→Numo de "congelamiento por inactividad"
 * (2026-11-26, pedido explícito del usuario), punto de entrada de
 * `POST /api/loyalty/kore/congelamiento`.
 *
 * REEMPLAZA POR COMPLETO al cron diario que vivía en
 * `loyalty/jobs/congelamientoPuntosCron.js` + `congelamiento-puntos.service.js`
 * (ambos borrados en este cambio, junto con el config `CONGELAMIENTO_MESES` y
 * el reporte "Puntos por vencer" — ninguno de los tres tiene sentido ya que
 * Kore controla la cadencia, no Numo). Antes Numo decidía, vía un número de
 * meses configurado a mano, cuándo una tarjeta se consideraba inactiva. Ahora
 * Kore hace esa evaluación de su lado y avisa a Numo con DOS eventos de
 * negocio independientes, cada uno con su propia cadencia:
 *
 *   - `cancelacion_puntos`: Kore detectó que la tarjeta no tuvo venta en X
 *     meses — Numo resetea su saldo de puntos a 0 (AJUSTE real en el ledger,
 *     mismo motivo `congelamiento_inactividad` que ya usaba el job viejo).
 *   - `desactivacion_tarjeta`: Kore desactiva la tarjeta cada X meses (otra
 *     cadencia, independiente de la anterior) — Numo pone `activa: false`.
 *
 * Por qué esto es un webhook PROPIO y no 2 eventos más del `POST /clientes`
 * ya existente (`loyalty-kore-cliente.routes.js`, evento `baja`): son
 * semánticas de negocio distintas. `baja` en `/clientes` significa "el
 * cliente se dio de baja del catálogo"; `desactivacion_tarjeta` acá significa
 * "Kore detectó inactividad y desactivó la tarjeta por su propia regla de
 * negocio". Mezclarlos en el mismo evento le haría perder a cualquiera que
 * revise el historial la respuesta a "¿por qué se tocó esta tarjeta?" — el
 * `accion` que cada uno deja en `LoyaltyCardAuditLog` (`webhook_kore_baja` vs.
 * `webhook_kore_desactivacion`) es justamente lo que preserva esa
 * trazabilidad.
 *
 * Identidad: a diferencia de `_buscarCard` en `loyalty-kore-cliente.service.js`
 * (donde `telefono` es un FALLBACK si no hay match por `koreClienteId`), acá
 * el usuario pidió explícitamente que `telefono` sea OBLIGATORIO como
 * verificación de seguridad — no como fallback — "para asegurarnos que se
 * está manipulando la tarjeta correcta": se resuelve la tarjeta por
 * `koreClienteId` (identidad estable) y se exige que su `telefono` coincida
 * exactamente con el recibido; si no coincide, se rechaza el evento entero
 * (409) en vez de arriesgarse a tocar una tarjeta equivocada por un
 * `koreClienteId` corrupto o mal mapeado del lado de Kore.
 *
 * Idempotencia:
 *   - `cancelacion_puntos` mueve puntos reales → idempotencia REAL, mismo
 *     patrón exacto que `loyalty-kore-venta-retiro.service.js#procesarVentaRetiroKore`
 *     (ver ese archivo para el detalle completo): se busca
 *     `referenciaExterna = "congelamiento:<koreEventoId>"` ANTES de tocar
 *     nada; si ya existe, se responde "ya cancelado" sin duplicar. La
 *     tarjeta se bloquea (`LOCK.UPDATE`) dentro de una transacción antes de
 *     escribir, y una carrera que gane el insert (`SequelizeUniqueConstraintError`)
 *     se trata igual de benigna que en venta-retiro.
 *   - `desactivacion_tarjeta` es idempotente por naturaleza: poner
 *     `activa: false` sobre una tarjeta ya inactiva es un no-op (no hace
 *     falta una clave de idempotencia dedicada, a diferencia del caso
 *     financiero de arriba).
 *
 * CONTRATO IMPORTANTE con Kore (dejarlo explícito también en el Swagger del
 * router): `_aplicarMovimiento` (loyalty-ledger.service.js) tira
 * `UnprocessableError` (422) si la tarjeta ya está `activa: false` — esa es
 * una invariante YA existente del ledger (D4), no algo que este webhook
 * invente o esquive. Esto significa que si Kore manda `cancelacion_puntos`
 * DESPUÉS de `desactivacion_tarjeta` para la misma tarjeta, el webhook va a
 * responder 422 a propósito — mismo criterio que venta-retiro: "un error acá
 * es un error real, se propaga tal cual", sin reintentos ocultos de este
 * lado. Kore debe mandar `cancelacion_puntos` antes o independientemente de
 * `desactivacion_tarjeta`, nunca después.
 */

const { Transaction } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const {
  LoyaltyCard, LoyaltyPointsLedger, LoyaltyAdjustmentReason,
} = require('../../../shared/models/postgres');
const { BadRequestError, ConflictError, NotFoundError } = require('../../../shared/errors/AppError');
const { toCents, fromCents, normalizarTelefono } = require('../../shared/puntos.util');
const { _aplicarMovimiento } = require('./loyalty-ledger.service');
const { registrarCambioTarjeta } = require('../cards/loyalty-card-audit.service');
const { logger } = require('../../../shared/utils/logger');
const { emitToLoyaltyCard, emitToAll } = require('../../../banks/shared/socket');

const ORIGEN_KORE = 'kore';
const REASON_CLAVE = 'congelamiento_inactividad';
const EVENTOS_VALIDOS = ['cancelacion_puntos', 'desactivacion_tarjeta'];

// Actor sintético para `updatedById`/`updatedByNombre` y `LoyaltyCardAuditLog` — mismo criterio
// que `ACTOR_WEBHOOK` de loyalty-kore-cliente.service.js, nombre propio para distinguir en
// cualquier auditoría futura que esto vino del webhook de congelamiento, no del de catálogo.
const ACTOR_WEBHOOK = { dbId: 'kore-webhook-congelamiento', nombre: 'Kore (webhook congelamiento)' };

function _requerido(valor, nombreCampo) {
  if (valor === undefined || valor === null || String(valor).trim() === '') {
    throw new BadRequestError(`${nombreCampo} es requerido.`);
  }
  return String(valor).trim();
}

function _referenciaCancelacion(koreEventoId) {
  return `congelamiento:${koreEventoId}`;
}

/**
 * A diferencia de `_buscarCard` de loyalty-kore-cliente.service.js, acá `telefono` NUNCA es un
 * fallback de búsqueda — es una verificación obligatoria de que `koreClienteId` y `telefono`
 * apuntan a la MISMA tarjeta (pedido explícito del usuario, ver docstring del archivo).
 */
async function _resolverCard({ koreClienteId, telefono }) {
  const card = await LoyaltyCard.findOne({ where: { koreClienteId } });
  if (!card) {
    throw new NotFoundError(`Tarjeta de Lealtad (koreClienteId="${koreClienteId}")`);
  }
  if (card.telefono !== telefono) {
    throw new ConflictError(
      `El teléfono recibido ("${telefono}") no coincide con el registrado para koreClienteId="${koreClienteId}" ("${card.telefono}") — se rechaza el evento para evitar tocar la tarjeta equivocada.`,
    );
  }
  return card;
}

async function _procesarCancelacionPuntos({ koreClienteId, telefono, koreEventoId, motivo }) {
  const referenciaExterna = _referenciaCancelacion(koreEventoId);

  // Idempotencia REAL, ANTES de resolver la tarjeta o tocar cualquier otra cosa — mismo criterio
  // que procesarVentaRetiroKore.
  const yaCancelado = await LoyaltyPointsLedger.findOne({
    where: { origen: ORIGEN_KORE, referenciaExterna },
  });
  if (yaCancelado) {
    emitToLoyaltyCard(yaCancelado.cardId, 'lealtad-tarjeta:actualizada', { cardId: yaCancelado.cardId, motivo: 'congelamiento-kore' });
    return {
      ok: true, evento: 'cancelacion_puntos', cardId: yaCancelado.cardId, yaCancelado: true,
    };
  }

  const card = await _resolverCard({ koreClienteId, telefono });

  const justificacion = `Cancelación automática de puntos por inactividad (webhook Kore, evento ${koreEventoId})${motivo ? ` — ${motivo}` : ''}`;

  try {
    const resultado = await sequelize.transaction(async (t) => {
      // Mismo patrón que procesarVentaRetiroKore/registrarAjuste: la tarjeta se bloquea
      // (LOCK.UPDATE) antes de decidir nada, para evitar TOCTOU contra cualquier otro movimiento
      // concurrente de la misma tarjeta.
      const cardLocked = await LoyaltyCard.findByPk(card.id, { transaction: t, lock: Transaction.LOCK.UPDATE });
      if (!cardLocked) throw new NotFoundError('Tarjeta de Lealtad');

      const saldoActualCents = toCents(cardLocked.saldoPuntos);
      // Mismo criterio defensivo que tenía el job viejo: una tarjeta ya en saldo 0 nunca se
      // vuelve a tocar (sin esto, un AJUSTE de -0 generaría una fila de ledger vacía de sentido).
      if (saldoActualCents <= 0) {
        return {
          ok: true, evento: 'cancelacion_puntos', cardId: cardLocked.id, puntosCancelados: 0, saldoResultante: 0, yaEnCero: true,
        };
      }

      const reason = await LoyaltyAdjustmentReason.findOne({ where: { clave: REASON_CLAVE } });
      if (!reason || !reason.activo) {
        throw new BadRequestError(`El motivo de ajuste "${REASON_CLAVE}" no existe o está inactivo (correr seed-lealtad.js).`);
      }

      const entry = await _aplicarMovimiento(t, cardLocked, {
        tipo:   'AJUSTE',
        puntos: fromCents(-saldoActualCents),
        reasonId: reason.id,
        justificacion,
        origen: ORIGEN_KORE,
        referenciaExterna,
      });

      return {
        ok: true,
        evento: 'cancelacion_puntos',
        cardId: cardLocked.id,
        puntosCancelados: fromCents(saldoActualCents),
        saldoResultante: Number(entry.saldoResultante),
      };
    });

    if (!resultado.yaEnCero) {
      emitToLoyaltyCard(card.id, 'lealtad-tarjeta:actualizada', { cardId: card.id, motivo: 'congelamiento-kore' });
    }
    return resultado;
  } catch (err) {
    if (err?.name === 'SequelizeUniqueConstraintError') {
      // Carrera: otro request con el mismo koreEventoId (reenvío de Kore en paralelo) ganó el
      // insert de la fila de cancelación entre el chequeo de idempotencia de arriba y este
      // insert — ya quedó cancelado. Mismo criterio que procesarVentaRetiroKore.
      logger.warn(`[loyalty-kore-congelamiento] koreEventoId=${koreEventoId}: carrera detectada al insertar la cancelación, ya la aplicó otro request concurrente.`);
      emitToLoyaltyCard(card.id, 'lealtad-tarjeta:actualizada', { cardId: card.id, motivo: 'congelamiento-kore' });
      return { ok: true, evento: 'cancelacion_puntos', cardId: card.id, yaCancelado: true };
    }
    throw err;
  }
}

async function _procesarDesactivacionTarjeta({ koreClienteId, telefono }) {
  const card = await _resolverCard({ koreClienteId, telefono });

  if (!card.activa) {
    // Idempotente por naturaleza — desactivar algo ya desactivado es un no-op, no hace falta
    // clave de idempotencia dedicada (a diferencia de cancelacion_puntos, que sí mueve puntos).
    return {
      ok: true, evento: 'desactivacion_tarjeta', cardId: card.id, yaDesactivada: true,
    };
  }

  const antes = { activa: true };
  await card.update({
    activa:          false,
    updatedById:     ACTOR_WEBHOOK.dbId,
    updatedByNombre: ACTOR_WEBHOOK.nombre,
  });

  // `accion` es STRING(30) en LoyaltyCardAuditLog — 'webhook_kore_desactivacion' (26 caracteres)
  // entra bien. Nombre PROPIO, distinto de 'webhook_kore_baja' (ver docstring del archivo): la
  // trazabilidad de POR QUÉ se tocó la tarjeta depende de que estos dos nunca se confundan.
  await registrarCambioTarjeta({
    cardId: card.id, antes, despues: { activa: false }, accion: 'webhook_kore_desactivacion', origen: ORIGEN_KORE,
  });

  emitToAll('lealtad-cliente-kore:actualizado', { cardId: card.id });
  return { ok: true, evento: 'desactivacion_tarjeta', cardId: card.id };
}

/**
 * Punto de entrada del webhook `POST /api/loyalty/kore/congelamiento`.
 * `evento`: 'cancelacion_puntos' | 'desactivacion_tarjeta'. `koreClienteId`, `koreEventoId` y
 * `telefono` siempre requeridos (ver docstring del archivo sobre por qué `telefono` es
 * obligatorio acá, a diferencia de `/clientes`). `motivo` es opcional, solo para auditoría/log.
 */
async function procesarCongelamientoKore(payload) {
  const {
    evento, koreClienteId, koreEventoId, telefono, motivo,
  } = payload ?? {};

  if (!EVENTOS_VALIDOS.includes(evento)) {
    throw new BadRequestError(`evento inválido: "${evento}". Debe ser uno de: ${EVENTOS_VALIDOS.join(', ')}.`);
  }

  const koreClienteIdTrim = _requerido(koreClienteId, 'koreClienteId');
  const koreEventoIdTrim = _requerido(koreEventoId, 'koreEventoId');
  const telefonoTrim = _requerido(telefono, 'telefono');

  let telefonoNorm;
  try {
    telefonoNorm = normalizarTelefono(telefonoTrim);
  } catch (err) {
    throw new BadRequestError(err.message);
  }

  if (evento === 'cancelacion_puntos') {
    return _procesarCancelacionPuntos({
      koreClienteId: koreClienteIdTrim, telefono: telefonoNorm, koreEventoId: koreEventoIdTrim, motivo: motivo ?? null,
    });
  }

  return _procesarDesactivacionTarjeta({ koreClienteId: koreClienteIdTrim, telefono: telefonoNorm });
}

module.exports = { procesarCongelamientoKore };
