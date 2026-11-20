'use strict';

/**
 * domains/ledger/loyalty-kore-reversion-abono.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — reversión del efecto de puntos (ACUMULACION/CANJE/PROMOCION)
 * de UN abono puntual de Kore, disparada desde `POST /api/erp/cxc-reversiones`
 * (dominio `banks/erp`) cuando el payload trae `referencia` (== `idAbono`, ver
 * proposal.md del cambio `cxc-reversiones-puntos-abono`).
 *
 * Deliberadamente un ARCHIVO NUEVO, no una extensión de
 * `loyalty-kore-venta-retiro.service.js` (D1 del design; renombrado el
 * 2026-11-20 desde su nombre anterior asociado a cancelación de venta): ese
 * archivo sigue siendo el único dueño del retiro de VENTA completo (parcial/total,
 * prorrateado); este archivo revierte un abono puntual sin tocar el resto de la
 * CxC ni de la venta — son dos eventos de negocio distintos.
 *
 * No-op silencioso (D5) — NUNCA lanza para "nada que revertir": abono sin efecto
 * de lealtad, abono ya revertido antes (idempotencia), o guard (D2) que determina
 * que una cancelación de venta previa ya lo cubrió. Solo un error genuinamente
 * inesperado (DB caída) se propaga — lo maneja el try/catch del caller en
 * `erp-reversion.service.js` (D6), que nunca bloquea ni revierte el procesamiento
 * bancario que sigue.
 */

const { Transaction, Op } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const { LoyaltyCard, LoyaltyPointsLedger } = require('../../../shared/models/postgres');
const { toCents, fromCents } = require('../../shared/puntos.util');
const { _aplicarMovimiento } = require('./loyalty-ledger.service');
const { logger } = require('../../../shared/utils/logger');
const { emitToLoyaltyCard } = require('../../../banks/shared/socket');

const ORIGEN_KORE = 'kore';

function _refReversionAbono(serieFolio, idAbono) { return `${serieFolio}:${idAbono}:REVERSION`; }

// Cubre ambas formas de reversión que pueden convivir bajo el mismo serieFolio: la de ESTE
// archivo (`${serieFolio}:${idAbono}:REVERSION`, sin segundo ':') y la de
// `loyalty-kore-venta-retiro.service.js` (`${serieFolio}:REVERSION:${serieFolioRetiro}`,
// sin idAbono — D2: por eso el guard necesita el agregado COMPLETO del serieFolio, una query
// filtrada por idAbono nunca vería esta segunda forma).
function _esCualquierReversion(referenciaExterna) {
  return referenciaExterna?.includes(':REVERSION');
}

/**
 * Revierte el efecto de lealtad (ACUMULACION de nivel/promoción + CANJE) de UN abono puntual de
 * Kore. No-op silencioso (D5) si el abono nunca tuvo efecto de lealtad, si ya fue revertido antes
 * (idempotencia), o si el guard (D2) determina que una cancelación de venta previa ya lo cubrió.
 */
async function revertirPuntosPorAbono({ serieFolio, idAbono, motivo }) {
  const filas = await LoyaltyPointsLedger.findAll({
    where: { origen: ORIGEN_KORE, referenciaExterna: { [Op.like]: `${serieFolio}:%` } },
  });

  const filasDeEsteAbono = filas.filter(f => f.referenciaExterna.startsWith(`${serieFolio}:${idAbono}:`));
  const yaRevertidoEsteAbono = filasDeEsteAbono.some(f => f.referenciaExterna === _refReversionAbono(serieFolio, idAbono));
  if (yaRevertidoEsteAbono) return { reverted: false }; // idempotencia: ya se procesó este aviso antes

  const abonoOriginales = filasDeEsteAbono.filter(f => !f.referenciaExterna.endsWith(':REVERSION'));
  if (!abonoOriginales.length) return { reverted: false }; // este abono nunca tuvo efecto de lealtad

  const abonoNetCents = abonoOriginales.reduce((acc, f) => acc + toCents(f.puntos), 0);
  if (abonoNetCents === 0) return { reverted: false };

  // D2: agregado COMPLETO del serieFolio (no solo este abono) — único cálculo que puede ver un
  // retiro de venta previo (venta-retiro usa `${serieFolio}:REVERSION:<serieFolioRetiro>`,
  // SIN idAbono, invisible para cualquier query filtrada por idAbono).
  const netTotalCents = filas
    .filter(f => !_esCualquierReversion(f.referenciaExterna))
    .reduce((acc, f) => acc + toCents(f.puntos), 0);
  const yaRevertidoTotalCents = filas
    .filter(f => _esCualquierReversion(f.referenciaExterna))
    .reduce((acc, f) => acc + toCents(f.puntos), 0);
  const remanenteCents = netTotalCents + yaRevertidoTotalCents;

  // Clamp: nunca revertir más de lo que realmente queda vigente a nivel de TODA la venta.
  const magnitud = Math.min(Math.abs(abonoNetCents), Math.abs(remanenteCents));
  if (magnitud === 0) {
    logger.info(`[loyalty-kore-reversion-abono] serieFolio=${serieFolio} idAbono=${idAbono}: remanente ya en 0 (cubierto por un retiro de venta previo) — no-op.`);
    return { reverted: false };
  }
  const montoRevertirCents = -Math.sign(abonoNetCents) * magnitud;

  const cardId = abonoOriginales[0].cardId;

  try {
    const entry = await sequelize.transaction(async (t) => {
      const card = await LoyaltyCard.findByPk(cardId, { transaction: t, lock: Transaction.LOCK.UPDATE });
      if (!card) return null; // tarjeta borrada — defensivo, no debería pasar (FK RESTRICT)
      return _aplicarMovimiento(t, card, {
        tipo: 'AJUSTE',
        puntos: fromCents(montoRevertirCents),
        justificacion: `Reversión automática de abono ${idAbono} de ${serieFolio} (cxc-reversiones Kore)${motivo ? ` — ${motivo}` : ''}`,
        origen: ORIGEN_KORE,
        referenciaExterna: _refReversionAbono(serieFolio, idAbono),
      });
    });
    if (!entry) return { reverted: false };

    emitToLoyaltyCard(cardId, 'lealtad-tarjeta:actualizada', { cardId, motivo: 'reversion-abono-kore' });
    return { reverted: true, puntosRevertidos: fromCents(montoRevertirCents), cardId };
  } catch (err) {
    if (err?.name === 'SequelizeUniqueConstraintError') {
      // Carrera: otro request concurrente (reintento/duplicado de Kore) ganó el insert — inofensivo,
      // mismo criterio que procesarVentaRetiroKore.
      return { reverted: false };
    }
    throw err; // error inesperado: lo maneja el try/catch del caller en erp-reversion.service.js (D6)
  }
}

module.exports = { revertirPuntosPorAbono };
