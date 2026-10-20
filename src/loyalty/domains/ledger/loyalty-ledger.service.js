'use strict';

/**
 * domains/ledger/loyalty-ledger.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — único punto de escritura de puntos (D4). Toda mutación
 * pasa por `_aplicarMovimiento`, que corre dentro de la transacción abierta
 * por el caller con la tarjeta ya bloqueada (`LOCK.UPDATE`), igual que
 * `poliza.repository.js#setEstado`. El CHECK `saldo_puntos >= 0` en Postgres
 * (PR1) es el respaldo de última línea si algún día algo escribe fuera de acá.
 *
 * Toda la aritmética de puntos vive en centavos enteros (D3, ver
 * `shared/puntos.util.js`) — el modelo guarda/lee DECIMAL como string.
 */

const { Transaction } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const {
  LoyaltyCard, LoyaltyPointsLedger, LoyaltyTierHistory, LoyaltyAdjustmentReason, CentroCosto,
} = require('../../../shared/models/postgres');
const { BadRequestError, ConflictError, UnprocessableError, NotFoundError } = require('../../../shared/errors/AppError');
const { toCents, fromCents, calcularPuntos } = require('../../shared/puntos.util');
const globalConfigService = require('../../../shared/services/global-config.service');
// Sin require circular: loyalty-config.service.js no importa nada de este
// archivo (verificado), así que un require normal al tope basta — no hace
// falta el patrón de require perezoso que usa p.ej. shared/middleware/auth.js.
const configService = require('../config/loyalty-config.service');

function _userLabel(user) {
  return {
    usuarioId:     user?.dbId != null ? String(user.dbId) : (user?._id != null ? String(user._id) : null),
    usuarioNombre: user?.nombre ?? null,
  };
}

/**
 * Único escritor del ledger (D4). Recibe la tarjeta YA bloqueada
 * (`findByPk(..., {lock: LOCK.UPDATE})`) dentro de la transacción `t` del
 * caller. Inserta la fila y actualiza el saldo denormalizado en la misma
 * transacción — nunca hay un momento en que uno exista sin el otro.
 */
async function _aplicarMovimiento(t, card, mov) {
  if (!card.activa) {
    throw new UnprocessableError('La tarjeta está inactiva.');
  }

  const saldoActualCents = toCents(card.saldoPuntos);
  const puntosCents = toCents(mov.puntos);
  const saldoResultanteCents = saldoActualCents + puntosCents;

  const entry = await LoyaltyPointsLedger.create({
    cardId:             card.id,
    tipo:               mov.tipo,
    puntos:             fromCents(puntosCents),
    saldoResultante:    fromCents(saldoResultanteCents),
    montoVenta:          mov.montoVenta ?? null,
    porcentajeAplicado:  mov.porcentajeAplicado ?? null,
    nivelAplicado:       mov.nivelAplicado ?? null,
    promocionNombre:     mov.promocionNombre ?? null,
    promocionPorcentaje: mov.promocionPorcentaje ?? null,
    centroCostoId:      mov.centroCostoId ?? null,
    reasonId:           mov.reasonId ?? null,
    justificacion:      mov.justificacion ?? null,
    origen:             mov.origen ?? 'manual',
    referenciaExterna:  mov.referenciaExterna ?? null,
    usuarioId:          mov.usuarioId ?? null,
    usuarioNombre:      mov.usuarioNombre ?? null,
  }, { transaction: t });

  await card.update({ saldoPuntos: fromCents(saldoResultanteCents) }, { transaction: t });

  return entry;
}

async function _obtenerCentroCosto(centroCostoId) {
  const centroCosto = await CentroCosto.findByPk(centroCostoId);
  if (!centroCosto || !centroCosto.isActive) {
    throw new UnprocessableError('El centro de costo no existe o está inactivo.');
  }
  return centroCosto;
}

/**
 * ACUMULACION: `puntos = montoVenta × %nivel`, tomado como snapshot (D5) — el
 * % se congela en la fila y una config futura nunca lo recalcula. Falla
 * cerrado (D11) si el % del nivel no está configurado: nunca acumula al 0%.
 */
async function registrarAcumulacion({ cardId, montoVenta, centroCostoId }, user) {
  if (!(Number(montoVenta) > 0)) {
    throw new BadRequestError('montoVenta debe ser mayor a 0.');
  }

  const centroCosto = await _obtenerCentroCosto(centroCostoId);
  if (!centroCosto.participaLealtad) {
    throw new UnprocessableError('Esta sucursal no participa en el programa de lealtad.');
  }

  return sequelize.transaction(async (t) => {
    const card = await LoyaltyCard.findByPk(cardId, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!card) throw new NotFoundError('Tarjeta');
    if (!card.activa) throw new UnprocessableError('La tarjeta está inactiva.');

    let pctRaw;
    try {
      pctRaw = await globalConfigService.getValue('lealtad', `PORCENTAJE_${card.nivel.toUpperCase()}`);
    } catch (err) {
      throw new UnprocessableError(
        `Configure el % del nivel ${card.nivel} en Lealtad → Configuración.`,
      );
    }
    const pct = Number(pctRaw);

    // Promoción vigente (2026-10-20): su % de bono se SUMA al % del nivel —
    // sin promo vigente, pctTotal === pct (comportamiento idéntico a hoy).
    const promoVigente = await configService.obtenerPromocionVigente();
    const pctTotal = promoVigente ? pct + Number(promoVigente.porcentajeBono) : pct;

    const montoCents = toCents(montoVenta);
    const puntosCents = calcularPuntos(montoCents, pctTotal);
    if (puntosCents === 0) {
      throw new BadRequestError('El monto de venta no genera puntos con el % configurado.');
    }

    const { usuarioId, usuarioNombre } = _userLabel(user);
    const entry = await _aplicarMovimiento(t, card, {
      tipo:                'ACUMULACION',
      puntos:              fromCents(puntosCents),
      montoVenta,
      porcentajeAplicado:  pctTotal,
      nivelAplicado:       card.nivel,
      promocionNombre:     promoVigente?.nombre ?? null,
      promocionPorcentaje: promoVigente?.porcentajeBono ?? null,
      centroCostoId,
      origen: 'manual',
      usuarioId,
      usuarioNombre,
    });

    return { movimiento: entry, saldoPuntos: entry.saldoResultante };
  });
}

/**
 * CANJE: debita a 1 punto = $1.00 MXN. El tope contra el saldo se valida
 * DESPUÉS de bloquear la fila (evita TOCTOU entre dos canjes concurrentes).
 */
async function registrarCanje({ cardId, puntos, centroCostoId }, user) {
  if (!(Number(puntos) > 0)) {
    throw new BadRequestError('puntos debe ser mayor a 0.');
  }

  const centroCosto = await _obtenerCentroCosto(centroCostoId);
  if (!centroCosto.permiteCanjeLealtad) {
    throw new UnprocessableError('Esta sucursal no permite canjes de lealtad.');
  }

  return sequelize.transaction(async (t) => {
    const card = await LoyaltyCard.findByPk(cardId, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!card) throw new NotFoundError('Tarjeta');
    if (!card.activa) throw new UnprocessableError('La tarjeta está inactiva.');

    const puntosCents = toCents(puntos);
    const saldoCents  = toCents(card.saldoPuntos);
    if (puntosCents > saldoCents) {
      throw new ConflictError('El canje excede el saldo de puntos disponible.');
    }

    const { usuarioId, usuarioNombre } = _userLabel(user);
    const entry = await _aplicarMovimiento(t, card, {
      tipo:   'CANJE',
      puntos: fromCents(-puntosCents),
      centroCostoId,
      origen: 'manual',
      usuarioId,
      usuarioNombre,
    });

    return { movimiento: entry, saldoPuntos: entry.saldoResultante };
  });
}

/**
 * AJUSTE: requiere un motivo activo cuyo `signoPermitido` concuerde con el
 * signo del movimiento, y una justificación no trivial.
 *
 * Motivos con `reason.porcentaje` configurado (2026-10-20) calculan los
 * puntos automáticamente desde `montoBase` (igual criterio D5/D3 que
 * ACUMULACION: snapshot de % + aritmética en centavos) en vez de tomar
 * `puntos` directo del body — ese es el camino manual de siempre, intacto
 * cuando el motivo no tiene % configurado.
 */
async function registrarAjuste({ cardId, puntos, montoBase, reasonId, justificacion }, user) {
  const justificacionTrim = String(justificacion ?? '').trim();
  if (justificacionTrim.length < 5) {
    throw new BadRequestError('justificacion es requerida (mínimo 5 caracteres).');
  }

  const reason = await LoyaltyAdjustmentReason.findByPk(reasonId);
  if (!reason || !reason.activo) {
    throw new BadRequestError('El motivo de ajuste no existe o está inactivo.');
  }

  let puntosCents;
  let montoBaseAplicado = null;
  let porcentajeAplicado = null;

  if (reason.porcentaje != null) {
    // Camino automático: el signo ya NO se decide por el valor de `puntos`
    // del body — lo decide `reason.signoPermitido` (bloqueado a POSITIVO o
    // NEGATIVO al crear/editar el motivo con %, nunca AMBOS).
    if (!(Number(montoBase) > 0)) {
      throw new BadRequestError('montoBase debe ser mayor a 0 para un motivo con % de cálculo automático.');
    }
    if (reason.signoPermitido !== 'POSITIVO' && reason.signoPermitido !== 'NEGATIVO') {
      // Defensivo: no debería pasar (crearReason/editarReason lo bloquean),
      // pero datos legados podrían tener AMBOS + porcentaje configurado.
      throw new BadRequestError(
        `El motivo "${reason.nombre}" tiene % de cálculo automático configurado con un signoPermitido inválido (${reason.signoPermitido}); debe ser POSITIVO o NEGATIVO.`,
      );
    }

    const montoCents = toCents(montoBase);
    const puntosCalculados = calcularPuntos(montoCents, reason.porcentaje);
    puntosCents = reason.signoPermitido === 'NEGATIVO' ? -puntosCalculados : puntosCalculados;
    if (puntosCents === 0) {
      throw new BadRequestError('El monto no genera puntos con el % configurado para este motivo.');
    }

    montoBaseAplicado = montoBase;
    porcentajeAplicado = reason.porcentaje;
  } else {
    // Camino manual, igual que siempre: `puntos` viene directo del body.
    const puntosNum = Number(puntos);
    if (!Number.isFinite(puntosNum) || puntosNum === 0) {
      throw new BadRequestError('puntos debe ser un número distinto de 0.');
    }
    puntosCents = toCents(puntosNum);
  }

  const signo = puntosCents > 0 ? 'POSITIVO' : 'NEGATIVO';
  if (reason.signoPermitido !== 'AMBOS' && reason.signoPermitido !== signo) {
    throw new BadRequestError(
      `El motivo "${reason.nombre}" no permite ajustes de signo ${signo === 'POSITIVO' ? 'positivo' : 'negativo'}.`,
    );
  }

  return sequelize.transaction(async (t) => {
    const card = await LoyaltyCard.findByPk(cardId, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!card) throw new NotFoundError('Tarjeta');
    if (!card.activa) throw new UnprocessableError('La tarjeta está inactiva.');

    const saldoCents = toCents(card.saldoPuntos);
    if (saldoCents + puntosCents < 0) {
      throw new ConflictError('El ajuste dejaría el saldo de puntos en negativo.');
    }

    const { usuarioId, usuarioNombre } = _userLabel(user);
    const entry = await _aplicarMovimiento(t, card, {
      tipo:               'AJUSTE',
      puntos:             fromCents(puntosCents),
      montoVenta:         montoBaseAplicado,
      porcentajeAplicado,
      reasonId,
      justificacion: justificacionTrim,
      origen: 'manual',
      usuarioId,
      usuarioNombre,
    });

    return { movimiento: entry, saldoPuntos: entry.saldoResultante };
  });
}

async function listarMovimientos(cardId, { page = 1, limit = 50 } = {}) {
  const p = Math.max(1, Number(page) || 1);
  const l = Math.min(100, Number(limit) || 50);

  const { count, rows } = await LoyaltyPointsLedger.findAndCountAll({
    where:  { cardId },
    order:  [['createdAt', 'DESC']],
    limit:  l,
    offset: (p - 1) * l,
  });

  return { total: count, page: p, limit: l, pages: Math.ceil(count / l), movimientos: rows };
}

async function listarNiveles(cardId) {
  return LoyaltyTierHistory.findAll({ where: { cardId }, order: [['at', 'DESC']] });
}

module.exports = {
  registrarAcumulacion,
  registrarCanje,
  registrarAjuste,
  listarMovimientos,
  listarNiveles,
  _aplicarMovimiento,
};
