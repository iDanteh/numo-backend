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
    montoVenta:         mov.montoVenta ?? null,
    porcentajeAplicado: mov.porcentajeAplicado ?? null,
    nivelAplicado:      mov.nivelAplicado ?? null,
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

    const montoCents = toCents(montoVenta);
    const puntosCents = calcularPuntos(montoCents, pct);
    if (puntosCents === 0) {
      throw new BadRequestError('El monto de venta no genera puntos con el % configurado.');
    }

    const { usuarioId, usuarioNombre } = _userLabel(user);
    const entry = await _aplicarMovimiento(t, card, {
      tipo:               'ACUMULACION',
      puntos:             fromCents(puntosCents),
      montoVenta,
      porcentajeAplicado: pct,
      nivelAplicado:      card.nivel,
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
 */
async function registrarAjuste({ cardId, puntos, reasonId, justificacion }, user) {
  const puntosNum = Number(puntos);
  if (!Number.isFinite(puntosNum) || puntosNum === 0) {
    throw new BadRequestError('puntos debe ser un número distinto de 0.');
  }

  const justificacionTrim = String(justificacion ?? '').trim();
  if (justificacionTrim.length < 5) {
    throw new BadRequestError('justificacion es requerida (mínimo 5 caracteres).');
  }

  const reason = await LoyaltyAdjustmentReason.findByPk(reasonId);
  if (!reason || !reason.activo) {
    throw new BadRequestError('El motivo de ajuste no existe o está inactivo.');
  }

  const signo = puntosNum > 0 ? 'POSITIVO' : 'NEGATIVO';
  if (reason.signoPermitido !== 'AMBOS' && reason.signoPermitido !== signo) {
    throw new BadRequestError(
      `El motivo "${reason.nombre}" no permite ajustes de signo ${signo === 'POSITIVO' ? 'positivo' : 'negativo'}.`,
    );
  }

  return sequelize.transaction(async (t) => {
    const card = await LoyaltyCard.findByPk(cardId, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!card) throw new NotFoundError('Tarjeta');
    if (!card.activa) throw new UnprocessableError('La tarjeta está inactiva.');

    const puntosCents = toCents(puntosNum);
    const saldoCents  = toCents(card.saldoPuntos);
    if (saldoCents + puntosCents < 0) {
      throw new ConflictError('El ajuste dejaría el saldo de puntos en negativo.');
    }

    const { usuarioId, usuarioNombre } = _userLabel(user);
    const entry = await _aplicarMovimiento(t, card, {
      tipo:   'AJUSTE',
      puntos: fromCents(puntosCents),
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
