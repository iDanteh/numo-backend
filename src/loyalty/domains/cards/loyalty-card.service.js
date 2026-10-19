'use strict';

/**
 * domains/cards/loyalty-card.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — ciclo de vida de la tarjeta (D1: el teléfono es la
 * única llave de identidad). El NIP en texto plano (D6) solo existe en el
 * valor de retorno de `crearTarjeta`/`cambiarNip` — nunca se persiste ni se
 * loguea, y el modelo excluye `nipHash` por `defaultScope` (PR1).
 */

const { Transaction, Op } = require('sequelize');
const { sequelize } = require('../../../config/database.postgres');
const { LoyaltyCard, LoyaltyTierHistory } = require('../../../shared/models/postgres');
const { BadRequestError, ConflictError, NotFoundError } = require('../../../shared/errors/AppError');
const { normalizarTelefono } = require('../../shared/puntos.util');
const { generarNip, hashNip } = require('../../shared/nip.util');

const NIVELES_VALIDOS = ['Bronce', 'Plata', 'Oro'];

function _userLabel(user) {
  return {
    usuarioId:     user?.dbId != null ? String(user.dbId) : (user?._id != null ? String(user._id) : null),
    usuarioNombre: user?.nombre ?? null,
  };
}

function _esDuplicadoUnico(err) {
  return err?.name === 'SequelizeUniqueConstraintError';
}

/**
 * Crea una tarjeta nueva. El teléfono se normaliza (D1/puntos.util) antes de
 * validar unicidad — rechaza duplicados contra tarjetas activas E inactivas
 * (un teléfono inactivo sigue siendo "la misma persona" para el programa).
 */
async function crearTarjeta({ telefono, nombre, nivel = 'Bronce' }, user, { transaction } = {}) {
  const telefonoNorm = normalizarTelefono(telefono);
  const nombreTrim = String(nombre ?? '').trim();
  if (!nombreTrim) {
    throw new BadRequestError('nombre es requerido.');
  }
  if (!NIVELES_VALIDOS.includes(nivel)) {
    throw new BadRequestError(`nivel inválido: "${nivel}".`);
  }

  // `transaction` es opcional (PR8: el script de migración reutiliza este servicio dentro
  // de UNA sola transacción para todo el lote, design.md "Apply. One Postgres transaction
  // for all ok rows"). Se pasa condicionalmente (nunca como `undefined` explícito) para no
  // alterar la cantidad de argumentos de las llamadas existentes desde las rutas (PR4).
  const existente = transaction
    ? await LoyaltyCard.findOne({ where: { telefono: telefonoNorm }, transaction })
    : await LoyaltyCard.findOne({ where: { telefono: telefonoNorm } });
  if (existente) {
    throw new ConflictError('Ya existe una tarjeta registrada con ese teléfono.');
  }

  const nip = generarNip();
  const nipHash = await hashNip(nip);
  const { usuarioId, usuarioNombre } = _userLabel(user);

  const datosCard = {
    telefono: telefonoNorm,
    nombre:   nombreTrim,
    nivel,
    nipHash,
    saldoPuntos: 0,
    activa: true,
    createdById:     usuarioId,
    createdByNombre: usuarioNombre,
  };

  let card;
  try {
    card = transaction
      ? await LoyaltyCard.create(datosCard, { transaction })
      : await LoyaltyCard.create(datosCard);
  } catch (err) {
    if (_esDuplicadoUnico(err)) {
      throw new ConflictError('Ya existe una tarjeta registrada con ese teléfono.');
    }
    throw err;
  }

  return { card, nip };
}

/** `search` matchea prefijo de teléfono o ILIKE sobre nombre. */
async function listar({ search, nivel, activa, page = 1, limit = 50 } = {}) {
  const where = {};
  if (nivel) where.nivel = nivel;
  if (activa !== undefined && activa !== null && activa !== '') {
    where.activa = activa === true || activa === 'true';
  }
  if (search) {
    const s = String(search).trim();
    where[Op.or] = [
      { telefono: { [Op.like]: `${s}%` } },
      { nombre:   { [Op.iLike]: `%${s}%` } },
    ];
  }

  const p = Math.max(1, Number(page) || 1);
  const l = Math.min(100, Number(limit) || 50);
  const { count, rows } = await LoyaltyCard.findAndCountAll({
    where,
    order:  [['nombre', 'ASC']],
    limit:  l,
    offset: (p - 1) * l,
  });

  return { total: count, page: p, limit: l, pages: Math.ceil(count / l), cards: rows };
}

async function obtener(id) {
  const card = await LoyaltyCard.findByPk(id);
  if (!card) throw new NotFoundError('Tarjeta');
  return card;
}

/** Whitelist de campos editables. `telefono` re-aplica la misma unicidad que `crearTarjeta`. */
async function editarTarjeta(id, { nombre, telefono, activa } = {}, user) {
  const card = await LoyaltyCard.findByPk(id);
  if (!card) throw new NotFoundError('Tarjeta');

  const updates = {};
  if (nombre !== undefined) {
    updates.nombre = String(nombre).trim();
  }
  if (telefono !== undefined) {
    const telefonoNorm = normalizarTelefono(telefono);
    if (telefonoNorm !== card.telefono) {
      const existente = await LoyaltyCard.findOne({ where: { telefono: telefonoNorm, id: { [Op.ne]: id } } });
      if (existente) {
        throw new ConflictError('Ya existe una tarjeta registrada con ese teléfono.');
      }
      updates.telefono = telefonoNorm;
    }
  }
  if (activa !== undefined) {
    updates.activa = Boolean(activa);
  }

  const { usuarioId, usuarioNombre } = _userLabel(user);
  updates.updatedById = usuarioId;
  updates.updatedByNombre = usuarioNombre;

  try {
    await card.update(updates);
  } catch (err) {
    if (_esDuplicadoUnico(err)) {
      throw new ConflictError('Ya existe una tarjeta registrada con ese teléfono.');
    }
    throw err;
  }

  return card;
}

/**
 * Genera un NIP nuevo y resetea el lockout (D7) — el anterior deja de autenticar de inmediato.
 *
 * Bug real corregido (2026-10-19, pedido explícito del usuario — "agrega solo el quién y
 * cuándo"): esta función nunca tocaba `updatedById`/`updatedByNombre` pese a que `LoyaltyCard`
 * ya tiene esas columnas y `editarTarjeta`/`cambiarNivel` sí las setean — cambiar el NIP de una
 * tarjeta no dejaba NINGÚN rastro de quién lo hizo. `updatedAt` ya se actualizaba solo (Sequelize
 * lo maneja automático en cualquier `.update()`), pero sin saber QUIÉN eso no sirve para
 * trazabilidad real. No se agrega un historial completo (ver discovery sobre
 * PolizaAuditLog/ConfigAuditLog) — decisión explícita del usuario de ir con el mínimo.
 */
async function cambiarNip(cardId, user) {
  const card = await LoyaltyCard.findByPk(cardId);
  if (!card) throw new NotFoundError('Tarjeta');

  const nip = generarNip();
  const nipHash = await hashNip(nip);
  const { usuarioId, usuarioNombre } = _userLabel(user);
  await card.update({
    nipHash, nipIntentosFallidos: 0, nipBloqueadoHasta: null,
    updatedById: usuarioId, updatedByNombre: usuarioNombre,
  });

  return { nip };
}

/** Cambia el nivel y audita en `loyalty_tier_history`, en una sola transacción. No mueve `saldoPuntos`. */
async function cambiarNivel(cardId, nuevoNivel, { motivo } = {}, user) {
  if (!NIVELES_VALIDOS.includes(nuevoNivel)) {
    throw new BadRequestError(`nivel inválido: "${nuevoNivel}".`);
  }

  return sequelize.transaction(async (t) => {
    const card = await LoyaltyCard.findByPk(cardId, { transaction: t, lock: Transaction.LOCK.UPDATE });
    if (!card) throw new NotFoundError('Tarjeta');
    if (card.nivel === nuevoNivel) {
      throw new BadRequestError('La tarjeta ya está en ese nivel.');
    }

    const nivelAnterior = card.nivel;
    await card.update({ nivel: nuevoNivel }, { transaction: t });

    const { usuarioId, usuarioNombre } = _userLabel(user);
    await LoyaltyTierHistory.create({
      cardId: card.id,
      nivelAnterior,
      nivelNuevo: nuevoNivel,
      motivo: motivo ?? null,
      usuarioId,
      usuarioNombre,
    }, { transaction: t });

    return card;
  });
}

module.exports = { crearTarjeta, listar, obtener, editarTarjeta, cambiarNip, cambiarNivel };
