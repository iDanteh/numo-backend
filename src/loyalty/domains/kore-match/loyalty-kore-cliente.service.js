'use strict';

/**
 * domains/kore-match/loyalty-kore-cliente.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — webhook Kore→Numo de catálogo de clientes (altas/bajas/
 * actualizaciones en tiempo real), punto de entrada de `POST /api/loyalty/kore/clientes`.
 * Complementa al job bajo demanda de `loyalty-kore-match.service.js` (que sigue
 * vivo como herramienta de backfill/reconciliación) con sincronización push.
 *
 * A diferencia de `loyalty-kore-venta.service.js` (transaccional, necesita
 * idempotencia por `referenciaExterna` porque mueve puntos reales), esto es
 * "último valor gana" sobre datos de catálogo — reenviar el mismo evento dos
 * veces simplemente vuelve a escribir los mismos valores, sin efecto
 * financiero. No hay tabla de fallos (`LoyaltyKoreVentaFallida`) para esto:
 * un evento que falla responde el error en la propia respuesta HTTP al
 * webhook (Kore puede reintentar), no queda nada que reprocesar del lado
 * de Numo.
 */

const { LoyaltyCard } = require('../../../shared/models/postgres');
const { BadRequestError, ConflictError, NotFoundError } = require('../../../shared/errors/AppError');
const { normalizarTelefono } = require('../../shared/puntos.util');
const { crearTarjeta } = require('../cards/loyalty-card.service');

const EVENTOS_VALIDOS = ['alta', 'actualizacion', 'baja'];

// Actor sintético para `createdById`/`updatedById` — mismo criterio que `origen: 'kore'` en el
// ledger de ventas: estos cambios no los hace un usuario humano, pero las columnas de auditoría
// de LoyaltyCard (`createdById`/`createdByNombre`/`updatedById`/`updatedByNombre`) son de texto
// libre, no una FK a un usuario real, así que un valor descriptivo fijo alcanza para trazabilidad.
const ACTOR_WEBHOOK = { dbId: 'kore-webhook', nombre: 'Kore (webhook catálogo)' };

function _esDuplicadoUnico(err) {
  return err?.name === 'SequelizeUniqueConstraintError';
}

function _requiereKoreClienteId(koreClienteId) {
  if (koreClienteId === undefined || koreClienteId === null || String(koreClienteId).trim() === '') {
    throw new BadRequestError('koreClienteId es requerido.');
  }
  return String(koreClienteId).trim();
}

/** Busca por `koreClienteId` primero (estable aunque el teléfono cambie), fallback por teléfono. */
async function _buscarCard({ koreClienteId, telefono }) {
  if (koreClienteId) {
    const card = await LoyaltyCard.findOne({ where: { koreClienteId } });
    if (card) return card;
  }
  if (telefono) {
    let telefonoNorm;
    try {
      telefonoNorm = normalizarTelefono(telefono);
    } catch {
      return null;
    }
    return LoyaltyCard.findOne({ where: { telefono: telefonoNorm } });
  }
  return null;
}

async function _aplicarActualizacion(card, { koreClienteId, telefono, nombre }) {
  const updates = {
    updatedById:     ACTOR_WEBHOOK.dbId,
    updatedByNombre: ACTOR_WEBHOOK.nombre,
  };
  if (koreClienteId) updates.koreClienteId = koreClienteId;
  if (nombre !== undefined && String(nombre).trim()) updates.nombre = String(nombre).trim();
  if (telefono && telefono !== card.telefono) updates.telefono = telefono;

  try {
    await card.update(updates);
  } catch (err) {
    if (_esDuplicadoUnico(err)) {
      throw new ConflictError('Ya existe otra tarjeta registrada con ese teléfono.');
    }
    throw err;
  }

  return { ok: true, evento: 'actualizacion', cardId: card.id, creada: false };
}

async function _procesarAlta({ koreClienteId, telefono, nombre }) {
  const existente = await LoyaltyCard.findOne({ where: { telefono } });
  if (existente) {
    // Idempotente: un "alta" para un teléfono que ya tiene tarjeta se trata como actualización,
    // nunca se duplica ni se lanza error por "ya existe".
    return _aplicarActualizacion(existente, { koreClienteId, telefono, nombre });
  }

  if (!String(nombre ?? '').trim()) {
    throw new BadRequestError('nombre es requerido para dar de alta una tarjeta nueva.');
  }

  let resultado;
  try {
    resultado = await crearTarjeta({ telefono, nombre }, ACTOR_WEBHOOK);
  } catch (err) {
    if (err instanceof ConflictError) {
      // Carrera: otra llamada creó la tarjeta entre el findOne de arriba y este create.
      const card = await LoyaltyCard.findOne({ where: { telefono } });
      if (card) return _aplicarActualizacion(card, { koreClienteId, telefono, nombre });
    }
    throw err;
  }

  // El NIP que devuelve crearTarjeta() nunca se expone en la respuesta de este webhook — el
  // cliente lo obtiene por la consulta pública (NIP→OTP) ya existente.
  const { card } = resultado;
  await card.update({ koreClienteId, revisionPendiente: true });

  return { ok: true, evento: 'alta', cardId: card.id, creada: true };
}

async function _procesarActualizacion({ koreClienteId, telefono, nombre }) {
  const card = await _buscarCard({ koreClienteId, telefono });
  if (!card) throw new NotFoundError('Tarjeta (no se encontró por koreClienteId ni teléfono)');
  return _aplicarActualizacion(card, { koreClienteId, telefono, nombre });
}

async function _procesarBaja({ koreClienteId, telefono }) {
  const card = await _buscarCard({ koreClienteId, telefono });
  if (!card) throw new NotFoundError('Tarjeta (no se encontró por koreClienteId ni teléfono)');

  // Nunca se borra el registro — hay saldo de puntos real detrás de la tarjeta.
  await card.update({
    activa:          false,
    updatedById:     ACTOR_WEBHOOK.dbId,
    updatedByNombre: ACTOR_WEBHOOK.nombre,
  });

  return { ok: true, evento: 'baja', cardId: card.id };
}

/**
 * Punto de entrada del webhook `POST /api/loyalty/kore/clientes`.
 * `evento`: 'alta' | 'actualizacion' | 'baja'. `koreClienteId` siempre requerido;
 * `telefono` requerido (10 dígitos) para 'alta'/'actualizacion', opcional (solo
 * como fallback de búsqueda) para 'baja'.
 */
async function procesarClienteKore(payload) {
  const { evento, telefono, nombre } = payload ?? {};

  if (!EVENTOS_VALIDOS.includes(evento)) {
    throw new BadRequestError(`evento inválido: "${evento}". Debe ser uno de: ${EVENTOS_VALIDOS.join(', ')}.`);
  }
  const koreClienteId = _requiereKoreClienteId(payload?.koreClienteId);

  if (evento === 'baja') {
    return _procesarBaja({ koreClienteId, telefono });
  }

  let telefonoNorm;
  try {
    telefonoNorm = normalizarTelefono(telefono);
  } catch (err) {
    throw new BadRequestError(err.message);
  }

  if (evento === 'alta') {
    return _procesarAlta({ koreClienteId, telefono: telefonoNorm, nombre });
  }
  return _procesarActualizacion({ koreClienteId, telefono: telefonoNorm, nombre });
}

module.exports = { procesarClienteKore };
