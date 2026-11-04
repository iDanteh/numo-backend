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
// Misma normalización que usa el import de migración (loyalty-import-apply.service.js) y el job
// de match (loyalty-kore-match.service.js) para `tarjetaLegada` — reusada acá para que el webhook
// y esas dos vías nunca puedan dejar el campo con un formato distinto entre sí.
const { normalizarTarjeta } = require('../import/loyalty-import.parser');
const { registrarCambioTarjeta } = require('../cards/loyalty-card-audit.service');

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

async function _aplicarActualizacion(card, { koreClienteId, telefono, nombre, activo, tarjetaLegada }) {
  // Snapshot ANTES de armar `updates`/llamar a `.update()` — ver docstring de
  // `registrarCambioTarjeta` sobre por qué esto tiene que pasar en este orden.
  const antes = {
    koreClienteId: card.koreClienteId, nombre: card.nombre, telefono: card.telefono,
    activa: card.activa, tarjetaLegada: card.tarjetaLegada,
  };
  const updates = {
    updatedById:     ACTOR_WEBHOOK.dbId,
    updatedByNombre: ACTOR_WEBHOOK.nombre,
  };
  if (koreClienteId) updates.koreClienteId = koreClienteId;
  if (nombre !== undefined && String(nombre).trim()) updates.nombre = String(nombre).trim();
  if (telefono && telefono !== card.telefono) updates.telefono = telefono;
  // `activo` (2026-11-03, pedido explícito del usuario): reactiva una tarjeta que un `baja`
  // anterior dejó en activa:false — hasta ahora NINGÚN evento podía revertir eso (gap real
  // encontrado al responder una pregunta del usuario sobre reactivación de monedero en Kore).
  // Opcional: si Kore no lo manda, `activa` queda intacta (mismo criterio que nombre/telefono).
  if (activo !== undefined) updates.activa = Boolean(activo);
  // `tarjetaLegada` (gap real cerrado): antes de esto, el número de tarjeta/monedero de un
  // cliente YA matcheado nunca se actualizaba — `tarjetaLegada` solo se setea una vez, en el
  // match inicial (loyalty-kore-match.service.js) o en el import de migración. Si Kore manda este
  // evento con un número nuevo, ahora se persiste acá — mismo `normalizarTarjeta()` que usan esas
  // otras dos vías, para que el formato nunca diverja entre ellas. String vacío se trata igual que
  // "no vino" (no borra el valor existente) — un evento real siempre trae un número real.
  if (tarjetaLegada !== undefined && String(tarjetaLegada).trim()) {
    updates.tarjetaLegada = normalizarTarjeta(tarjetaLegada);
  }

  try {
    await card.update(updates);
  } catch (err) {
    if (_esDuplicadoUnico(err)) {
      throw new ConflictError('Ya existe otra tarjeta registrada con ese teléfono.');
    }
    throw err;
  }

  // `updatedById`/`updatedByNombre` son metadata de quién hizo el cambio, no un dato de
  // catálogo en sí — se excluyen del diff para no generar una fila falsa "cambió" en cada
  // actualización aunque ningún campo de negocio se haya tocado de verdad.
  const { updatedById, updatedByNombre, ...despues } = updates;
  await registrarCambioTarjeta({
    cardId: card.id, antes, despues, accion: 'webhook_kore_actualizacion', origen: 'kore',
  });

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

async function _procesarActualizacion({ koreClienteId, telefono, nombre, activo, tarjetaLegada }) {
  const card = await _buscarCard({ koreClienteId, telefono });
  if (!card) throw new NotFoundError('Tarjeta (no se encontró por koreClienteId ni teléfono)');
  return _aplicarActualizacion(card, { koreClienteId, telefono, nombre, activo, tarjetaLegada });
}

async function _procesarBaja({ koreClienteId, telefono }) {
  const card = await _buscarCard({ koreClienteId, telefono });
  if (!card) throw new NotFoundError('Tarjeta (no se encontró por koreClienteId ni teléfono)');

  const antes = { activa: card.activa };

  // Nunca se borra el registro — hay saldo de puntos real detrás de la tarjeta.
  await card.update({
    activa:          false,
    updatedById:     ACTOR_WEBHOOK.dbId,
    updatedByNombre: ACTOR_WEBHOOK.nombre,
  });

  await registrarCambioTarjeta({
    cardId: card.id, antes, despues: { activa: false }, accion: 'webhook_kore_baja', origen: 'kore',
  });

  return { ok: true, evento: 'baja', cardId: card.id };
}

/**
 * Punto de entrada del webhook `POST /api/loyalty/kore/clientes`.
 * `evento`: 'alta' | 'actualizacion' | 'baja'. `koreClienteId` siempre requerido;
 * `telefono` requerido (10 dígitos) para 'alta'/'actualizacion', opcional (solo
 * como fallback de búsqueda) para 'baja'.
 * `activo` (2026-11-03): SOLO se lee/aplica en 'actualizacion' — a propósito, no en 'alta'
 * (una tarjeta recién creada siempre nace activa, reactivar es un caso de "ya existe y
 * estaba dada de baja", no de alta) ni en 'baja' (ya tiene su propio camino fijo a false).
 * `tarjetaLegada`: mismo criterio que `activo` — SOLO 'actualizacion' lo lee. Una 'alta' siempre
 * crea la tarjeta sin este dato (lo trae después el match inicial o un `actualizacion` posterior),
 * y 'baja' no tiene nada que ver con el número de tarjeta.
 */
async function procesarClienteKore(payload) {
  const { evento, telefono, nombre, activo, tarjetaLegada } = payload ?? {};

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
  return _procesarActualizacion({
    koreClienteId, telefono: telefonoNorm, nombre, activo, tarjetaLegada,
  });
}

module.exports = { procesarClienteKore };
