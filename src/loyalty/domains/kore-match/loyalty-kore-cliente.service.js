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
const { crearTarjeta, cambiarNivel, nivelEsValido } = require('../cards/loyalty-card.service');
// Misma normalización que usa el import de migración (loyalty-import-apply.service.js) y el job
// de match (loyalty-kore-match.service.js) para `tarjetaLegada` — reusada acá para que el webhook
// y esas dos vías nunca puedan dejar el campo con un formato distinto entre sí.
const { normalizarTarjeta } = require('../import/loyalty-import.parser');
const { registrarCambioTarjeta } = require('../cards/loyalty-card-audit.service');
const { emitToAll } = require('../../../banks/shared/socket');

const EVENTOS_VALIDOS = ['alta', 'actualizacion', 'baja'];

// Actor sintético para `createdById`/`updatedById` — mismo criterio que `origen: 'kore'` en el
// ledger de ventas: estos cambios no los hace un usuario humano, pero las columnas de auditoría
// de LoyaltyCard (`createdById`/`createdByNombre`/`updatedById`/`updatedByNombre`) son de texto
// libre, no una FK a un usuario real, así que un valor descriptivo fijo alcanza para trazabilidad.
const ACTOR_WEBHOOK = { dbId: 'kore-webhook', nombre: 'Kore (webhook catálogo)' };

function _esDuplicadoUnico(err) {
  return err?.name === 'SequelizeUniqueConstraintError';
}

// `nivel` (2026-11-06, pedido explícito del usuario): Kore ahora también manda el nivel del cliente
// en el webhook de catálogo — antes esto era 100% asignación manual del staff, nunca tocado acá.
// REDISEÑO 2026-11-06 (mismo día, segunda ronda): el catálogo de niveles pasa a gestionarlo Kore
// (ver LoyaltyKoreNivel.js) — ya no hay un array fijo que validar contra, `nivelEsValido()` consulta
// el catálogo real.
// REDISEÑO 2026-11-06 (mismo día, tercera ronda, pedido explícito del usuario): `nivel` pasa de
// opcional a OBLIGATORIO en 'alta'/'actualizacion' — Kore controla el nivel por completo, Numo lo
// necesita siempre, incluso en un ajuste. `_validarNivel` ahora exige presencia ADEMÁS de validez
// (defensa en profundidad: el gate principal vive en `procesarClienteKore`, ver abajo, pero esta
// función también se usa directo desde `_aplicarActualizacion`). `baja` nunca llama a esto.
async function _validarNivel(nivel) {
  if (nivel === undefined || nivel === null || String(nivel).trim() === '') {
    throw new BadRequestError('nivel es requerido.');
  }
  if (!(await nivelEsValido(nivel))) {
    throw new BadRequestError(`nivel inválido: "${nivel}".`);
  }
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

async function _aplicarActualizacion(card, { koreClienteId, telefono, nombre, activo, tarjetaLegada, nivel }) {
  await _validarNivel(nivel);

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

  // `nivel` (2026-11-06, pedido explícito del usuario, ahora OBLIGATORIO — ver _validarNivel arriba
  // y el gate en procesarClienteKore): NUNCA se mete en `updates`/`registrarCambioTarjeta` de arriba
  // a propósito, porque ese log excluye `nivel` deliberadamente (ver docstring de
  // loyalty-card-audit.service.js: el historial de nivel vive en `LoyaltyTierHistory`, no se duplica
  // en `LoyaltyCardAuditLog`). Se reusa `cambiarNivel()` — el mismo primitivo que ya usa el botón
  // manual "Cambiar nivel" — para que un cambio de nivel disparado por Kore quede en el mismo
  // historial que uno hecho a mano. Guard `nivel !== card.nivel`: `cambiarNivel()` lanza si el nivel
  // nuevo es igual al actual, y Kore puede reenviar el mismo valor en cada actualización (catálogo
  // "último valor gana").
  if (nivel !== card.nivel) {
    await cambiarNivel(card.id, nivel, { motivo: 'Webhook Kore (catálogo de clientes)' }, ACTOR_WEBHOOK);
  }

  return { ok: true, evento: 'actualizacion', cardId: card.id, creada: false };
}

async function _procesarAlta({ koreClienteId, telefono, nombre, nivel }) {
  // Busca primero por `koreClienteId` (identidad real y estable del cliente en Kore), igual que
  // `actualizacion`/`baja` — recién si no aparece, por teléfono. Bug real encontrado 2026-11-05:
  // `koreClienteId` identifica al CLIENTE, no al monedero — si elimina su tarjeta y se registra de
  // nuevo con un teléfono DISTINTO, Kore reusa el MISMO koreClienteId. Buscando por ahí primero,
  // ese "alta" reactiva y actualiza la MISMA fila (mismo saldo/nivel, línea de tiempo continua en
  // `LoyaltyCardAuditLog`: alta→baja→actualizacion→...) en vez de crear una tarjeta nueva separada
  // para el mismo cliente real — antes esto producía 2+ filas con el mismo koreClienteId, y
  // `_buscarCard` podía resolver a la vieja en vez de la nueva (causa real del 409 "ya existe otra
  // tarjeta con ese teléfono" al reactivar, visto en producción).
  const existente = await _buscarCard({ koreClienteId, telefono });
  if (existente) {
    // Idempotente: un "alta" para un cliente que ya tiene tarjeta (por koreClienteId o teléfono)
    // se trata como actualización, nunca se duplica ni se lanza error por "ya existe". `activo:
    // true`: un "alta" sobre una tarjeta que había sido eliminada/desactivada antes debe
    // reactivarla — para Kore esto ES un alta, no manda un evento de reactivación aparte.
    return _aplicarActualizacion(existente, { koreClienteId, telefono, nombre, activo: true, nivel });
  }

  if (!String(nombre ?? '').trim()) {
    throw new BadRequestError('nombre es requerido para dar de alta una tarjeta nueva.');
  }

  let resultado;
  try {
    // `nivel`: OBLIGATORIO en este webhook (ver el gate en procesarClienteKore, más abajo) — a
    // diferencia del alta MANUAL del panel (que sigue sin mandarlo, y ahí sí aplica el fallback de
    // `obtenerNivelInicial()` dentro de crearTarjeta()), acá siempre viaja el nivel real de Kore.
    // `crearTarjeta()` igual valida contra el catálogo (`nivelEsValido()`) como salvaguarda.
    resultado = await crearTarjeta({ telefono, nombre, nivel }, ACTOR_WEBHOOK);
  } catch (err) {
    if (err instanceof ConflictError) {
      // Carrera: otra llamada creó la tarjeta entre el findOne de arriba y este create.
      const card = await LoyaltyCard.findOne({ where: { telefono } });
      if (card) return _aplicarActualizacion(card, { koreClienteId, telefono, nombre, activo: true, nivel });
    }
    throw err;
  }

  // El NIP que devuelve crearTarjeta() nunca se expone en la respuesta de este webhook — el
  // cliente lo obtiene por la consulta pública (NIP→OTP) ya existente.
  const { card } = resultado;
  await card.update({ koreClienteId, revisionPendiente: true });

  return { ok: true, evento: 'alta', cardId: card.id, creada: true };
}

async function _procesarActualizacion({ koreClienteId, telefono, nombre, activo, tarjetaLegada, nivel }) {
  const card = await _buscarCard({ koreClienteId, telefono });
  if (!card) throw new NotFoundError('Tarjeta (no se encontró por koreClienteId ni teléfono)');
  return _aplicarActualizacion(card, { koreClienteId, telefono, nombre, activo, tarjetaLegada, nivel });
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
 * `activo`: Kore lo manda explícitamente solo en 'actualizacion'. 'alta' nunca lo lee del
 * payload, pero SIEMPRE fuerza `activa:true` en la tarjeta resultante (bug real 2026-11-05: Kore
 * manda 'alta' tanto para un cliente nuevo como para uno que había eliminado su tarjeta y se
 * registra de nuevo — en ambos casos la tarjeta debe quedar activa). 'baja' ya tiene su propio
 * camino fijo a false.
 * `tarjetaLegada`: SOLO 'actualizacion' lo lee. Una 'alta' siempre crea la tarjeta sin este dato
 * (lo trae después el match inicial o un `actualizacion` posterior), y 'baja' no tiene nada que
 * ver con el número de tarjeta.
 * `nivel`: OBLIGATORIO en 'alta' y 'actualizacion' (2026-11-06, pedido explícito del usuario — Kore
 * controla el nivel por completo, Numo lo necesita siempre, incluso en un ajuste). Si no viene
 * (undefined/null/vacío), 400 "nivel es requerido." ANTES de llegar a _procesarAlta/
 * _procesarActualizacion. Debe ser uno de los niveles activos del catálogo real (`nivelEsValido()`,
 * ver LoyaltyKoreNivel.js), otro valor responde 400. 'alta' sobre una tarjeta nueva lo usa como
 * nivel inicial. 'actualizacion' (y 'alta' cuando cae a actualización) lo cambia vía
 * `cambiarNivel()` si viene distinto al actual — mismo primitivo que el botón manual "Cambiar
 * nivel", queda en `LoyaltyTierHistory`. 'baja' no lo lee ni lo exige.
 */
// Notificación en vivo (2026-11-10, pedido explícito del usuario): los 4 webhooks de Kore en
// Lealtad no emitían nada por socket hasta hoy — esto alimenta la bandeja "pendientes de
// revisión Kore" (clientes-pendientes.component.ts) para que se autorefresque sin que alguien
// tenga que recargar. Broadcast GLOBAL (sin sala, `emitToAll`) a propósito: no hay una dimensión
// de scoping real acá (cualquiera con `loyalty:read` ve la misma bandeja completa), mismo
// criterio ya usado por `ErpReversionCreatedEvent`/`CollectionRequestCreatedEvent` en el
// dominio de Bancos. Señal de invalidación pura (solo `cardId`, sin datos de negocio) — el
// frontend vuelve a pedir el listado por REST, nunca se duplica el dato en el socket.
async function procesarClienteKore(payload) {
  const { evento, telefono, nombre, activo, tarjetaLegada, nivel } = payload ?? {};

  if (!EVENTOS_VALIDOS.includes(evento)) {
    throw new BadRequestError(`evento inválido: "${evento}". Debe ser uno de: ${EVENTOS_VALIDOS.join(', ')}.`);
  }
  const koreClienteId = _requiereKoreClienteId(payload?.koreClienteId);

  if (evento === 'baja') {
    const resultado = await _procesarBaja({ koreClienteId, telefono });
    emitToAll('lealtad-cliente-kore:actualizado', { cardId: resultado.cardId });
    return resultado;
  }

  // `nivel` obligatorio (2026-11-06, tercera ronda) — gate único acá para que 'alta' (tarjeta nueva
  // Y tarjeta existente que cae a actualización) y 'actualizacion' lo exijan por igual, sin
  // duplicar el chequeo en cada rama interna.
  if (nivel === undefined || nivel === null || String(nivel).trim() === '') {
    throw new BadRequestError('nivel es requerido.');
  }

  let telefonoNorm;
  try {
    telefonoNorm = normalizarTelefono(telefono);
  } catch (err) {
    throw new BadRequestError(err.message);
  }

  const resultado = evento === 'alta'
    ? await _procesarAlta({ koreClienteId, telefono: telefonoNorm, nombre, nivel })
    : await _procesarActualizacion({
      koreClienteId, telefono: telefonoNorm, nombre, activo, tarjetaLegada, nivel,
    });
  emitToAll('lealtad-cliente-kore:actualizado', { cardId: resultado.cardId });
  return resultado;
}

module.exports = { procesarClienteKore };
