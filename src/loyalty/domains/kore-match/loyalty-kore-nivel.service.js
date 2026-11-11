'use strict';

/**
 * domains/kore-match/loyalty-kore-nivel.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — webhook Kore→Numo de catálogo de niveles (2026-11-06,
 * pedido explícito del usuario), punto de entrada de
 * `POST /api/loyalty/kore/niveles`. Complementa al sync bajo demanda
 * (`loyalty-config.service.js#sincronizarNivelesKore`) con push en tiempo
 * real para altas/ajustes/bajas de nivel.
 *
 * Mismo criterio que `loyalty-kore-cliente.service.js`: esto es catálogo,
 * "último valor gana" — reenviar el mismo evento dos veces vuelve a escribir
 * los mismos valores, sin efecto financiero directo (a diferencia del
 * webhook de venta). No hay tabla de fallos ni idempotencia por ledger: un
 * evento que falla responde el error en la propia respuesta HTTP (Kore puede
 * reintentar).
 *
 * Bootstrap transicional (ver seed-lealtad.js + loyalty-config.service.js):
 * 3 filas Bronce/Plata/Oro con `koreId:null` existen desde el día uno para
 * no romper el alta/cambio de nivel mientras el catálogo real de Kore no
 * llegó. El primer `alta` de Kore para un `nombre` que ya existe como fila
 * transicional ADOPTA esa fila (le asigna el koreId real) en vez de
 * duplicarla — misma función que usa el sync, `_buscarOAdoptarNivelKore`,
 * reexportada de `loyalty-config.service.js` para no duplicar la lógica.
 */

const { LoyaltyKoreNivel } = require('../../../shared/models/postgres');
const { BadRequestError, NotFoundError } = require('../../../shared/errors/AppError');
const { _buscarOAdoptarNivelKore } = require('../config/loyalty-config.service');
const { emitToAll } = require('../../../banks/shared/socket');

const EVENTOS_VALIDOS = ['alta', 'actualizacion', 'baja'];

function _requiereKoreId(koreId) {
  const v = String(koreId ?? '').trim();
  if (!v) throw new BadRequestError('koreId es requerido.');
  return v;
}

/** `alta`: requiere koreId+nombre+porcentaje (a diferencia de clientes, acá no hay alta parcial —
 *  un nivel sin % no tiene sentido de negocio). Idempotente: si ya existe por koreId, o si adopta
 *  una fila transicional por nombre, actualiza en vez de duplicar. `estatus` (2026-11-06, pedido
 *  explícito del usuario, OBLIGATORIO desde 2026-11-11 — ver el gate en `procesarNivelKore`, mismo
 *  criterio que el gate de `nivel` en `loyalty-kore-cliente.service.js`): booleano, define el
 *  `activo` inicial del nivel. Ya no hay default por ausencia — la presencia está garantizada por
 *  el gate de arriba antes de llegar acá. */
async function _procesarAlta({
  koreId, nombre, porcentaje, estatus,
}) {
  if (!nombre) {
    throw new BadRequestError('nombre es requerido para dar de alta un nivel.');
  }
  if (porcentaje === undefined || porcentaje === null || !(Number(porcentaje) > 0)) {
    throw new BadRequestError('porcentaje es requerido y debe ser mayor a 0 para dar de alta un nivel.');
  }

  const datos = {
    koreId, nombre, porcentaje: Number(porcentaje), activo: Boolean(estatus),
  };
  const existente = await _buscarOAdoptarNivelKore({ koreId, nombre });
  if (existente) {
    await existente.update(datos);
    return { ok: true, evento: 'alta', nivelId: existente.id, creado: false };
  }

  try {
    const fila = await LoyaltyKoreNivel.create({ ...datos, esInicial: false });
    return { ok: true, evento: 'alta', nivelId: fila.id, creado: true };
  } catch (err) {
    if (err?.name === 'SequelizeUniqueConstraintError') {
      // Carrera: otra llamada creó/adoptó la fila entre el findOne de arriba y este create.
      const fila = await LoyaltyKoreNivel.findOne({ where: { koreId } });
      if (fila) {
        await fila.update(datos);
        return { ok: true, evento: 'alta', nivelId: fila.id, creado: false };
      }
    }
    throw err;
  }
}

/** `actualizacion`: ajuste de nombre/porcentaje/estatus por `koreId`. `nombre`/`porcentaje` siguen
 *  siendo opcionales (igual criterio que el webhook de clientes — si no vienen, Numo no los toca).
 *  `estatus` (2026-11-06, pedido explícito del usuario: "controlar qué niveles están activos o
 *  no"; OBLIGATORIO desde 2026-11-11 — ver el gate en `procesarNivelKore`, mismo criterio que el
 *  gate de `nivel` en `loyalty-kore-cliente.service.js`): booleano, mapea directo a `activo` —
 *  mismo criterio que `activo` en `loyalty-kore-cliente.service.js`. Al ser obligatorio, `activo`
 *  ya SIEMPRE se incluye en el `update()` de este evento (ya no es parcial para este campo
 *  específico, aunque `nombre`/`porcentaje` siguen siéndolo). No reemplaza al evento `baja` (que
 *  sigue funcionando igual, y sigue sin leer `estatus`), solo da una vía más directa para
 *  activar/desactivar sin tener que mandar un evento aparte — incluyendo "reactivar" un nivel que
 *  un `baja` anterior dejó inactivo, mandando `actualizacion` con `estatus:true`. */
async function _procesarActualizacion({
  koreId, nombre, porcentaje, estatus,
}) {
  const fila = await LoyaltyKoreNivel.findOne({ where: { koreId } });
  if (!fila) throw new NotFoundError('Nivel de Kore (koreId no encontrado)');

  const updates = {};
  if (nombre !== undefined && String(nombre).trim()) updates.nombre = String(nombre).trim();
  if (porcentaje !== undefined && porcentaje !== null) {
    if (!(Number(porcentaje) > 0)) {
      throw new BadRequestError('porcentaje debe ser mayor a 0.');
    }
    updates.porcentaje = Number(porcentaje);
  }
  updates.activo = Boolean(estatus);
  await fila.update(updates);
  return { ok: true, evento: 'actualizacion', nivelId: fila.id };
}

/** `baja`: desactiva por `koreId` — nunca borra la fila (puede quedar referenciada desde
 *  LoyaltyCard.nivel/LoyaltyTierHistory de tarjetas ya existentes). */
async function _procesarBaja({ koreId }) {
  const fila = await LoyaltyKoreNivel.findOne({ where: { koreId } });
  if (!fila) throw new NotFoundError('Nivel de Kore (koreId no encontrado)');
  await fila.update({ activo: false });
  return { ok: true, evento: 'baja', nivelId: fila.id };
}

/**
 * Punto de entrada del webhook `POST /api/loyalty/kore/niveles`.
 * `evento`: 'alta' | 'actualizacion' | 'baja'. `koreId` siempre requerido.
 * `nombre'/`porcentaje`: requeridos en 'alta', opcionales en 'actualizacion' (si se omiten, Numo no
 * los toca). `estatus` (2026-11-06, pedido explícito del usuario; OBLIGATORIO desde 2026-11-11,
 * pedido explícito del usuario, mismo criterio que el gate de `nivel` en
 * `loyalty-kore-cliente.service.js`): booleano, OBLIGATORIO en 'alta' (define el `activo` inicial,
 * ya no hay default) y 'actualizacion' (activa/desactiva, ya siempre toca `activo`). Si no viene
 * (undefined/null) en 'alta'/'actualizacion', 400 "estatus es requerido." ANTES de llegar a
 * `_procesarAlta`/`_procesarActualizacion` — ver el gate único más abajo. 'baja' NUNCA lee este
 * campo (desactiva incondicionalmente) y no lo exige. Nota: no existe un evento de "reactivación"
 * separado — se logra mandando 'actualizacion' con `estatus:true` sobre un nivel que un 'baja'
 * anterior dejó inactivo (o un nuevo 'alta' si Kore re-manda el mismo `koreId`).
 */
// Notificación en vivo (2026-11-10, pedido explícito del usuario — "relevante para el registro
// de acumulaciones manuales": el staff consulta esta tabla para saber qué % va a aplicar antes
// de ir a registrar una acumulación a mano, aunque el cálculo real en `registrarAcumulacion()`
// siempre usa el catálogo vigente sin importar el socket). Broadcast GLOBAL (sin sala, mismo
// criterio que `lealtad-cliente-kore:actualizado` de arriba) — alimenta la tabla "Niveles Kore"
// de `lealtad-config.component.ts`. Señal de invalidación pura, sin payload de negocio.
async function procesarNivelKore(payload) {
  const {
    evento, nombre, porcentaje, estatus,
  } = payload ?? {};

  if (!EVENTOS_VALIDOS.includes(evento)) {
    throw new BadRequestError(`evento inválido: "${evento}". Debe ser uno de: ${EVENTOS_VALIDOS.join(', ')}.`);
  }
  const koreId = _requiereKoreId(payload?.koreId);

  // `estatus` obligatorio en 'alta'/'actualizacion' (2026-11-11, pedido explícito del usuario) —
  // gate único acá, ANTES de branchear por evento, para que ambas ramas lo exijan por igual sin
  // duplicar el chequeo en cada una. Mismo criterio que el gate de `nivel` en
  // `procesarClienteKore` (loyalty-kore-cliente.service.js): Kore controla el dato por completo,
  // Numo lo necesita siempre, incluso en un ajuste — ya no hay un default razonable que asumir en
  // su ausencia. `estatus` es booleano: el chequeo es solo de presencia (undefined/null), nunca de
  // "truthy" — `false` es un valor válido y legítimo, no debe disparar este error. 'baja' nunca
  // entra acá (desactiva incondicionalmente, no lee este campo).
  if (evento !== 'baja' && (estatus === undefined || estatus === null)) {
    throw new BadRequestError('estatus es requerido.');
  }

  const nombreTrim = nombre !== undefined ? String(nombre).trim() : undefined;

  let resultado;
  if (evento === 'baja') {
    resultado = await _procesarBaja({ koreId });
  } else if (evento === 'alta') {
    resultado = await _procesarAlta({
      koreId, nombre: nombreTrim, porcentaje, estatus,
    });
  } else {
    resultado = await _procesarActualizacion({
      koreId, nombre: nombreTrim, porcentaje, estatus,
    });
  }
  emitToAll('lealtad-niveles-kore:actualizado', { nivelId: resultado.nivelId });
  return resultado;
}

module.exports = { procesarNivelKore };
