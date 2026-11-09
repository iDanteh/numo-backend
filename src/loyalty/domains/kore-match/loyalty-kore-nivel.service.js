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

const EVENTOS_VALIDOS = ['alta', 'actualizacion', 'baja'];

function _requiereKoreId(koreId) {
  const v = String(koreId ?? '').trim();
  if (!v) throw new BadRequestError('koreId es requerido.');
  return v;
}

/** `alta`: requiere koreId+nombre+porcentaje (a diferencia de clientes, acá no hay alta parcial —
 *  un nivel sin % no tiene sentido de negocio). Idempotente: si ya existe por koreId, o si adopta
 *  una fila transicional por nombre, actualiza en vez de duplicar. `estatus` (2026-11-06, pedido
 *  explícito del usuario): opcional — si Kore lo manda, define el `activo` inicial; si se omite,
 *  sigue el default de siempre (`true`), cero regresión. */
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
    koreId, nombre, porcentaje: Number(porcentaje), activo: estatus === undefined ? true : Boolean(estatus),
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

/** `actualizacion`: ajuste de nombre/porcentaje/estatus por `koreId`, campos opcionales (igual
 *  criterio que el webhook de clientes — si no viene, Numo no lo toca). `estatus` (2026-11-06,
 *  pedido explícito del usuario: "controlar qué niveles están activos o no"): booleano, mapea
 *  directo a `activo` — mismo criterio que `activo` en `loyalty-kore-cliente.service.js`. No
 *  reemplaza al evento `baja` (que sigue funcionando igual), solo da una vía más directa para
 *  activar/desactivar sin tener que mandar un evento aparte. */
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
  if (estatus !== undefined) updates.activo = Boolean(estatus);
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
 * los toca). `estatus` (2026-11-06): booleano opcional, válido en 'alta' (define el `activo`
 * inicial, default `true` si se omite) y 'actualizacion' (activa/desactiva, si se omite no toca
 * `activo`) — vía más directa que 'baja' para lo mismo, no la reemplaza. 'baja' no lee ninguno de
 * estos campos.
 */
async function procesarNivelKore(payload) {
  const {
    evento, nombre, porcentaje, estatus,
  } = payload ?? {};

  if (!EVENTOS_VALIDOS.includes(evento)) {
    throw new BadRequestError(`evento inválido: "${evento}". Debe ser uno de: ${EVENTOS_VALIDOS.join(', ')}.`);
  }
  const koreId = _requiereKoreId(payload?.koreId);
  const nombreTrim = nombre !== undefined ? String(nombre).trim() : undefined;

  if (evento === 'baja') return _procesarBaja({ koreId });
  if (evento === 'alta') return _procesarAlta({
    koreId, nombre: nombreTrim, porcentaje, estatus,
  });
  return _procesarActualizacion({
    koreId, nombre: nombreTrim, porcentaje, estatus,
  });
}

module.exports = { procesarNivelKore };
