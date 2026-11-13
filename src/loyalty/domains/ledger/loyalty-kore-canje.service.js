'use strict';

/**
 * domains/ledger/loyalty-kore-canje.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — código de autorización de un solo uso para que un cliente
 * autorice canjear sus puntos como forma de pago dentro de una venta de Kore
 * (2026-11-13, pedido explícito del usuario — CAMBIO DE RESPONSABILIDAD: el
 * contrato previo del webhook `POST /venta`, ver `Docs/integracion-kore-puntos-
 * lealtad-DRAFT.md` sección 1, decía que esta confirmación era responsabilidad
 * EXCLUSIVA de Kore. Ahora es Numo quien genera, envía y valida el código — ver
 * documento nuevo `Docs/integracion-kore-canje-codigo-autorizacion-DRAFT.md`,
 * que reemplaza esa responsabilidad SOLO para este caso puntual, sin tocar el
 * documento viejo ni el diagrama Bizagi).
 *
 * Flujo real: Kore consulta el saldo (`GET /saldo`, ya existente) → cajero decide
 * cuántos puntos usar → Kore pide este código (`solicitarCodigoCanje`) → Numo lo
 * manda por el canal de mensajería de la consulta pública (hoy SIMULADO, mismo
 * criterio que `loyalty-recovery.service.js#solicitarCodigo` — no hay proveedor
 * real elegido todavía) → cajero tipea el código en Kore → Kore lo valida
 * (`validarCodigoCanje`) → si es válido, Kore cobra y recién ENTONCES llama a
 * `POST /venta` (sin cambios, ver `loyalty-kore-venta.service.js`) — ese
 * endpoint no sabe nada de este código, ya se validó antes de llegar ahí.
 *
 * Columnas PROPIAS en LoyaltyCard (`canjeCodigoHash`/`canjeCodigoExpiraEn`/
 * `canjeIntentosFallidos`/`canjeBloqueadaHasta`), separadas de las de
 * recuperación/consulta pública a propósito — un cliente puede tener una
 * consulta pública en curso desde su casa al mismo tiempo que un cajero le
 * dispara un canje en caja, sin que un flujo pise al otro.
 *
 * A diferencia de la consulta pública (que abre una sesión de varios usos), el
 * código de canje es de UN SOLO USO — se consume de inmediato en el primer
 * intento exitoso (no hay "ver de nuevo"). Vigencia configurable (minutos),
 * ver `loyalty-config.service.js#obtenerCanjeCodigoVigenciaMin` (default 5).
 *
 * Auth: este servicio no conoce permisos — lo gatea la ruta (`verifyKoreApiKey`,
 * mismo mecanismo server-a-servidor que el resto de los endpoints Kore→Numo,
 * ver `loyalty-kore-venta.routes.js`).
 *
 * `solicitarCodigoCanje` SÍ revela si el teléfono/tarjeta no existe o está
 * inactiva (a diferencia de la consulta pública): es tráfico autenticado
 * server-a-servidor con un partner que ya resolvió al cliente vía `/saldo`
 * antes de llegar acá, no tráfico público anónimo — el anti-enumeración de la
 * consulta pública no aplica, y Kore necesita un error accionable para su
 * propia UI. `validarCodigoCanje` SÍ mantiene un único error genérico (código
 * incorrecto/expirado/bloqueado/teléfono desconocido, todo igual) porque esa
 * función sí es la superficie real de fuerza bruta (adivinar un código de 6
 * dígitos), mismo criterio que el resto del módulo de recuperación.
 */

const { LoyaltyCard, LoyaltySecurityEvent } = require('../../../shared/models/postgres');
const { BadRequestError, NotFoundError, UnprocessableError } = require('../../../shared/errors/AppError');
const {
  generarCodigoRecuperacion, hashCodigo, compararCodigo, DUMMY_CODIGO_HASH,
  MAX_INTENTOS_FALLIDOS, LOCKOUT_MS,
} = require('../../shared/nip.util');
const { logger } = require('../../../shared/utils/logger');
const configService = require('../config/loyalty-config.service');

const TELEFONO_RE = /^\d{10}$/;
const CODIGO_RE = /^\d{6}$/;

class CanjeCodigoInvalidoError extends UnprocessableError {
  constructor() {
    super('El código no es válido o expiró.');
    this.code = 'LOYALTY_CANJE_CODIGO_INVALIDO';
  }
}

/** Nunca el teléfono completo en un log — solo los últimos 4 dígitos (mismo criterio que
 *  loyalty-recovery.service.js). */
function _maskTelefono(telefono) {
  const str = String(telefono ?? '');
  return str.length >= 4 ? `***${str.slice(-4)}` : '***';
}

/** Auditoría append-only (LoyaltySecurityEvent) — fire-and-forget, mismo criterio que
 *  loyalty-recovery.service.js#_registrarEvento: un fallo al auditar nunca debe romper el flujo
 *  real de un canje en caja. */
async function _registrarEvento(tipo, resultado, motivo, telefono) {
  try {
    await LoyaltySecurityEvent.create({
      tipo, resultado, motivo: motivo ?? null, telefono: telefono || null, ip: null,
    });
  } catch (err) {
    logger.error(`[loyalty-kore-canje] No se pudo registrar evento de auditoría (tipo=${tipo}, resultado=${resultado}): ${err.message}`);
  }
}

/**
 * Pide el código de autorización de canje para un teléfono. A diferencia de la consulta pública,
 * SÍ lanza si el teléfono tiene formato inválido, la tarjeta no existe o está inactiva — es
 * tráfico server-a-servidor autenticado con Kore, no anónimo (ver docstring del archivo).
 *
 * @param {string} telefono
 * @returns {Promise<{mensaje: string, codigoDev?: string}>}
 */
async function solicitarCodigoCanje(telefono) {
  const telefonoStr = String(telefono ?? '').trim();
  if (!TELEFONO_RE.test(telefonoStr)) {
    throw new BadRequestError('telefono inválido (10 dígitos).');
  }

  const card = await LoyaltyCard.findOne({ where: { telefono: telefonoStr } });
  if (!card) {
    _registrarEvento('solicitud_codigo_canje', 'fallo', 'telefono desconocido', telefonoStr);
    throw new NotFoundError('Tarjeta de Lealtad');
  }
  if (!card.activa) {
    _registrarEvento('solicitud_codigo_canje', 'fallo', 'tarjeta inactiva', telefonoStr);
    throw new BadRequestError('La tarjeta está inactiva, no se puede autorizar un canje.');
  }

  const vigenciaMin = await configService.obtenerCanjeCodigoVigenciaMin();
  const codigo = generarCodigoRecuperacion();
  const canjeCodigoHash = await hashCodigo(codigo);
  await card.update({
    canjeCodigoHash,
    canjeCodigoExpiraEn: new Date(Date.now() + vigenciaMin * 60 * 1000),
    canjeIntentosFallidos: 0,
    canjeBloqueadaHasta: null,
  });

  // El código NUNCA en el log, igual que el resto del módulo.
  logger.info(`[loyalty-kore-canje] Código de canje generado tel=${_maskTelefono(telefonoStr)}`);
  _registrarEvento('solicitud_codigo_canje', 'exito', null, telefonoStr);

  const respuesta = { mensaje: 'Código enviado' };
  // SIMULACIÓN TEMPORAL — mismo criterio y mismo motivo que
  // loyalty-recovery.service.js#solicitarCodigo: no hay proveedor de mensajería real elegido
  // todavía (WhatsApp/SMS). Remover esta rama cuando se conecte el envío real.
  if (process.env.NODE_ENV !== 'production') {
    respuesta.codigoDev = codigo;
  }
  return respuesta;
}

/**
 * Valida el código de canje. Un solo uso: se consume de inmediato si el intento tiene éxito (a
 * diferencia de la consulta pública, acá no hay sesión de varios usos). Único error genérico para
 * cualquier causa de falla (formato, teléfono desconocido, bloqueo, sin código vigente, código
 * incorrecto) — esta función SÍ es la superficie real de fuerza bruta.
 *
 * @param {string} telefono
 * @param {string} codigo
 * @returns {Promise<{valido: true}>}
 */
async function validarCodigoCanje(telefono, codigo) {
  const telefonoStr = String(telefono ?? '').trim();
  const codigoStr = String(codigo ?? '').trim();

  if (!TELEFONO_RE.test(telefonoStr) || !CODIGO_RE.test(codigoStr)) {
    await compararCodigo(codigoStr || 'formato-invalido', DUMMY_CODIGO_HASH);
    _registrarEvento('confirmacion_codigo_canje', 'fallo', 'formato invalido', telefonoStr);
    throw new CanjeCodigoInvalidoError();
  }

  const card = await LoyaltyCard.scope('withCanjeCodigoHash').findOne({ where: { telefono: telefonoStr } });
  if (!card) {
    await compararCodigo(codigoStr, DUMMY_CODIGO_HASH);
    _registrarEvento('confirmacion_codigo_canje', 'fallo', 'telefono desconocido', telefonoStr);
    throw new CanjeCodigoInvalidoError();
  }

  const bloqueada = card.canjeBloqueadaHasta != null && new Date(card.canjeBloqueadaHasta) > new Date();
  if (bloqueada) {
    await compararCodigo(codigoStr, card.canjeCodigoHash || DUMMY_CODIGO_HASH);
    _registrarEvento('confirmacion_codigo_canje', 'fallo', 'canje bloqueado', telefonoStr);
    throw new CanjeCodigoInvalidoError();
  }

  const sinCodigoVigente =
    !card.canjeCodigoHash ||
    card.canjeCodigoExpiraEn == null ||
    new Date(card.canjeCodigoExpiraEn) <= new Date();
  if (sinCodigoVigente) {
    await compararCodigo(codigoStr, DUMMY_CODIGO_HASH);
    _registrarEvento('confirmacion_codigo_canje', 'fallo', 'sin codigo vigente', telefonoStr);
    throw new CanjeCodigoInvalidoError();
  }

  const codigoOk = await compararCodigo(codigoStr, card.canjeCodigoHash);
  if (!codigoOk) {
    const intentos = card.canjeIntentosFallidos + 1;
    const cambios = { canjeIntentosFallidos: intentos };
    if (intentos >= MAX_INTENTOS_FALLIDOS) {
      cambios.canjeBloqueadaHasta = new Date(Date.now() + LOCKOUT_MS);
    }
    await card.update(cambios);
    _registrarEvento('confirmacion_codigo_canje', 'fallo', 'codigo incorrecto', telefonoStr);
    throw new CanjeCodigoInvalidoError();
  }

  // Éxito: se consume de inmediato — un solo uso, nunca una sesión de varios usos.
  await card.update({
    canjeCodigoHash: null,
    canjeCodigoExpiraEn: null,
    canjeIntentosFallidos: 0,
    canjeBloqueadaHasta: null,
  });
  _registrarEvento('confirmacion_codigo_canje', 'exito', null, telefonoStr);

  return { valido: true };
}

module.exports = { solicitarCodigoCanje, validarCodigoCanje, CanjeCodigoInvalidoError };
