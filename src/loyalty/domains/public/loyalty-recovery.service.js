'use strict';

/**
 * domains/public/loyalty-recovery.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — recuperación de NIP en 2 pasos para la consulta pública
 * (cliente final, sin sesión). Mismo threat model que loyalty-public.service.js
 * (D6-D8, Threat Matrix): toda falla debe ser indistinguible desde afuera, con
 * igualación de tiempo e intentos limitados.
 *
 * Paso 1 (`solicitarCodigo`): dado un teléfono, genera y persiste (hasheado) un
 * código de 6 dígitos con vencimiento de 10 min. Nunca revela si el teléfono
 * existe o no — misma respuesta genérica en ambos casos (anti-enumeración,
 * igual que consultaPublica nunca distingue "teléfono desconocido" de "NIP
 * incorrecto"). El "envío" del código está SIMULADO: no hay proveedor de
 * mensajería elegido todavía (pendiente real del proyecto, Brevo/WhatsApp
 * API) — el código se loguea (nunca en texto plano hacia afuera) y, fuera de
 * producción, se incluye en la respuesta bajo `codigoDev` para poder probar el
 * flujo completo. Buscar `codigoDev` cuando se conecte el proveedor real: hay
 * que borrar esa rama.
 *
 * Paso 2 (`confirmarCodigo`): valida el código contra el hash guardado y, si
 * es correcto, genera un NIP nuevo (el cliente nunca elige su propio NIP —
 * mismo criterio que crearTarjeta/cambiarNip del lado staff) y lo devuelve una
 * única vez. El código es de un solo uso: se limpia al confirmar, sea cual
 * sea el resultado del intento (correcto limpia todo; incorrecto solo
 * incrementa el contador de fallos, el código sigue vivo hasta expirar o
 * hasta el 5to fallo).
 */

const { LoyaltyCard } = require('../../../shared/models/postgres');
const { UnprocessableError } = require('../../../shared/errors/AppError');
const {
  generarNip, hashNip,
  generarCodigoRecuperacion, hashCodigo, compararCodigo, DUMMY_CODIGO_HASH,
  MAX_INTENTOS_FALLIDOS, LOCKOUT_MS,
} = require('../../shared/nip.util');
const { logger } = require('../../../shared/utils/logger');

const CODIGO_VIGENCIA_MS = 10 * 60 * 1000;

const TELEFONO_RE = /^\d{10}$/;
const CODIGO_RE = /^\d{6}$/;

const MENSAJE_SOLICITUD_GENERICO =
  'Si el teléfono está registrado, te enviaremos un código de verificación.';

class RecoveryFailedError extends UnprocessableError {
  constructor() {
    super('El código no es válido o expiró.');
    this.code = 'LOYALTY_RECOVERY_FAILED';
  }
}

/** Nunca el teléfono completo en un log — solo los últimos 4 dígitos. */
function _maskTelefono(telefono) {
  const str = String(telefono ?? '');
  return str.length >= 4 ? `***${str.slice(-4)}` : '***';
}

function _logFallo(motivo, telefono, ip) {
  logger.warn(`[loyalty-recovery] Intento fallido (${motivo}) tel=${_maskTelefono(telefono)} ip=${ip ?? 'desconocida'}`);
}

/**
 * Paso 1: solicita un código de verificación para recuperar el NIP.
 * SIEMPRE responde el mismo mensaje genérico, exista o no la tarjeta — nunca
 * lanza por "teléfono no encontrado" (ese es el punto anti-enumeración).
 *
 * @param {string} telefono
 * @param {{ip?: string}} [ctx]
 * @returns {Promise<{mensaje: string, codigoDev?: string}>}
 */
async function solicitarCodigo(telefono, { ip } = {}) {
  const telefonoStr = String(telefono ?? '').trim();

  if (!TELEFONO_RE.test(telefonoStr)) {
    // Igualación de tiempo: un formato inválido no debería responder más
    // rápido que un teléfono válido pero inexistente.
    await compararCodigo('formato-invalido', DUMMY_CODIGO_HASH);
    _logFallo('formato inválido', telefonoStr, ip);
    return { mensaje: MENSAJE_SOLICITUD_GENERICO };
  }

  const card = await LoyaltyCard.findOne({ where: { telefono: telefonoStr } });

  if (!card) {
    // Igualación de tiempo (mismo criterio que consultaPublica con
    // DUMMY_HASH): sin esto, "teléfono no existe" respondería notablemente
    // más rápido que generar+hashear un código real, una fuga de timing que
    // permitiría enumerar teléfonos registrados.
    await compararCodigo('formato-invalido', DUMMY_CODIGO_HASH);
    _logFallo('teléfono desconocido', telefonoStr, ip);
    return { mensaje: MENSAJE_SOLICITUD_GENERICO };
  }

  const codigo = generarCodigoRecuperacion();
  const recuperacionCodigoHash = await hashCodigo(codigo);
  await card.update({
    recuperacionCodigoHash,
    recuperacionExpiraEn: new Date(Date.now() + CODIGO_VIGENCIA_MS),
    recuperacionIntentosFallidos: 0,
    recuperacionBloqueadaHasta: null,
  });

  // El código NUNCA en el log, igual que el NIP — solo confirmamos que se
  // generó uno nuevo para ese teléfono (enmascarado).
  logger.info(`[loyalty-recovery] Código generado tel=${_maskTelefono(telefonoStr)}`);

  const respuesta = { mensaje: MENSAJE_SOLICITUD_GENERICO };
  // SIMULACIÓN TEMPORAL — no hay proveedor de mensajería elegido todavía
  // (Brevo/WhatsApp API, pendiente real del proyecto). Remover esta rama
  // cuando se conecte el envío real; hasta entonces, fuera de producción,
  // el código viaja en la respuesta para poder probar el flujo completo.
  if (process.env.NODE_ENV !== 'production') {
    respuesta.codigoDev = codigo;
  }
  return respuesta;
}

/**
 * Paso 2: confirma el código y, si es válido, genera y devuelve un NIP nuevo.
 * UN SOLO camino de error para: teléfono no encontrado, formato inválido, sin
 * código solicitado, código expirado, tarjeta bloqueada, código incorrecto —
 * nunca distingue la causa (mismo criterio que LookupFailedError).
 *
 * @param {string} telefono
 * @param {string} codigo
 * @param {{ip?: string}} [ctx]
 * @returns {Promise<{nip: string}>}
 */
async function confirmarCodigo(telefono, codigo, { ip } = {}) {
  const telefonoStr = String(telefono ?? '').trim();
  const codigoStr = String(codigo ?? '').trim();

  if (!TELEFONO_RE.test(telefonoStr) || !CODIGO_RE.test(codigoStr)) {
    await compararCodigo(codigoStr || 'formato-invalido', DUMMY_CODIGO_HASH);
    _logFallo('formato inválido', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  const card = await LoyaltyCard.scope('withRecuperacionCodigoHash').findOne({ where: { telefono: telefonoStr } });

  if (!card) {
    await compararCodigo(codigoStr, DUMMY_CODIGO_HASH);
    _logFallo('teléfono desconocido', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  const bloqueada = card.recuperacionBloqueadaHasta != null && new Date(card.recuperacionBloqueadaHasta) > new Date();
  if (bloqueada) {
    // Igual se compara (contra el hash real si existe, si no contra el dummy)
    // para no introducir una fuga de timing entre "bloqueada" y "código
    // incorrecto" — el resultado se descarta a propósito.
    await compararCodigo(codigoStr, card.recuperacionCodigoHash || DUMMY_CODIGO_HASH);
    _logFallo('recuperación bloqueada', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  const sinCodigoVigente =
    !card.recuperacionCodigoHash ||
    card.recuperacionExpiraEn == null ||
    new Date(card.recuperacionExpiraEn) <= new Date();
  if (sinCodigoVigente) {
    await compararCodigo(codigoStr, DUMMY_CODIGO_HASH);
    _logFallo('sin código vigente', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  const codigoOk = await compararCodigo(codigoStr, card.recuperacionCodigoHash);
  if (!codigoOk) {
    const intentos = card.recuperacionIntentosFallidos + 1;
    const cambios = { recuperacionIntentosFallidos: intentos };
    if (intentos >= MAX_INTENTOS_FALLIDOS) {
      cambios.recuperacionBloqueadaHasta = new Date(Date.now() + LOCKOUT_MS);
    }
    await card.update(cambios);
    _logFallo('código incorrecto', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  const nip = generarNip();
  const nipHash = await hashNip(nip);
  await card.update({
    nipHash,
    nipIntentosFallidos: 0,
    nipBloqueadoHasta: null,
    // Código de un solo uso: se limpia por completo al confirmarse.
    recuperacionCodigoHash: null,
    recuperacionExpiraEn: null,
    recuperacionIntentosFallidos: 0,
    recuperacionBloqueadaHasta: null,
    // Auditoría mínima (mismo criterio que cambiarNip de staff): quién/cuándo,
    // sin tabla de historial completo.
    updatedById: null,
    updatedByNombre: 'Recuperación pública (auto-servicio)',
  });

  return { nip };
}

module.exports = { solicitarCodigo, confirmarCodigo, RecoveryFailedError };
