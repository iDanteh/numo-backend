'use strict';

/**
 * domains/public/loyalty-recovery.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — código de verificación en 2 pasos para la consulta
 * pública de puntos (cliente final, sin sesión). Mismo threat model que
 * loyalty-public.service.js (D6-D8, Threat Matrix): toda falla debe ser
 * indistinguible desde afuera, con igualación de tiempo e intentos limitados.
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
 * Paso 2 (`confirmarCodigoConsulta`, 2026-10-26 — reemplazo del NIP estático en
 * la ruta pública de consulta de puntos, ver memoria
 * project_lealtad_consulta_publica_otp): valida el código (factorizado en
 * `_validarCodigo`) y, si es correcto, devuelve el mismo payload mínimo que
 * `loyalty-public.service.js#consultaPublica` (nombre, nivel, saldoPuntos). El
 * código es de un solo uso: se consume solo si el intento tiene éxito
 * completo. El NIP estático de la tarjeta no se toca acá — sigue existiendo
 * para uso interno de staff (revelar/cambiar NIP en tarjeta-detail).
 *
 * El flujo "recuperar NIP olvidado" que originalmente vivía en este archivo
 * (`confirmarCodigo`, ruta pública `/recuperacion/confirmar`) se eliminó
 * 2026-10-26: quedó huérfano una vez que el NIP dejó de ser necesario para
 * cualquier acción de cara al cliente final.
 *
 * Auditoría (2026-10-26, ver LoyaltySecurityEvent.js): cada intento (éxito o
 * fallo) de los dos pasos se registra en una tabla append-only vía
 * `_registrarEvento`, llamada desde `_logFallo` y desde los dos caminos de
 * éxito. Complementa — no reemplaza — el log de Winston existente: permite
 * investigar patrones de abuso por teléfono o IP a través del tiempo, algo
 * que el log rotativo y los contadores de `LoyaltyCard` (que se pisan) no
 * permiten. Fire-and-forget: un fallo al escribir el evento nunca rompe el
 * flujo de negocio real.
 */

const { LoyaltyCard, LoyaltySecurityEvent } = require('../../../shared/models/postgres');
const { UnprocessableError } = require('../../../shared/errors/AppError');
const {
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

/**
 * Auditoría append-only (ver LoyaltySecurityEvent.js) — fire-and-forget: un
 * fallo al escribir el evento nunca debe romper el flujo de negocio real, por
 * eso se atrapa acá adentro y nunca se relanza ni se espera en el llamador.
 */
async function _registrarEvento(tipo, resultado, motivo, telefono, ip) {
  try {
    await LoyaltySecurityEvent.create({
      tipo, resultado, motivo: motivo ?? null, telefono: telefono || null, ip: ip ?? null,
    });
  } catch (err) {
    logger.error(`[loyalty-recovery] No se pudo registrar evento de auditoría (tipo=${tipo}, resultado=${resultado}): ${err.message}`);
  }
}

function _logFallo(tipo, motivo, telefono, ip) {
  logger.warn(`[loyalty-recovery] Intento fallido (${motivo}) tel=${_maskTelefono(telefono)} ip=${ip ?? 'desconocida'}`);
  _registrarEvento(tipo, 'fallo', motivo, telefono, ip);
}

/**
 * Paso 1: solicita un código de verificación para la consulta pública.
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
    _logFallo('solicitud_codigo', 'formato invalido', telefonoStr, ip);
    return { mensaje: MENSAJE_SOLICITUD_GENERICO };
  }

  const card = await LoyaltyCard.findOne({ where: { telefono: telefonoStr } });

  if (!card) {
    // Igualación de tiempo (mismo criterio que consultaPublica con
    // DUMMY_HASH): sin esto, "teléfono no existe" respondería notablemente
    // más rápido que generar+hashear un código real, una fuga de timing que
    // permitiría enumerar teléfonos registrados.
    await compararCodigo('formato-invalido', DUMMY_CODIGO_HASH);
    _logFallo('solicitud_codigo', 'telefono desconocido', telefonoStr, ip);
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
  _registrarEvento('solicitud_codigo', 'exito', null, telefonoStr, ip);

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

/** Campos que limpian el código de un solo uso — se aplican en el `update()`
 *  de éxito de `confirmarCodigoConsulta` (nunca en la propia validación: un
 *  código solo se consume si el intento efectivamente tuvo éxito). */
const CODIGO_CONSUMIDO = {
  recuperacionCodigoHash: null,
  recuperacionExpiraEn: null,
  recuperacionIntentosFallidos: 0,
  recuperacionBloqueadaHasta: null,
};

/**
 * Valida el código contra el hash guardado y devuelve la tarjeta si es
 * correcto. Lanza `RecoveryFailedError` en cualquier camino de falla —
 * teléfono no encontrado, formato inválido, sin código vigente, tarjeta
 * bloqueada o código incorrecto — nunca distingue la causa. Factorizada
 * aparte de `confirmarCodigoConsulta` para mantener la validación separada de
 * qué hacer con el resultado. No persiste nada por sí misma: el llamador
 * decide cómo consumir el código (ver `CODIGO_CONSUMIDO`) dentro de su propio
 * `update()`.
 *
 * @param {string} telefono
 * @param {string} codigo
 * @param {{ip?: string}} [ctx]
 * @returns {Promise<import('../../../shared/models/postgres').LoyaltyCard>}
 */
async function _validarCodigo(telefono, codigo, { ip } = {}) {
  const telefonoStr = String(telefono ?? '').trim();
  const codigoStr = String(codigo ?? '').trim();

  if (!TELEFONO_RE.test(telefonoStr) || !CODIGO_RE.test(codigoStr)) {
    await compararCodigo(codigoStr || 'formato-invalido', DUMMY_CODIGO_HASH);
    _logFallo('confirmacion_codigo', 'formato invalido', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  const card = await LoyaltyCard.scope('withRecuperacionCodigoHash').findOne({ where: { telefono: telefonoStr } });

  if (!card) {
    await compararCodigo(codigoStr, DUMMY_CODIGO_HASH);
    _logFallo('confirmacion_codigo', 'telefono desconocido', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  const bloqueada = card.recuperacionBloqueadaHasta != null && new Date(card.recuperacionBloqueadaHasta) > new Date();
  if (bloqueada) {
    // Igual se compara (contra el hash real si existe, si no contra el dummy)
    // para no introducir una fuga de timing entre "bloqueada" y "código
    // incorrecto" — el resultado se descarta a propósito.
    await compararCodigo(codigoStr, card.recuperacionCodigoHash || DUMMY_CODIGO_HASH);
    _logFallo('confirmacion_codigo', 'recuperacion bloqueada', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  const sinCodigoVigente =
    !card.recuperacionCodigoHash ||
    card.recuperacionExpiraEn == null ||
    new Date(card.recuperacionExpiraEn) <= new Date();
  if (sinCodigoVigente) {
    await compararCodigo(codigoStr, DUMMY_CODIGO_HASH);
    _logFallo('confirmacion_codigo', 'sin codigo vigente', telefonoStr, ip);
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
    _logFallo('confirmacion_codigo', 'codigo incorrecto', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  return card;
}

/**
 * Paso 2 (flujo "consulta de puntos", reemplaza al NIP estático): confirma
 * el código y devuelve el mismo payload mínimo que
 * `loyalty-public.service.js#consultaPublica` — nunca genera ni toca el NIP.
 *
 * @param {string} telefono
 * @param {string} codigo
 * @param {{ip?: string}} [ctx]
 * @returns {Promise<{nombre: string, nivel: string, saldoPuntos: string}>}
 */
async function confirmarCodigoConsulta(telefono, codigo, ctx = {}) {
  const card = await _validarCodigo(telefono, codigo, ctx);

  // Mismo criterio que consultaPublica: una tarjeta inactiva no debe
  // devolver saldo, con el mismo error genérico (nunca distinguir la causa).
  // No consume el código acá — un código solo se da por usado si el intento
  // efectivamente tuvo éxito.
  if (!card.activa) {
    _logFallo('confirmacion_codigo', 'tarjeta inactiva', telefono, ctx.ip);
    throw new RecoveryFailedError();
  }

  await card.update(CODIGO_CONSUMIDO);
  _registrarEvento('confirmacion_codigo', 'exito', null, card.telefono, ctx.ip);

  const primerNombre = String(card.nombre ?? '').trim().split(/\s+/)[0] ?? '';
  return { nombre: primerNombre, nivel: card.nivel, saldoPuntos: card.saldoPuntos };
}

module.exports = { solicitarCodigo, confirmarCodigoConsulta, RecoveryFailedError };
