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
 *
 * Sesión corta post-código (2026-11-09, pedido explícito del usuario): al
 * confirmar el código OTP se emite un token de sesión (`consultaSesionTokenHash`
 * en LoyaltyCard, alta entropía — ver generarSesionToken/nip.util.js, NO es el
 * código OTP ni se compara con bcrypt) que permite "ver de nuevo" el saldo
 * (`verConsultaSesion`) hasta `SESION_USOS_INICIALES` veces más SIN pedir un
 * código nuevo — la vista que ya se muestra al confirmar el código no cuenta
 * contra ese número (1 vista por código + 3 por sesión = 4 totales,
 * confirmado con el usuario). Al agotarse los usos, la sesión se limpia
 * (token/usos/expiración a null) para que no se pueda reusar aunque todavía
 * esté dentro de la ventana de tiempo. La misma sesión habilita
 * `guardarEmailConsulta` (dejar un correo de contacto futuro) — eso NO
 * consume un uso ni toca `ultimaConsultaAt`/`totalConsultas`, no es "ver el
 * saldo".
 *
 * Reingreso transparente (2026-11-09, pedido explícito del usuario): el frontend NO expone
 * ningún botón "ver de nuevo" — en su lugar, persiste el token en el dispositivo (localStorage) y,
 * si el cliente sale y vuelve a tipear el MISMO teléfono, reintenta `verConsultaSesion` antes de
 * pedir un código nuevo. Transparente a este archivo: desde acá es indistinguible de cualquier
 * otra llamada a `verConsultaSesion`.
 *
 * Historial de email (2026-11-09, solo interno/staff en tarjeta-detail — la consulta pública
 * NUNCA lo muestra): cada cambio de `emailContacto` vía `guardarEmailConsulta` se audita con
 * `registrarCambioTarjeta` (mismo mecanismo que teléfono/nombre/activa, ver
 * loyalty-card-audit.service.js), con `origen: 'cliente'` nuevo (antes solo 'numo'/'kore').
 *
 * Límite mensual de consultas CON CÓDIGO (2026-11-09, pedido explícito del usuario, blindaje
 * adicional): tope configurable (`CONSULTA_CODIGO_MAX_MES`, ver loyalty-config.service.js) de
 * cuántos ciclos de código completos (`solicitarCodigo` + `confirmarCodigoConsulta` exitoso) puede
 * hacer UNA tarjeta por mes calendario (zona America/Mexico_City) — las 3 vistas de sesión NO
 * cuentan aparte, son parte del mismo ciclo. Preserva el threat model al 100%: si el límite ya se
 * alcanzó, `solicitarCodigo` simplemente NO genera un código real (deja `recuperacionCodigoHash`
 * intacto) pero devuelve el MISMO mensaje genérico de siempre — ningún campo, status ni timing
 * distinto hacia afuera. `confirmarCodigoConsulta` no necesita ningún chequeo nuevo: sin código
 * real generado, cualquier intento de confirmación falla solo en `_validarCodigo` por "sin código
 * vigente", el mismo error de siempre.
 */

const { LoyaltyCard, LoyaltySecurityEvent } = require('../../../shared/models/postgres');
const { UnprocessableError } = require('../../../shared/errors/AppError');
const {
  generarCodigoRecuperacion, hashCodigo, compararCodigo, DUMMY_CODIGO_HASH,
  MAX_INTENTOS_FALLIDOS, LOCKOUT_MS,
  generarSesionToken, hashSesionToken, compararSesionToken, DUMMY_SESION_TOKEN_HASH,
} = require('../../shared/nip.util');
const { logger } = require('../../../shared/utils/logger');
// Historial de movimientos (2026-11-03) — mismo criterio que loyalty-public.service.js#consultaPublica.
const ledgerService = require('../ledger/loyalty-ledger.service');
// Historial de cambios de catálogo (2026-11-09) — reusa la MISMA infraestructura que ya usa
// loyalty-card.service.js/loyalty-kore-cliente.service.js para teléfono/nombre/activa/etc., en vez
// de construir un historial propio para emailContacto. `origen: 'cliente'` es nuevo (antes solo
// 'numo'/'kore') — sin isIn en el modelo, no hace falta tocar LoyaltyCardAuditLog.js.
const { registrarCambioTarjeta } = require('../cards/loyalty-card-audit.service');
// Límite mensual de consultas con código (2026-11-09) — ver docstring de arriba.
const configService = require('../config/loyalty-config.service');

const CODIGO_VIGENCIA_MS = 10 * 60 * 1000;

// Sesión de consulta post-código (2026-11-09) — ver docstring de arriba.
const SESION_VIGENCIA_MS = 15 * 60 * 1000;
const SESION_USOS_INICIALES = 3;

/**
 * Período "YYYY-MM" en zona America/Mexico_City — mismo patrón timezone-safe que `_hoyMexico()`
 * en loyalty-config.service.js (y rateLimiter.js del dominio `visor`), a nivel MES en vez de día:
 * un server en UTC desfasaría el mes real en México hasta 6h antes/después de medianoche. Formato
 * verificado: `Intl.DateTimeFormat('en-CA', {month:'2-digit', year:'numeric', timeZone:...})` da
 * exactamente "2026-11" (no requiere parseo/recorte adicional).
 */
function _periodoActualMexico() {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Mexico_City', month: '2-digit', year: 'numeric',
  }).format(new Date());
}

/** Contador efectivo de ciclos de código de ESTE mes — reset perezoso de solo lectura: si el
 *  período guardado no es el actual (o nunca se usó este mecanismo), el contador real es 0, sin
 *  persistir nada todavía (eso lo decide cada llamador). */
function _consultaCodigoMesContadorActual(card) {
  return card.consultaCodigoMesPeriodo === _periodoActualMexico() ? card.consultaCodigoMesContador : 0;
}

const TELEFONO_RE = /^\d{10}$/;
const CODIGO_RE = /^\d{6}$/;
// Validación de formato simple, sin pretensión de RFC 5322 completo — alcanza para rechazar
// entradas obviamente mal tipeadas, no hay flujo de verificación por correo (ver docstring).
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

const MENSAJE_SOLICITUD_GENERICO =
  'Si el teléfono está registrado, te enviaremos un código de verificación.';

class RecoveryFailedError extends UnprocessableError {
  constructor() {
    super('El código no es válido o expiró.');
    this.code = 'LOYALTY_RECOVERY_FAILED';
  }
}

/** Distinto de RecoveryFailedError a propósito: un formato de correo inválido no es parte del
 *  threat model de enumeración/fuerza bruta (no revela nada sobre teléfono/sesión/código), es
 *  validación de formulario común — mezclarlo con "el código no es válido o expiró" confundiría
 *  a un cliente que ya pasó la verificación y solo tipeó mal su email. */
class EmailInvalidoError extends UnprocessableError {
  constructor() {
    super('Ingresá un correo electrónico válido.');
    this.code = 'LOYALTY_EMAIL_INVALIDO';
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

  // Límite mensual de consultas con código (2026-11-09) — ver docstring del archivo. Chequeo de
  // SOLA LECTURA (el reset perezoso del contador, si corresponde, se persiste recién al confirmar
  // un código exitoso, no acá). Si ya se alcanzó, NO se genera ningún código real: misma respuesta
  // genérica de siempre, cero señales distintas hacia afuera (anti-enumeración intacto).
  const maxMes = await configService.obtenerConsultaCodigoMaxMes();
  if (maxMes !== null && _consultaCodigoMesContadorActual(card) >= maxMes) {
    // Igualación de tiempo (mismo criterio que los 2 caminos de arriba): sin este dummy compare,
    // este camino sería medible más RÁPIDO que "código generado" (que sí hashea) — una fuga de
    // timing que permitiría distinguir "límite alcanzado" de "bajo el límite" desde afuera.
    await compararCodigo('formato-invalido', DUMMY_CODIGO_HASH);
    _logFallo('solicitud_codigo', 'limite mensual alcanzado', telefonoStr, ip);
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
 * @returns {Promise<{nombre: string, nivel: string, saldoPuntos: string, movimientos: object[]}>}
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

  // Sesión corta post-código (2026-11-09) — ver docstring del archivo. El token viaja en texto
  // plano SOLO en esta respuesta (igual que `codigoDev` arriba): nunca se vuelve a poder leer una
  // vez emitido, solo se guarda su hash.
  const sesionToken = generarSesionToken();
  // Límite mensual de consultas con código (2026-11-09) — el reset perezoso se PERSISTE acá (único
  // lugar que escribe estos 2 campos): si el período guardado ya no es el actual, arranca de 1 en
  // el período nuevo; si coincide, suma 1 al contador existente. Mismo update() que el resto de la
  // sesión — una sola escritura atómica.
  const periodoActual = _periodoActualMexico();
  const consultaCodigoMesContador = card.consultaCodigoMesPeriodo === periodoActual
    ? card.consultaCodigoMesContador + 1
    : 1;
  await card.update({
    ...CODIGO_CONSUMIDO,
    consultaSesionTokenHash:     hashSesionToken(sesionToken),
    consultaSesionUsosRestantes: SESION_USOS_INICIALES,
    consultaSesionExpiraEn:      new Date(Date.now() + SESION_VIGENCIA_MS),
    ultimaConsultaAt:            new Date(),
    totalConsultas:              (card.totalConsultas || 0) + 1,
    consultaCodigoMesPeriodo:    periodoActual,
    consultaCodigoMesContador,
  });
  _registrarEvento('confirmacion_codigo', 'exito', null, card.telefono, ctx.ip);

  // Nombre completo (2026-10-29, pedido explícito del usuario) — antes solo el primer nombre
  // (`primerNombre`); `card.nombre` ya es el combinado "Nombre Apellidos", no hace falta concatenar.
  // Historial de movimientos (2026-11-03) — últimos 20, payload recortado (ver ledgerService.listarMovimientosPublicos).
  const movimientos = await ledgerService.listarMovimientosPublicos(card.id);
  return {
    nombre: card.nombre, nivel: card.nivel, saldoPuntos: card.saldoPuntos, movimientos,
    token: sesionToken, usosRestantes: SESION_USOS_INICIALES,
    // Correo ya registrado en una sesión anterior (2026-11-09, si existe) — el frontend lo usa
    // para mostrar el campo ya bloqueado/opaco desde el primer vistazo, sin depender de que haya
    // pasado por guardarEmailConsulta() en ESTA misma sesión de navegador.
    emailContacto: card.emailContacto,
  };
}

/**
 * Valida teléfono+token de sesión contra lo guardado — mismo criterio que `_validarCodigo`:
 * SIEMPRE el mismo error (`RecoveryFailedError`), nunca distingue formato inválido, teléfono
 * desconocido, sesión inexistente/expirada/agotada, token incorrecto o tarjeta inactiva. No
 * consume nada por sí misma (eso lo decide cada llamador: `verConsultaSesion` resta un uso,
 * `guardarEmailConsulta` no toca la sesión).
 *
 * @param {string} telefono
 * @param {string} token
 * @param {string} tipoEvento  'ver_consulta' | 'guardar_email' — para _logFallo/auditoría.
 * @param {{ip?: string}} [ctx]
 * @returns {Promise<import('../../../shared/models/postgres').LoyaltyCard>}
 */
async function _validarSesion(telefono, token, tipoEvento, { ip } = {}) {
  const telefonoStr = String(telefono ?? '').trim();
  const tokenStr = String(token ?? '').trim();

  if (!TELEFONO_RE.test(telefonoStr) || !tokenStr) {
    // compararSesionToken es síncrona (SHA-256 + timingSafeEqual, no bcrypt) — igual se corre
    // contra el dummy para no introducir una fuga de timing entre "formato inválido" y "token
    // incorrecto contra una sesión real".
    compararSesionToken(tokenStr || 'formato-invalido', DUMMY_SESION_TOKEN_HASH);
    _logFallo(tipoEvento, 'formato invalido', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  const card = await LoyaltyCard.scope('withConsultaSesionTokenHash').findOne({ where: { telefono: telefonoStr } });

  if (!card) {
    compararSesionToken(tokenStr, DUMMY_SESION_TOKEN_HASH);
    _logFallo(tipoEvento, 'telefono desconocido', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  const sinSesionVigente =
    !card.consultaSesionTokenHash ||
    card.consultaSesionExpiraEn == null ||
    new Date(card.consultaSesionExpiraEn) <= new Date() ||
    !(card.consultaSesionUsosRestantes > 0);
  if (sinSesionVigente) {
    compararSesionToken(tokenStr, card.consultaSesionTokenHash || DUMMY_SESION_TOKEN_HASH);
    _logFallo(tipoEvento, 'sin sesion vigente', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  const tokenOk = compararSesionToken(tokenStr, card.consultaSesionTokenHash);
  if (!tokenOk) {
    _logFallo(tipoEvento, 'token incorrecto', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  // Mismo criterio que confirmarCodigoConsulta: una tarjeta inactiva no debe devolver saldo ni
  // dejar guardar un email, con el mismo error genérico.
  if (!card.activa) {
    _logFallo(tipoEvento, 'tarjeta inactiva', telefonoStr, ip);
    throw new RecoveryFailedError();
  }

  return card;
}

/**
 * "Ver de nuevo" el saldo sin pedir un código nuevo (2026-11-09) — consume uno de los
 * `consultaSesionUsosRestantes` de la sesión abierta por `confirmarCodigoConsulta`. Al llegar a
 * 0, limpia la sesión entera (no solo el contador) para que el token quede inservible aunque
 * siga dentro de la ventana de 15 min — a la 4ta vista hay que pedir un código nuevo, sin
 * excepciones.
 *
 * @param {string} telefono
 * @param {string} token
 * @param {{ip?: string}} [ctx]
 * @returns {Promise<{nombre: string, nivel: string, saldoPuntos: string, movimientos: object[], usosRestantes: number}>}
 */
async function verConsultaSesion(telefono, token, ctx = {}) {
  const card = await _validarSesion(telefono, token, 'ver_consulta', ctx);

  const usosRestantes = card.consultaSesionUsosRestantes - 1;
  const cambios = {
    ultimaConsultaAt: new Date(),
    totalConsultas:   (card.totalConsultas || 0) + 1,
  };
  if (usosRestantes > 0) {
    cambios.consultaSesionUsosRestantes = usosRestantes;
  } else {
    cambios.consultaSesionTokenHash = null;
    cambios.consultaSesionUsosRestantes = null;
    cambios.consultaSesionExpiraEn = null;
  }
  await card.update(cambios);
  _registrarEvento('ver_consulta', 'exito', null, card.telefono, ctx.ip);

  const movimientos = await ledgerService.listarMovimientosPublicos(card.id);
  return {
    nombre: card.nombre, nivel: card.nivel, saldoPuntos: card.saldoPuntos, movimientos,
    usosRestantes: Math.max(usosRestantes, 0),
    emailContacto: card.emailContacto,
  };
}

/**
 * Deja un correo de contacto futuro (2026-11-09) — requiere una sesión de consulta vigente (mismo
 * chequeo que `verConsultaSesion`), pero NO consume un uso ni toca `ultimaConsultaAt`/
 * `totalConsultas`: dejar un email no es "ver el saldo". Sin flujo de verificación de correo, solo
 * validación de formato — lanza `EmailInvalidoError` (distinto de `RecoveryFailedError`, ver su
 * docstring) si el formato no es válido.
 *
 * Historial (2026-11-09, visible solo para staff en tarjeta-detail, NUNCA en esta misma pantalla
 * pública): reusa `registrarCambioTarjeta` (mismo mecanismo que teléfono/nombre/activa en
 * loyalty-card.service.js) — `antes` se captura ANTES del `.update()` a propósito (Sequelize muta
 * la instancia en memoria apenas resuelve, ver docstring de registrarCambioTarjeta). Awaited
 * directo, sin try/catch — mismo criterio que el resto de call-sites existentes: un fallo acá es
 * de infraestructura real, no debe enmascararse con el error genérico de sesión.
 *
 * @param {string} telefono
 * @param {string} token
 * @param {string} email
 * @param {{ip?: string}} [ctx]
 * @returns {Promise<{mensaje: string, emailContacto: string}>}
 */
async function guardarEmailConsulta(telefono, token, email, ctx = {}) {
  const card = await _validarSesion(telefono, token, 'guardar_email', ctx);

  const emailStr = String(email ?? '').trim();
  if (!EMAIL_RE.test(emailStr)) {
    _logFallo('guardar_email', 'formato invalido', telefono, ctx.ip);
    throw new EmailInvalidoError();
  }

  const antes = { emailContacto: card.emailContacto ?? null };
  await card.update({ emailContacto: emailStr });
  await registrarCambioTarjeta({
    cardId: card.id,
    antes,
    despues: { emailContacto: emailStr },
    accion: 'actualizacion_email_publico',
    origen: 'cliente',
  });
  _registrarEvento('guardar_email', 'exito', null, card.telefono, ctx.ip);

  return { mensaje: 'Listo, guardamos tu correo.', emailContacto: emailStr };
}

module.exports = {
  solicitarCodigo, confirmarCodigoConsulta, verConsultaSesion, guardarEmailConsulta,
  RecoveryFailedError, EmailInvalidoError,
};
