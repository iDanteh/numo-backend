'use strict';

/**
 * loyalty/shared/nip.util.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — NIP de 4 dígitos (D6). Generado server-side con
 * `crypto.randomInt` (CSPRNG, no Math.random), nunca persistido en texto
 * plano. `DUMMY_HASH` (D8) existe solo para que `bcrypt.compare` corra contra
 * un teléfono desconocido en la consulta pública (PR4) y así igualar el
 * tiempo de respuesta entre "teléfono no existe" y "NIP incorrecto" — nunca
 * se usa para autenticar nada real.
 */

const crypto = require('crypto');
const bcrypt = require('bcryptjs');

const BCRYPT_COST = 10;

// "0000".."9999" repetidos + los dos patrones secuenciales obvios (D6).
const NIPS_DEBILES = new Set([
  '1234', '4321',
  ...Array.from({ length: 10 }, (_, d) => String(d).repeat(4)),
]);

/** Genera un NIP de 4 dígitos, reintentando si cae en la lista débil. */
function generarNip() {
  let nip;
  do {
    nip = String(crypto.randomInt(0, 10000)).padStart(4, '0');
  } while (NIPS_DEBILES.has(nip));
  return nip;
}

function hashNip(nip) {
  return bcrypt.hash(nip, BCRYPT_COST);
}

function compararNip(nip, hash) {
  return bcrypt.compare(nip, hash);
}

// Hash módulo-level de un NIP que nunca se asigna a ninguna tarjeta real —
// calculado una sola vez al cargar el módulo (costo 10, rápido en el boot).
const DUMMY_HASH = bcrypt.hashSync('no-such-card-nip', BCRYPT_COST);

// Umbral de intentos/lockout compartido entre la consulta pública
// (loyalty-public.service.js) y la recuperación de NIP en 2 pasos
// (loyalty-recovery.service.js) — un solo lugar para estos números mágicos,
// antes vivían hardcodeados solo en loyalty-public.service.js.
const MAX_INTENTOS_FALLIDOS = 5;
const LOCKOUT_MS = 15 * 60 * 1000;

// "000000".."999999" repetidos + las dos secuencias obvias (D6, mismo criterio que NIPS_DEBILES,
// agregado 2026-11-13 — hardening pedido explícito del usuario). El espacio de 1M ya hace la fuerza
// bruta inviable por sí solo (ver MAX_INTENTOS_FALLIDOS/LOCKOUT_MS), así que el beneficio real es
// marginal, pero deja este generador consistente con generarNip() sin costo.
const CODIGOS_DEBILES = new Set([
  '012345', '543210',
  ...Array.from({ length: 10 }, (_, d) => String(d).repeat(6)),
]);

/** Código de verificación de 6 dígitos — recuperación de NIP (loyalty-recovery.service.js) y código
 * de autorización de canje (loyalty-kore-canje.service.js). Generado 100% por el sistema (nunca
 * elegido por un usuario), reintentando si cae en un patrón obvio (CODIGOS_DEBILES). */
function generarCodigoRecuperacion() {
  let codigo;
  do {
    codigo = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
  } while (CODIGOS_DEBILES.has(codigo));
  return codigo;
}

function hashCodigo(codigo) {
  return bcrypt.hash(codigo, BCRYPT_COST);
}

function compararCodigo(codigo, hash) {
  return bcrypt.compare(codigo, hash);
}

// Mismo criterio que DUMMY_HASH: un hash que nunca se asigna a ninguna
// tarjeta real, usado para igualar tiempos cuando el teléfono no existe.
const DUMMY_CODIGO_HASH = bcrypt.hashSync('no-such-recovery-code', BCRYPT_COST);

// ── Token de sesión de consulta (2026-11-09) ──────────────────────────────
// Distinto del código OTP de arriba: NO es un secreto de baja entropía que un
// humano tipea (como el NIP o el código de 6 dígitos), es generado y
// comparado 100% por máquina — 32 bytes aleatorios (CSPRNG) ya son
// infactibles de fuerza bruta, así que acá alcanza con SHA-256 + comparación
// timing-safe (`crypto.timingSafeEqual`) en vez de bcrypt: más barato, y este
// token se revalida varias veces seguidas (hasta 3 "vistas" por sesión) sin
// el costo de bcrypt en cada una.
function generarSesionToken() {
  return crypto.randomBytes(32).toString('hex');
}

function hashSesionToken(token) {
  return crypto.createHash('sha256').update(token).digest('hex');
}

/** Comparación timing-safe de dos hashes hex de igual longitud. Si alguno de
 *  los dos (típicamente el guardado, cuando no hay sesión vigente) no mide lo
 *  mismo que el otro, igual se compara contra `DUMMY_SESION_TOKEN_HASH` para
 *  no filtrar por timing si hay o no una sesión real detrás del teléfono. */
function compararSesionToken(token, hashGuardado) {
  const tokenHash = Buffer.from(hashSesionToken(token), 'hex');
  const contraHash = Buffer.from(hashGuardado || DUMMY_SESION_TOKEN_HASH, 'hex');
  if (tokenHash.length !== contraHash.length) return false;
  return crypto.timingSafeEqual(tokenHash, contraHash);
}

// Mismo criterio que DUMMY_HASH/DUMMY_CODIGO_HASH: un hash que nunca se
// asigna a ninguna tarjeta real, usado para igualar tiempos cuando no hay
// sesión de consulta vigente.
const DUMMY_SESION_TOKEN_HASH = hashSesionToken('no-such-consulta-session-token');

module.exports = {
  generarNip, hashNip, compararNip, DUMMY_HASH, NIPS_DEBILES,
  MAX_INTENTOS_FALLIDOS, LOCKOUT_MS,
  generarCodigoRecuperacion, hashCodigo, compararCodigo, DUMMY_CODIGO_HASH, CODIGOS_DEBILES,
  generarSesionToken, hashSesionToken, compararSesionToken, DUMMY_SESION_TOKEN_HASH,
};
