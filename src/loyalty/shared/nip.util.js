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

/** Código de verificación de 6 dígitos para recuperación de NIP. Generado
 * 100% por el sistema (nunca elegido por un usuario), por eso no hace falta
 * filtrar valores "débiles" como sí se hace con generarNip(). */
function generarCodigoRecuperacion() {
  return String(crypto.randomInt(0, 1000000)).padStart(6, '0');
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

module.exports = {
  generarNip, hashNip, compararNip, DUMMY_HASH, NIPS_DEBILES,
  MAX_INTENTOS_FALLIDOS, LOCKOUT_MS,
  generarCodigoRecuperacion, hashCodigo, compararCodigo, DUMMY_CODIGO_HASH,
};
