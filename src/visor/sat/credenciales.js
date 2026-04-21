/**
 * Almacén temporal de credenciales e.firma cifradas con AES-256-GCM.
 *
 * Seguridad:
 *  - Cifrado: AES-256-GCM con IV aleatorio de 12 bytes por cada operación.
 *  - Clave maestra: CREDS_MASTER_KEY (32 bytes en hex) desde variables de entorno.
 *  - Formato almacenado: iv:authTag:datosCifrados (todo en hex).
 *  - TTL: 8 horas, expiración automática via índice MongoDB.
 *  - Las credenciales se eliminan al terminar cualquier job (éxito o fallo).
 *  - Nunca se loggea contenido de credenciales.
 *  - Los Buffers en memoria se limpian (fill 0) tras su uso.
 */

const crypto = require('crypto');
const SATCredencial = require('../models/SATCredencial');
const { logger } = require('../../shared/utils/logger');

const ALGORITHM      = 'aes-256-gcm';
const IV_LENGTH      = 12;   // bytes
const AUTH_TAG_LENGTH = 16;  // bytes

// ─────────────────────────────────────────────────────────────────────────────
// Clave maestra
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Retorna la clave maestra como Buffer de 32 bytes.
 * Lanza si no está configurada o tiene longitud incorrecta.
 */
const getMasterKey = () => {
  const hex = process.env.CREDS_MASTER_KEY;

  if (!hex) {
    throw new Error('CREDS_MASTER_KEY no está configurada en las variables de entorno.');
  }
  if (hex.length !== 64) {
    throw new Error(
      `CREDS_MASTER_KEY debe ser exactamente 64 caracteres hex (32 bytes). ` +
      `Longitud actual: ${hex.length}.`,
    );
  }
  if (!/^[0-9a-fA-F]{64}$/.test(hex)) {
    throw new Error('CREDS_MASTER_KEY contiene caracteres no hexadecimales.');
  }

  return Buffer.from(hex, 'hex');
};

// ─────────────────────────────────────────────────────────────────────────────
// Cifrado / Descifrado
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Cifra un Buffer o string con AES-256-GCM.
 * @param {Buffer|string} dato
 * @returns {string}  formato: iv:authTag:datosCifrados  (hex separado por ':')
 */
const cifrar = (dato) => {
  const key    = getMasterKey();
  const iv     = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv, { authTagLength: AUTH_TAG_LENGTH });

  const input     = Buffer.isBuffer(dato) ? dato : Buffer.from(dato, 'utf-8');
  const encrypted = Buffer.concat([cipher.update(input), cipher.final()]);
  const authTag   = cipher.getAuthTag();

  const resultado = `${iv.toString('hex')}:${authTag.toString('hex')}:${encrypted.toString('hex')}`;

  // Limpiar buffers intermedios
  iv.fill(0);
  authTag.fill(0);
  encrypted.fill(0);

  return resultado;
};

/**
 * Descifra un string en formato iv:authTag:datosCifrados.
 * Valida longitudes exactas antes de intentar el descifrado.
 * @param {string} datosCifrados
 * @returns {Buffer}
 */
const descifrar = (datosCifrados) => {
  if (typeof datosCifrados !== 'string') {
    throw new Error('El dato cifrado debe ser un string.');
  }

  const partes = datosCifrados.split(':');

  if (partes.length !== 3) {
    throw new Error('Formato de credencial inválido: se esperan 3 segmentos separados por ":".');
  }

  const [ivHex, authTagHex, encryptedHex] = partes;

  // Validar longitudes exactas (hex = bytes * 2)
  if (ivHex.length !== IV_LENGTH * 2) {
    throw new Error(`IV inválido: se esperan ${IV_LENGTH * 2} caracteres hex.`);
  }
  if (authTagHex.length !== AUTH_TAG_LENGTH * 2) {
    throw new Error(`AuthTag inválido: se esperan ${AUTH_TAG_LENGTH * 2} caracteres hex.`);
  }
  if (encryptedHex.length === 0) {
    throw new Error('Datos cifrados vacíos.');
  }
  if (!/^[0-9a-fA-F]+$/.test(ivHex) ||
      !/^[0-9a-fA-F]+$/.test(authTagHex) ||
      !/^[0-9a-fA-F]+$/.test(encryptedHex)) {
    throw new Error('Formato de credencial inválido: caracteres no hexadecimales.');
  }

  const key       = getMasterKey();
  const iv        = Buffer.from(ivHex,        'hex');
  const authTag   = Buffer.from(authTagHex,   'hex');
  const encrypted = Buffer.from(encryptedHex, 'hex');

  const decipher = crypto.createDecipheriv(ALGORITHM, key, iv, { authTagLength: AUTH_TAG_LENGTH });
  decipher.setAuthTag(authTag);

  const decrypted = Buffer.concat([decipher.update(encrypted), decipher.final()]);

  // Limpiar buffers intermedios
  iv.fill(0);
  authTag.fill(0);
  encrypted.fill(0);

  return decrypted;
};

// ─────────────────────────────────────────────────────────────────────────────
// API pública
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Guarda las credenciales cifradas en MongoDB.
 * Si ya existen credenciales para el RFC, las reemplaza y reinicia el TTL.
 *
 * @param {string} rfc
 * @param {{cerB64: string, keyB64: string, password: string}} credenciales
 */
const guardar = async (rfc, { cerB64, keyB64, password }) => {
  const rfcNorm = rfc.toUpperCase().trim();

  const cerCifrado      = cifrar(cerB64);
  const keyCifrado      = cifrar(keyB64);
  const passwordCifrado = cifrar(password);

  await SATCredencial.findOneAndUpdate(
    { rfc: rfcNorm },
    {
      rfc: rfcNorm,
      cerCifrado,
      keyCifrado,
      passwordCifrado,
      createdAt: new Date(), // Reinicia el TTL
    },
    { upsert: true, new: true },
  );

  logger.info(`[Credenciales] Credenciales guardadas para RFC: ${rfcNorm}`);
};

/**
 * Obtiene y descifra las credenciales de un RFC.
 * Los Buffers devueltos deben limpiarse con .fill(0) tras su uso.
 *
 * @param {string} rfc
 * @returns {Promise<{cerBuffer: Buffer, keyBuffer: Buffer, passwordBuffer: Buffer} | null>}
 */
const obtener = async (rfc) => {
  const rfcNorm = rfc.toUpperCase().trim();
  const doc = await SATCredencial.findOne({ rfc: rfcNorm });
  if (!doc) return null;

  const cerBuffer      = Buffer.from(descifrar(doc.cerCifrado).toString('utf-8'),  'base64');
  const keyBuffer      = Buffer.from(descifrar(doc.keyCifrado).toString('utf-8'),  'base64');
  const passwordBuffer = descifrar(doc.passwordCifrado);

  return { cerBuffer, keyBuffer, passwordBuffer };
};

/**
 * Limpia los Buffers de credenciales en memoria sobrescribiéndolos con ceros.
 * Llamar siempre tras terminar de usar las credenciales (éxito o fallo).
 *
 * @param {{ cerBuffer?: Buffer, keyBuffer?: Buffer, passwordBuffer?: Buffer } | null} creds
 */
const limpiarBuffers = (creds) => {
  if (!creds) return;
  try { creds.cerBuffer?.fill(0);      } catch (_) { /* ignorar */ }
  try { creds.keyBuffer?.fill(0);      } catch (_) { /* ignorar */ }
  try { creds.passwordBuffer?.fill(0); } catch (_) { /* ignorar */ }
};

/**
 * Elimina las credenciales de un RFC de MongoDB.
 * Siempre llamar al terminar un job (éxito o fallo), junto con limpiarBuffers().
 *
 * @param {string} rfc
 */
const eliminar = async (rfc) => {
  const rfcNorm = rfc.toUpperCase().trim();
  await SATCredencial.deleteOne({ rfc: rfcNorm });
  logger.info(`[Credenciales] Credenciales eliminadas para RFC: ${rfcNorm}`);
};

/**
 * Verifica si existen credenciales activas para un RFC.
 *
 * @param {string} rfc
 * @returns {Promise<{tiene: boolean, ttlSegundos: number | null}>}
 */
const tieneCredenciales = async (rfc) => {
  const rfcNorm = rfc.toUpperCase().trim();
  const doc = await SATCredencial.findOne({ rfc: rfcNorm }, 'createdAt');
  if (!doc) return { tiene: false, ttlSegundos: null };

  const TTL_MS      = 8 * 60 * 60 * 1000;
  const expiraEn    = doc.createdAt.getTime() + TTL_MS;
  const ttlSegundos = Math.max(0, Math.floor((expiraEn - Date.now()) / 1000));

  return { tiene: ttlSegundos > 0, ttlSegundos };
};

module.exports = { guardar, obtener, eliminar, limpiarBuffers, tieneCredenciales };