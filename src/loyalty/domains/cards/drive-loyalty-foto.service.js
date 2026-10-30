'use strict';

/**
 * drive-loyalty-foto.service.js — Sube la foto del cliente de una tarjeta de
 * Lealtad (`LoyaltyCard.fotoUrl`) a Google Drive, en vez de guardarla como
 * Buffer en Postgres. La foto es 100% capturada en Numo (cámara del
 * dispositivo del staff, nunca viene de Kore).
 *
 * Mismo patrón y MISMA cuenta de servicio que
 * ../../../banks/domains/banks/drive-fichas.service.js (proyecto GCP
 * "comprobantes-nuno", `config.google.serviceAccountKeyComprobantes`, scope
 * `drive.file` — la cuenta de servicio solo puede ver/tocar los archivos que
 * ella misma crea, nunca todo el Drive). Cliente de Drive PROPIO de este
 * archivo, deliberadamente NO compartido con drive-fichas.service.js: son dos
 * flujos independientes (foto de cliente de Lealtad vs. ficha bancaria),
 * aunque hoy compartan cuenta de servicio.
 *
 * El folder sale de Configuraciones Globales (sección 'lealtad', clave
 * FOTO_CLIENTE_FOLDER_ID), igual que FICHAS_IMAGEN_FOLDER_ID en Bancos — por
 * eso `folderId()` es ASYNC. Sin fallback hardcodeado: si la clave no existe
 * o está vacía, se rechaza explícito (ver comentario en `folderId()`).
 */

const { Readable } = require('stream');
const { google } = require('googleapis');
const config = require('../../../config/env');
const globalConfigService = require('../../../shared/services/global-config.service');

class DriveLoyaltyFotoError extends Error {}

let _driveClient = null;
function getDriveClient() {
  if (_driveClient) return _driveClient;

  // Misma cuenta de servicio que drive-fichas.service.js (GOOGLE_SERVICE_ACCOUNT_KEY2) —
  // deliberadamente distinta de config.google.serviceAccountKey (visor/drive.controller.js).
  const keyRaw = config.google.serviceAccountKeyComprobantes;
  if (!keyRaw) throw new DriveLoyaltyFotoError('GOOGLE_SERVICE_ACCOUNT_KEY2 no está configurada en .env');

  let credentials;
  try {
    credentials = JSON.parse(keyRaw);
  } catch {
    throw new DriveLoyaltyFotoError('GOOGLE_SERVICE_ACCOUNT_KEY2 no es un JSON válido');
  }

  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/drive.file'],
  });
  _driveClient = google.drive({ version: 'v3', auth });
  return _driveClient;
}

/**
 * Folder de Drive para fotos de cliente de Lealtad — configurable en runtime
 * desde Configuraciones Globales (sección 'lealtad'). ASYNC a propósito, mismo
 * criterio que drive-fichas.service.js#folderId(): sin fallback hardcodeado,
 * si la clave no existe (getValue rechaza) o el valor está vacío/solo
 * espacios, se rechaza con un mensaje claro y accionable.
 */
async function folderId() {
  let raw;
  try {
    raw = await globalConfigService.getValue('lealtad', 'FOTO_CLIENTE_FOLDER_ID');
  } catch {
    throw new DriveLoyaltyFotoError(
      'La carpeta de Drive para fotos de cliente no está configurada — creá FOTO_CLIENTE_FOLDER_ID en Configuraciones Globales, sección Lealtad.',
    );
  }
  const id = (raw ?? '').toString().trim();
  if (!id) {
    throw new DriveLoyaltyFotoError(
      'La carpeta de Drive para fotos de cliente no está configurada — creá FOTO_CLIENTE_FOLDER_ID en Configuraciones Globales, sección Lealtad.',
    );
  }
  return id;
}

/**
 * Sube la foto del cliente a la carpeta de Drive configurada.
 * `nombreArchivo` lo arma el caller (loyalty-card.service.js, con el teléfono
 * de la tarjeta para poder rastrearla en Drive) y NO es necesariamente el
 * nombre original del archivo subido.
 * @returns {Promise<{driveFileId: string, driveWebViewLink: string|null}>}
 */
async function subirFotoCliente(buffer, mimetype, nombreArchivo) {
  const drive = getDriveClient();
  try {
    const res = await drive.files.create({
      requestBody: {
        name: nombreArchivo || `foto-cliente-${Date.now()}`,
        parents: [await folderId()],
      },
      media: {
        mimeType: mimetype,
        body: Readable.from(buffer),
      },
      fields: 'id, webViewLink',
      supportsAllDrives: true,
    });
    return { driveFileId: res.data.id, driveWebViewLink: res.data.webViewLink ?? null };
  } catch (err) {
    if (err instanceof DriveLoyaltyFotoError) throw err;
    throw new DriveLoyaltyFotoError(`No se pudo subir la foto del cliente a Drive: ${err.message}`);
  }
}

/**
 * Descarga el binario de una foto de cliente ya subida — mismo patrón que
 * drive-fichas.service.js#descargarImagenFicha() (bug real corregido 2026-10-29: `fotoUrl`/
 * `driveWebViewLink` es la página visor de Drive, nunca embebible en un `<img>`; esto trae el
 * binario real para servirlo vía un proxy autenticado propio).
 * @returns {Promise<Buffer>}
 */
async function descargarFotoCliente(driveFileId) {
  const drive = getDriveClient();
  try {
    const res = await drive.files.get(
      { fileId: driveFileId, alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer' },
    );
    return Buffer.from(res.data);
  } catch (err) {
    throw new DriveLoyaltyFotoError(`No se pudo descargar la foto del cliente de Drive: ${err.message}`);
  }
}

module.exports = { subirFotoCliente, descargarFotoCliente, DriveLoyaltyFotoError };
