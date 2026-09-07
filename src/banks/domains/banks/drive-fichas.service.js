'use strict';

/**
 * drive-fichas.service.js — Sube la foto/documento de respaldo de una `ficha`
 * bancaria (BankMovement.ficha, folio del comprobante físico cargado a mano por
 * contabilidad) a Google Drive, en vez de guardarla como Buffer en Mongo.
 *
 * Mismo patrón y MISMA cuenta de servicio que
 * ../collection-requests/drive-comprobantes.service.js (proyecto GCP
 * "comprobantes-nuno", `config.google.serviceAccountKeyComprobantes`, scope
 * `drive.file` — la cuenta de servicio solo puede ver/tocar los archivos que
 * ella misma crea, nunca todo el Drive). Cliente de Drive PROPIO de este
 * archivo, deliberadamente NO compartido con drive-comprobantes.service.js:
 * son dos flujos independientes (ficha bancaria vs. comprobante de Solicitud
 * de Cobro), aunque hoy compartan cuenta de servicio.
 *
 * Diferencia clave con drive-comprobantes.service.js: ahí el folder sale de
 * .env (síncrono). Acá el folder sale de Configuraciones Globales
 * (sección 'bancos', clave FICHAS_IMAGEN_FOLDER_ID) — por eso `folderId()` es
 * ASYNC. Sin fallback hardcodeado: si la clave no existe o está vacía, se
 * rechaza explícito (ver comentario en `folderId()`).
 */

const { Readable } = require('stream');
const { google } = require('googleapis');
const config = require('../../../config/env');
const globalConfigService = require('../../../shared/services/global-config.service');

class DriveFichasError extends Error {}

let _driveClient = null;
function getDriveClient() {
  if (_driveClient) return _driveClient;

  // Misma cuenta de servicio que drive-comprobantes.service.js (GOOGLE_SERVICE_ACCOUNT_KEY2)
  // — deliberadamente distinta de config.google.serviceAccountKey (visor/drive.controller.js).
  const keyRaw = config.google.serviceAccountKeyComprobantes;
  if (!keyRaw) throw new DriveFichasError('GOOGLE_SERVICE_ACCOUNT_KEY2 no está configurada en .env');

  let credentials;
  try {
    credentials = JSON.parse(keyRaw);
  } catch {
    throw new DriveFichasError('GOOGLE_SERVICE_ACCOUNT_KEY2 no es un JSON válido');
  }

  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/drive.file'],
  });
  _driveClient = google.drive({ version: 'v3', auth });
  return _driveClient;
}

/**
 * Folder de Drive para imágenes de ficha — configurable en runtime desde
 * Configuraciones Globales (sección 'bancos'). ASYNC a propósito: a diferencia
 * de drive-comprobantes.service.js#folderId() (que lee .env de forma síncrona),
 * este valor puede cambiar sin redeploy. Sin fallback hardcodeado: si la clave
 * no existe (getValue rechaza) o el valor está vacío/solo espacios, se rechaza
 * con un mensaje claro y accionable.
 */
async function folderId() {
  let raw;
  try {
    raw = await globalConfigService.getValue('bancos', 'FICHAS_IMAGEN_FOLDER_ID');
  } catch {
    throw new DriveFichasError(
      'La carpeta de Drive para imágenes de ficha no está configurada — creá FICHAS_IMAGEN_FOLDER_ID en Configuraciones Globales, sección Bancos.',
    );
  }
  const id = (raw ?? '').toString().trim();
  if (!id) {
    throw new DriveFichasError(
      'La carpeta de Drive para imágenes de ficha no está configurada — creá FICHAS_IMAGEN_FOLDER_ID en Configuraciones Globales, sección Bancos.',
    );
  }
  return id;
}

/**
 * Sube la imagen/PDF de respaldo de una ficha a la carpeta de Drive configurada.
 * `nombreArchivo` es el nombre final con el que queda visible en Drive — lo arma
 * el caller (bank.service.js#_nombreArchivoFichaDrive: "folio - banco.ext",
 * pedido explícito del usuario para poder rastrear el movimiento buscando esos
 * dos datos en Drive) y NO es necesariamente el nombre original del archivo subido.
 * @returns {Promise<{driveFileId: string, driveWebViewLink: string|null}>}
 */
async function subirImagenFicha(buffer, mimetype, nombreArchivo) {
  const drive = getDriveClient();
  try {
    const res = await drive.files.create({
      requestBody: {
        name: nombreArchivo || `ficha-${Date.now()}`,
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
    if (err instanceof DriveFichasError) throw err;
    throw new DriveFichasError(`No se pudo subir la imagen de la ficha a Drive: ${err.message}`);
  }
}

/**
 * Elimina una imagen de ficha ya subida. Se usa en modo best-effort por el
 * llamador (bank.service.js#deleteFicha) — no captura sus propios errores acá,
 * el caller decide si bloquea o no según el contexto.
 */
async function eliminarImagenFicha(driveFileId) {
  const drive = getDriveClient();
  return drive.files.delete({ fileId: driveFileId, supportsAllDrives: true });
}

/**
 * Descarga el binario de una imagen de ficha ya subida — mismo patrón que
 * drive-comprobantes.service.js#descargarComprobante().
 * @returns {Promise<Buffer>}
 */
async function descargarImagenFicha(driveFileId) {
  const drive = getDriveClient();
  try {
    const res = await drive.files.get(
      { fileId: driveFileId, alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer' },
    );
    return Buffer.from(res.data);
  } catch (err) {
    throw new DriveFichasError(`No se pudo descargar la imagen de la ficha de Drive: ${err.message}`);
  }
}

module.exports = { subirImagenFicha, eliminarImagenFicha, descargarImagenFicha, DriveFichasError };
