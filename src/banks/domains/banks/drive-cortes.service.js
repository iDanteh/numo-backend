'use strict';

/**
 * drive-cortes.service.js — Sube y descarga los archivos Excel del historial de descargas de
 * Cortes en Google Drive (2026-11-06, pedido explícito del usuario: poder volver a bajar
 * cualquier reporte de un punto pasado de la historia, no solo ver el delta de backlog en
 * pantalla — ver CorteDescarga.model.js/listarDescargasCorte).
 *
 * Reusa la MISMA cuenta de servicio que drive-comprobantes.service.js (scope `drive.file`, ya
 * configurada y en producción) — solo cambia la carpeta destino, declarada en Configuraciones
 * Globales (sección 'bancos', clave CORTE_DESCARGAS_FOLDER_ID). Sin fallback a `.env` a
 * propósito (a diferencia de comprobantes, que es síncrono con una llamada de Kore): esto es un
 * botón de staff, no una integración externa — si la carpeta no está configurada, la subida
 * falla y `buildReporteCortes()` lo absorbe sin romper la descarga que el usuario está pidiendo
 * en el momento (ver su propio comentario).
 */

const { Readable } = require('stream');
const { google } = require('googleapis');
const config = require('../../../config/env');
const globalConfigService = require('../../../shared/services/global-config.service');

class DriveCortesError extends Error {}

let _driveClient = null;
function getDriveClient() {
  if (_driveClient) return _driveClient;

  // Misma cuenta de servicio "comprobantes-nuno" que ya usa drive-comprobantes.service.js —
  // nunca se creó una cuenta nueva solo para esto, el scope drive.file ya alcanza.
  const keyRaw = config.google.serviceAccountKeyComprobantes;
  if (!keyRaw) throw new DriveCortesError('GOOGLE_SERVICE_ACCOUNT_KEY2 no está configurada en .env');

  let credentials;
  try {
    credentials = JSON.parse(keyRaw);
  } catch {
    throw new DriveCortesError('GOOGLE_SERVICE_ACCOUNT_KEY2 no es un JSON válido');
  }

  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/drive.file'],
  });
  _driveClient = google.drive({ version: 'v3', auth });
  return _driveClient;
}

async function folderId() {
  const raw = await globalConfigService.getValue('bancos', 'CORTE_DESCARGAS_FOLDER_ID').catch(() => null);
  const id = (raw ?? '').toString().trim();
  if (!id) {
    throw new DriveCortesError(
      'La carpeta de Drive para el historial de descargas de Cortes no está configurada — creá '
      + 'CORTE_DESCARGAS_FOLDER_ID en Configuraciones Globales (sección Bancos).',
    );
  }
  return id;
}

/**
 * Sube el Excel ya generado de un corte a la carpeta compartida de Drive.
 * @returns {Promise<string>} driveFileId
 */
async function subirReporteCorte(buffer, originalName) {
  const drive = getDriveClient();
  try {
    const res = await drive.files.create({
      requestBody: { name: originalName, parents: [await folderId()] },
      media: {
        mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        body: Readable.from(buffer),
      },
      fields: 'id',
      supportsAllDrives: true,
    });
    return res.data.id;
  } catch (err) {
    if (err instanceof DriveCortesError) throw err;
    throw new DriveCortesError(`No se pudo subir el reporte de Cortes a Drive: ${err.message}`);
  }
}

/**
 * Descarga el binario de un reporte de Cortes ya subido — mismo patrón que
 * drive-comprobantes.service.js#descargarComprobante.
 * @returns {Promise<Buffer>}
 */
async function descargarReporteCorte(driveFileId) {
  const drive = getDriveClient();
  try {
    const res = await drive.files.get(
      { fileId: driveFileId, alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer' },
    );
    return Buffer.from(res.data);
  } catch (err) {
    throw new DriveCortesError(`No se pudo descargar el reporte de Cortes de Drive: ${err.message}`);
  }
}

module.exports = { subirReporteCorte, descargarReporteCorte, DriveCortesError };
