'use strict';

/**
 * drive-comprobantes.service.js — Sube y descarga los comprobantes de
 * transferencia de las Solicitudes de Cobro en la carpeta compartida de
 * Google Drive "Comprobantes Numo" (en vez de guardarlos como Buffer en
 * Mongo, que no escala con muchos comprobantes por solicitud).
 *
 * Cliente PROPIO, separado del que ya existe en visor/controllers/drive.controller.js
 * (ese es de solo lectura, scope `drive.readonly`, usado para importar CFDIs) —
 * este necesita escribir, así que usa `drive.file` (la cuenta de servicio solo
 * puede ver/tocar los archivos que ella misma crea, nunca todo el Drive).
 *
 * IMPORTANTE (operativo, no de código): una cuenta de servicio de Google no
 * tiene cuota de almacenamiento propia — si la carpeta configurada no es una
 * Unidad Compartida (o no hay domain-wide delegation), subir archivos falla
 * con "storageQuotaExceeded" sin importar que la carpeta esté "compartida" con
 * la cuenta de servicio. Ver GOOGLE_DRIVE_COMPROBANTES_FOLDER_ID en .env.
 */

const { Readable } = require('stream');
const { google } = require('googleapis');
const config = require('../../../config/env');

class DriveComprobantesError extends Error {}

let _driveClient = null;
function getDriveClient() {
  if (_driveClient) return _driveClient;

  const keyRaw = config.google.serviceAccountKey;
  if (!keyRaw) throw new DriveComprobantesError('GOOGLE_SERVICE_ACCOUNT_KEY no está configurada en .env');

  let credentials;
  try {
    credentials = JSON.parse(keyRaw);
  } catch {
    throw new DriveComprobantesError('GOOGLE_SERVICE_ACCOUNT_KEY no es un JSON válido');
  }

  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/drive.file'],
  });
  _driveClient = google.drive({ version: 'v3', auth });
  return _driveClient;
}

function folderId() {
  const id = config.google.driveComprobantesFolderId;
  if (!id) throw new DriveComprobantesError('GOOGLE_DRIVE_COMPROBANTES_FOLDER_ID no está configurada en .env');
  return id;
}

/**
 * Sube un comprobante a la carpeta compartida de Drive.
 * @returns {Promise<{driveFileId: string, driveWebViewLink: string|null}>}
 */
async function subirComprobante(buffer, mimetype, originalName) {
  const drive = getDriveClient();
  try {
    const res = await drive.files.create({
      requestBody: {
        name: originalName || `comprobante-${Date.now()}`,
        parents: [folderId()],
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
    throw new DriveComprobantesError(`No se pudo subir el comprobante a Drive: ${err.message}`);
  }
}

/**
 * Descarga el binario de un comprobante ya subido — mismo patrón que ya usa
 * visor/controllers/drive.controller.js para importar CFDIs.
 * @returns {Promise<Buffer>}
 */
async function descargarComprobante(driveFileId) {
  const drive = getDriveClient();
  try {
    const res = await drive.files.get(
      { fileId: driveFileId, alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer' },
    );
    return Buffer.from(res.data);
  } catch (err) {
    throw new DriveComprobantesError(`No se pudo descargar el comprobante de Drive: ${err.message}`);
  }
}

module.exports = { subirComprobante, descargarComprobante, DriveComprobantesError };
