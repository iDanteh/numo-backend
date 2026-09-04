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
 * la cuenta de servicio.
 *
 * CORRECCIÓN 2026-09-04 (pedido explícito del usuario): folderId() migrado de
 * .env a Configuraciones Globales (sección 'solicitudes', clave
 * COMPROBANTES_IMAGEN_FOLDER_ID) — mismo patrón que drive-fichas.service.js.
 * A diferencia de fichas (feature nueva, sin uso previo), ESTE flujo ya está
 * en producción activa: Kore llama a POST /collection-requests de forma
 * SÍNCRONA cada vez que una tienda crea una solicitud con comprobante — un
 * corte acá le devuelve un error a Kore en el momento. Por eso, a propósito,
 * SÍ hay fallback a GOOGLE_DRIVE_COMPROBANTES_FOLDER_ID (.env) si la clave
 * todavía no existe en Configuraciones Globales, con warning en logs para no
 * perder de vista que sigue pendiente declararla ahí. Cuando el usuario la
 * declare, Configuraciones Globales pasa a ser la fuente real sin más cambios
 * de código — el fallback simplemente deja de usarse solo.
 */

const { Readable } = require('stream');
const { google } = require('googleapis');
const config = require('../../../config/env');
const globalConfigService = require('../../../shared/services/global-config.service');
const { logger } = require('../../../shared/utils/logger');

class DriveComprobantesError extends Error {}

let _driveClient = null;
function getDriveClient() {
  if (_driveClient) return _driveClient;

  // Cuenta de servicio dedicada a comprobantes (proyecto GCP "comprobantes-nuno") —
  // deliberadamente distinta de config.google.serviceAccountKey, que es la que usa
  // visor/drive.controller.js en producción para CFDIs. Nunca reusar esa aquí.
  const keyRaw = config.google.serviceAccountKeyComprobantes;
  if (!keyRaw) throw new DriveComprobantesError('GOOGLE_SERVICE_ACCOUNT_KEY2 no está configurada en .env');

  let credentials;
  try {
    credentials = JSON.parse(keyRaw);
  } catch {
    throw new DriveComprobantesError('GOOGLE_SERVICE_ACCOUNT_KEY2 no es un JSON válido');
  }

  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/drive.file'],
  });
  _driveClient = google.drive({ version: 'v3', auth });
  return _driveClient;
}

async function folderId() {
  try {
    const raw = await globalConfigService.getValue('solicitudes', 'COMPROBANTES_IMAGEN_FOLDER_ID');
    const id = (raw ?? '').toString().trim();
    if (id) return id;
  } catch {
    // Todavía no existe la fila en Configuraciones Globales — cae al fallback de abajo.
  }

  const envId = config.google.driveComprobantesFolderId;
  if (envId) {
    logger.warn(
      '[drive-comprobantes] usando GOOGLE_DRIVE_COMPROBANTES_FOLDER_ID (.env) como respaldo — ' +
      'declará COMPROBANTES_IMAGEN_FOLDER_ID en Configuraciones Globales (sección Solicitudes) para dejar de depender del .env.',
    );
    return envId;
  }

  throw new DriveComprobantesError(
    'La carpeta de Drive para comprobantes no está configurada — creá COMPROBANTES_IMAGEN_FOLDER_ID en ' +
    'Configuraciones Globales (sección Solicitudes) o GOOGLE_DRIVE_COMPROBANTES_FOLDER_ID en .env.',
  );
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
    if (err instanceof DriveComprobantesError) throw err;
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
