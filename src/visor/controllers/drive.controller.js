const { validationResult } = require('express-validator');
const { google } = require('googleapis');
const AdmZip = require('adm-zip');
const CFDI = require('../models/CFDI');
const { parseCFDI } = require('../services/cfdiParser');
const { verifyCFDIWithSAT } = require('../services/satVerification');
const { asyncHandler } = require('../middleware/errorHandler');
const { logger } = require('../utils/logger');

const SAT_STATUS_VALIDOS = new Set(['Vigente', 'Cancelado', 'No Encontrado']);

// Consulta el SAT en background y actualiza satStatus sin bloquear la respuesta.
const verificarSATBackground = (cfdiData) => {
  const rfcEmisor = cfdiData.emisor?.rfc || '';
  if (!/^[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}$/i.test(rfcEmisor)) return;

  verifyCFDIWithSAT(
    cfdiData.uuid,
    rfcEmisor,
    cfdiData.receptor?.rfc || '',
    cfdiData.total,
    cfdiData.timbreFiscalDigital?.selloCFD || cfdiData.sello || '',
    cfdiData.version || '4.0',
  ).then(satResponse => {
    const estado = SAT_STATUS_VALIDOS.has(satResponse.state) ? satResponse.state : 'Error';
    return CFDI.updateMany(
      { uuid: cfdiData.uuid.toUpperCase() },
      { satStatus: estado, satLastCheck: new Date() },
    );
  }).catch(() => { /* best-effort, no bloquea */ });
};

/**
 * Crea un cliente de Google Drive autenticado con service account.
 * Las credenciales se leen del entorno en cada llamada.
 */
const getDriveClient = () => {
  const keyRaw = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyRaw) throw new Error('GOOGLE_SERVICE_ACCOUNT_KEY no configurada en .env');
  let credentials;
  try {
    credentials = JSON.parse(keyRaw);
  } catch {
    throw new Error('GOOGLE_SERVICE_ACCOUNT_KEY no es un JSON válido');
  }
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/drive.readonly'],
  });
  return google.drive({ version: 'v3', auth });
};

/**
 * GET /api/drive/folders
 */
const getFolders = asyncHandler(async (req, res) => {
  const drive = getDriveClient();
  const rootFolder = process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;

  const q = rootFolder
    ? `'${rootFolder}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`
    : `mimeType='application/vnd.google-apps.folder' and trashed=false`;

  const response = await drive.files.list({ q, fields: 'files(id, name)', pageSize: 100, orderBy: 'name', supportsAllDrives: true, includeItemsFromAllDrives: true });
  res.json({ folders: response.data.files || [] });
});

/**
 * POST /api/drive/import
 */
const importFromDrive = asyncHandler(async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) return res.status(400).json({ errors: errors.array() });

  const { source, ejercicio, periodo } = req.body;
  const ejercicioNum = parseInt(ejercicio);
  const periodoNum   = parseInt(periodo);

  // Para ERP, el backend siempre impone la carpeta configurada — el cliente no decide.
  const erpFolderId = process.env.GOOGLE_DRIVE_ERP_FOLDER_ID;
  let folderId = req.body.folderId;
  if (source === 'ERP') {
    if (!erpFolderId) return res.status(500).json({ error: 'GOOGLE_DRIVE_ERP_FOLDER_ID no configurado en el servidor' });
    folderId = erpFolderId;
  }

  const drive = getDriveClient();

  logger.info(`[Drive] Listando carpeta: ${folderId}`);

  // Primero listar TODO sin filtro de nombre para diagnóstico
  const listAll = await drive.files.list({
    q: `'${folderId}' in parents and trashed=false`,
    fields: 'files(id, name, mimeType)',
    pageSize: 100,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });
  logger.info(`[Drive] Total archivos en carpeta (sin filtro): ${listAll.data.files?.length ?? 0}`);
  (listAll.data.files || []).slice(0, 5).forEach(f => logger.info(`[Drive]  - ${f.name} | ${f.mimeType}`));

  const listRes = await drive.files.list({
    q: `'${folderId}' in parents and (name contains '.xml' or name contains '.zip') and trashed=false`,
    fields: 'files(id, name, size)',
    pageSize: 1000,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  const files = listRes.data.files || [];
  logger.info(`[Drive] Archivos XML/ZIP encontrados: ${files.length}`);
  if (files.length === 0) {
    return res.json({ message: 'No se encontraron archivos XML o ZIP en la carpeta', procesados: 0, nuevos: 0, actualizados: 0, omitidos: 0, errores: [] });
  }

  logger.info(`[Drive] ${files.length} archivos encontrados en carpeta ${folderId}`);

  let nuevos = 0, actualizados = 0, omitidos = 0;
  const errores = [];

  const perteneceAlPeriodo = (fecha) => {
    if (!fecha) return false;
    const d = new Date(fecha);
    return d.getFullYear() === ejercicioNum && (d.getMonth() + 1) === periodoNum;
  };

  const procesarXml = async (buffer, filename) => {
    const cfdiData = await parseCFDI(buffer.toString('utf8'));
    const d = cfdiData.fecha ? new Date(cfdiData.fecha) : null;
    logger.info(`[Drive] ${filename} | fecha: ${cfdiData.fecha} | año: ${d?.getFullYear()} | mes: ${d ? d.getMonth()+1 : null} | filtro: ${ejercicioNum}/${periodoNum}`);
    if (!perteneceAlPeriodo(cfdiData.fecha)) {
      omitidos++;
      return;
    }
    // Marcar Vigente como provisional hasta que la verificación SAT responda
    if (['SAT', 'MANUAL'].includes(source) && !cfdiData.satStatus) {
      cfdiData.satStatus = 'Vigente';
    }
    const prev = await CFDI.findOneAndUpdate(
      { uuid: cfdiData.uuid, source },
      { ...cfdiData, source, ejercicio: ejercicioNum, periodo: periodoNum, uploadedBy: req.user._id },
      { upsert: true, new: false, setDefaultsOnInsert: true },
    );
    prev === null ? nuevos++ : actualizados++;
    // Consultar estado real al SAT en background (actualiza satStatus cuando responde)
    verificarSATBackground(cfdiData);
  };

  for (const file of files) {
    try {
      const dlRes = await drive.files.get({ fileId: file.id, alt: 'media', supportsAllDrives: true }, { responseType: 'arraybuffer' });
      const buffer = Buffer.from(dlRes.data);

      if (file.name.toLowerCase().endsWith('.zip')) {
        const zip = new AdmZip(buffer);
        const xmlEntries = zip.getEntries().filter(e => e.entryName.toLowerCase().endsWith('.xml') && !e.isDirectory);
        for (const entry of xmlEntries) {
          try {
            await procesarXml(entry.getData(), entry.entryName);
          } catch (err) {
            errores.push({ filename: entry.entryName, error: err.message });
          }
        }
      } else {
        try {
          await procesarXml(buffer, file.name);
        } catch (err) {
          errores.push({ filename: file.name, error: err.message });
        }
      }
    } catch (err) {
      logger.error(`[Drive] Error en ${file.name}: ${err.message}`);
      errores.push({ filename: file.name, error: err.message });
    }
  }

  const procesados = nuevos + actualizados;
  logger.info(`[Drive] Completado: ${procesados} procesados (${nuevos} nuevos, ${actualizados} actualizados), ${omitidos} omitidos por periodo, ${errores.length} errores`);

  res.json({
    message: `${procesados} CFDIs procesados (${nuevos} nuevos, ${actualizados} actualizados), ${omitidos} omitidos por no corresponder al periodo, ${errores.length} con error`,
    procesados, nuevos, actualizados, omitidos, errores,
  });
});

module.exports = { getFolders, importFromDrive };
