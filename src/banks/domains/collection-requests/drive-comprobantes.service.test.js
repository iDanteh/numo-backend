'use strict';

// drive-comprobantes.service.test.js — sube/descarga el comprobante de una Solicitud de
// Cobro en Drive. CORRECCIÓN 2026-09-04 (pedido explícito del usuario): folderId() migrado
// de .env a Configuraciones Globales, CON fallback a .env — a diferencia de
// drive-fichas.service.js (feature nueva, sin fallback), este flujo ya está en producción
// activa con Kore llamando de forma síncrona, así que un corte acá no es aceptable mientras
// el usuario todavía no declaró la clave nueva. Mismo patrón de mocks que
// drive-fichas.service.test.js: se mockea `googleapis` completo y `global-config.service`.
jest.mock('googleapis', () => {
  const filesCreate = jest.fn();
  const filesGet    = jest.fn();
  const driveClient = { files: { create: filesCreate, get: filesGet } };
  return {
    google: {
      auth: { GoogleAuth: jest.fn().mockImplementation(() => ({})) },
      drive: jest.fn(() => driveClient),
    },
    __filesCreate: filesCreate,
    __filesGet:    filesGet,
  };
});

jest.mock('../../../config/env', () => ({
  google: {
    serviceAccountKeyComprobantes: JSON.stringify({ client_email: 'svc@test', private_key: 'fake-key' }),
    driveComprobantesFolderId: null, // cada test setea lo que necesita
  },
}));

jest.mock('../../../shared/services/global-config.service', () => ({
  getValue: jest.fn(),
}));

const { __filesCreate: filesCreate, __filesGet: filesGet } = require('googleapis');
const config              = require('../../../config/env');
const globalConfigService = require('../../../shared/services/global-config.service');
const { subirComprobante, descargarComprobante, DriveComprobantesError } = require('./drive-comprobantes.service');

beforeEach(() => {
  jest.clearAllMocks();
  config.google.driveComprobantesFolderId = null;
});

describe('subirComprobante — folderId() con fallback Configuraciones Globales → .env', () => {
  test('usa Configuraciones Globales cuando la clave ya está declarada ahí', async () => {
    globalConfigService.getValue.mockResolvedValue('FOLDER-GLOBAL-CONFIG');
    filesCreate.mockResolvedValue({ data: { id: 'file-1', webViewLink: 'https://drive/file-1' } });

    const result = await subirComprobante(Buffer.from('img'), 'image/png', 'comprobante.png');

    expect(result).toEqual({ driveFileId: 'file-1', driveWebViewLink: 'https://drive/file-1' });
    expect(globalConfigService.getValue).toHaveBeenCalledWith('solicitudes', 'COMPROBANTES_IMAGEN_FOLDER_ID');
    expect(filesCreate.mock.calls[0][0].requestBody.parents).toEqual(['FOLDER-GLOBAL-CONFIG']);
  });

  test('Configuraciones Globales gana sobre .env cuando AMBOS están declarados', async () => {
    globalConfigService.getValue.mockResolvedValue('FOLDER-GLOBAL-CONFIG');
    config.google.driveComprobantesFolderId = 'FOLDER-ENV';
    filesCreate.mockResolvedValue({ data: { id: 'file-1', webViewLink: null } });

    await subirComprobante(Buffer.from('img'), 'image/png', 'comprobante.png');

    expect(filesCreate.mock.calls[0][0].requestBody.parents).toEqual(['FOLDER-GLOBAL-CONFIG']);
  });

  test('cae a .env si la clave todavía no existe en Configuraciones Globales (getValue rechaza)', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error("No existe la configuración 'solicitudes.COMPROBANTES_IMAGEN_FOLDER_ID'"));
    config.google.driveComprobantesFolderId = 'FOLDER-ENV';
    filesCreate.mockResolvedValue({ data: { id: 'file-1', webViewLink: null } });

    const result = await subirComprobante(Buffer.from('img'), 'image/png', 'comprobante.png');

    expect(result.driveFileId).toBe('file-1');
    expect(filesCreate.mock.calls[0][0].requestBody.parents).toEqual(['FOLDER-ENV']);
  });

  test('cae a .env si Configuraciones Globales devuelve vacío/solo espacios', async () => {
    globalConfigService.getValue.mockResolvedValue('   ');
    config.google.driveComprobantesFolderId = 'FOLDER-ENV';
    filesCreate.mockResolvedValue({ data: { id: 'file-1', webViewLink: null } });

    await subirComprobante(Buffer.from('img'), 'image/png', 'comprobante.png');

    expect(filesCreate.mock.calls[0][0].requestBody.parents).toEqual(['FOLDER-ENV']);
  });

  test('sin Configuraciones Globales NI .env: rechaza con mensaje claro, no pega a Drive', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error('no existe'));
    config.google.driveComprobantesFolderId = null;

    await expect(subirComprobante(Buffer.from('img'), 'image/png', 'x.png'))
      .rejects.toThrow(/COMPROBANTES_IMAGEN_FOLDER_ID/);
    await expect(subirComprobante(Buffer.from('img'), 'image/png', 'x.png'))
      .rejects.toBeInstanceOf(DriveComprobantesError);
    expect(filesCreate).not.toHaveBeenCalled();
  });

  test('sube bien y arma requestBody con el nombre/mimetype recibidos', async () => {
    globalConfigService.getValue.mockResolvedValue('FOLDER-GLOBAL-CONFIG');
    filesCreate.mockResolvedValue({ data: { id: 'file-1', webViewLink: 'https://drive/file-1' } });

    await subirComprobante(Buffer.from('img'), 'image/png', 'comprobante.png');

    const callArgs = filesCreate.mock.calls[0][0];
    expect(callArgs.requestBody.name).toBe('comprobante.png');
    expect(callArgs.media.mimeType).toBe('image/png');
    expect(callArgs.supportsAllDrives).toBe(true);
  });
});

describe('descargarComprobante', () => {
  test('descarga bien y devuelve un Buffer con el binario', async () => {
    filesGet.mockResolvedValue({ data: Buffer.from('contenido-binario') });

    const result = await descargarComprobante('file-123');

    expect(filesGet).toHaveBeenCalledWith(
      { fileId: 'file-123', alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer' },
    );
    expect(Buffer.isBuffer(result)).toBe(true);
    expect(result.toString()).toBe('contenido-binario');
  });

  test('propaga el error de Drive como DriveComprobantesError', async () => {
    filesGet.mockRejectedValue(new Error('archivo no encontrado'));

    await expect(descargarComprobante('file-123')).rejects.toBeInstanceOf(DriveComprobantesError);
    await expect(descargarComprobante('file-123')).rejects.toThrow(/No se pudo descargar el comprobante/);
  });
});
