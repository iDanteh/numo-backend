'use strict';

// drive-fichas.service.test.js — sube/elimina la imagen de respaldo de una ficha bancaria en
// Drive. Mismo patrón de mocks que se usaría para drive-comprobantes.service.js: se mockea
// `googleapis` completo (sin pegarle a la API real) y `global-config.service` (el folder sale
// de Configuraciones Globales, no de .env, a diferencia del flujo de comprobantes).
jest.mock('googleapis', () => {
  const filesCreate = jest.fn();
  const filesDelete = jest.fn();
  const filesGet    = jest.fn();
  const driveClient = { files: { create: filesCreate, delete: filesDelete, get: filesGet } };
  return {
    google: {
      auth: { GoogleAuth: jest.fn().mockImplementation(() => ({})) },
      drive: jest.fn(() => driveClient),
    },
    __filesCreate: filesCreate,
    __filesDelete: filesDelete,
    __filesGet: filesGet,
  };
});

jest.mock('../../../config/env', () => ({
  google: {
    serviceAccountKeyComprobantes: JSON.stringify({ client_email: 'svc@test', private_key: 'fake-key' }),
  },
}));

jest.mock('../../../shared/services/global-config.service', () => ({
  getValue: jest.fn(),
}));

const { __filesCreate: filesCreate, __filesDelete: filesDelete, __filesGet: filesGet } = require('googleapis');
const globalConfigService = require('../../../shared/services/global-config.service');
const { subirImagenFicha, eliminarImagenFicha, descargarImagenFicha, DriveFichasError } = require('./drive-fichas.service');

beforeEach(() => {
  jest.clearAllMocks();
});

describe('subirImagenFicha', () => {
  test('sube bien con folder configurado en Configuraciones Globales', async () => {
    globalConfigService.getValue.mockResolvedValue('FOLDER-ABC');
    filesCreate.mockResolvedValue({ data: { id: 'file-1', webViewLink: 'https://drive/file-1' } });

    const result = await subirImagenFicha(Buffer.from('img'), 'image/png', 'ficha.png');

    expect(result).toEqual({ driveFileId: 'file-1', driveWebViewLink: 'https://drive/file-1' });
    expect(globalConfigService.getValue).toHaveBeenCalledWith('bancos', 'FICHAS_IMAGEN_FOLDER_ID');
    const callArgs = filesCreate.mock.calls[0][0];
    expect(callArgs.requestBody.parents).toEqual(['FOLDER-ABC']);
    expect(callArgs.requestBody.name).toBe('ficha.png');
    expect(callArgs.media.mimeType).toBe('image/png');
  });

  test('rechaza con mensaje claro si getValue rechaza (clave no existe en Configuraciones Globales)', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error("No existe la configuración 'bancos.FICHAS_IMAGEN_FOLDER_ID'"));

    await expect(subirImagenFicha(Buffer.from('img'), 'image/png', 'ficha.png'))
      .rejects.toThrow(/FICHAS_IMAGEN_FOLDER_ID/);
    await expect(subirImagenFicha(Buffer.from('img'), 'image/png', 'ficha.png'))
      .rejects.toBeInstanceOf(DriveFichasError);
    expect(filesCreate).not.toHaveBeenCalled();
  });

  test('rechaza si el valor configurado está vacío o solo espacios', async () => {
    globalConfigService.getValue.mockResolvedValue('   ');

    await expect(subirImagenFicha(Buffer.from('img'), 'image/png', 'ficha.png'))
      .rejects.toThrow(/FICHAS_IMAGEN_FOLDER_ID/);
    expect(filesCreate).not.toHaveBeenCalled();
  });
});

describe('eliminarImagenFicha', () => {
  test('llama drive.files.delete con el fileId correcto', async () => {
    filesDelete.mockResolvedValue({});

    await eliminarImagenFicha('file-123');

    expect(filesDelete).toHaveBeenCalledWith({ fileId: 'file-123', supportsAllDrives: true });
  });
});

describe('descargarImagenFicha', () => {
  test('descarga bien y devuelve un Buffer con el binario', async () => {
    filesGet.mockResolvedValue({ data: Buffer.from('contenido-binario') });

    const result = await descargarImagenFicha('file-123');

    expect(filesGet).toHaveBeenCalledWith(
      { fileId: 'file-123', alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer' },
    );
    expect(Buffer.isBuffer(result)).toBe(true);
    expect(result.toString()).toBe('contenido-binario');
  });

  test('propaga el error de Drive como DriveFichasError', async () => {
    filesGet.mockRejectedValue(new Error('archivo no encontrado'));

    await expect(descargarImagenFicha('file-123')).rejects.toBeInstanceOf(DriveFichasError);
    await expect(descargarImagenFicha('file-123')).rejects.toThrow(/No se pudo descargar la imagen de la ficha/);
  });
});
