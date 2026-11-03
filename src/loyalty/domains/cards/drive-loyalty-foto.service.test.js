'use strict';

// drive-loyalty-foto.service.test.js — sube la foto del cliente de una tarjeta de Lealtad a
// Drive. Mismo patrón de mocks que drive-fichas.service.test.js: se mockea `googleapis`
// completo (sin pegarle a la API real) y `global-config.service` (el folder sale de
// Configuraciones Globales, sección 'lealtad').
jest.mock('googleapis', () => {
  const filesCreate = jest.fn();
  const filesGet = jest.fn();
  const driveClient = { files: { create: filesCreate, get: filesGet } };
  return {
    google: {
      auth: { GoogleAuth: jest.fn().mockImplementation(() => ({})) },
      drive: jest.fn(() => driveClient),
    },
    __filesCreate: filesCreate,
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

const { __filesCreate: filesCreate, __filesGet: filesGet } = require('googleapis');
const globalConfigService = require('../../../shared/services/global-config.service');
const { subirFotoCliente, descargarFotoCliente, DriveLoyaltyFotoError } = require('./drive-loyalty-foto.service');

beforeEach(() => {
  jest.clearAllMocks();
});

describe('subirFotoCliente', () => {
  test('sube bien con folder configurado en Configuraciones Globales (sección lealtad)', async () => {
    globalConfigService.getValue.mockResolvedValue('FOLDER-LEALTAD-ABC');
    filesCreate.mockResolvedValue({ data: { id: 'file-1', webViewLink: 'https://drive/file-1' } });

    const result = await subirFotoCliente(Buffer.from('img'), 'image/png', 'foto-cliente.png');

    expect(result).toEqual({ driveFileId: 'file-1', driveWebViewLink: 'https://drive/file-1' });
    expect(globalConfigService.getValue).toHaveBeenCalledWith('lealtad', 'FOTO_CLIENTE_FOLDER_ID');
    const callArgs = filesCreate.mock.calls[0][0];
    expect(callArgs.requestBody.parents).toEqual(['FOLDER-LEALTAD-ABC']);
    expect(callArgs.requestBody.name).toBe('foto-cliente.png');
    expect(callArgs.media.mimeType).toBe('image/png');
  });

  test('rechaza con mensaje claro si getValue rechaza (clave no existe en Configuraciones Globales)', async () => {
    globalConfigService.getValue.mockRejectedValue(new Error("No existe la configuración 'lealtad.FOTO_CLIENTE_FOLDER_ID'"));

    await expect(subirFotoCliente(Buffer.from('img'), 'image/png', 'foto-cliente.png'))
      .rejects.toThrow(/FOTO_CLIENTE_FOLDER_ID/);
    await expect(subirFotoCliente(Buffer.from('img'), 'image/png', 'foto-cliente.png'))
      .rejects.toBeInstanceOf(DriveLoyaltyFotoError);
    expect(filesCreate).not.toHaveBeenCalled();
  });

  test('rechaza si el valor configurado está vacío o solo espacios', async () => {
    globalConfigService.getValue.mockResolvedValue('   ');

    await expect(subirFotoCliente(Buffer.from('img'), 'image/png', 'foto-cliente.png'))
      .rejects.toThrow(/FOTO_CLIENTE_FOLDER_ID/);
    expect(filesCreate).not.toHaveBeenCalled();
  });

  test('propaga un error de Drive como DriveLoyaltyFotoError', async () => {
    globalConfigService.getValue.mockResolvedValue('FOLDER-LEALTAD-ABC');
    filesCreate.mockRejectedValue(new Error('cuota excedida'));

    await expect(subirFotoCliente(Buffer.from('img'), 'image/png', 'foto-cliente.png'))
      .rejects.toBeInstanceOf(DriveLoyaltyFotoError);
    await expect(subirFotoCliente(Buffer.from('img'), 'image/png', 'foto-cliente.png'))
      .rejects.toThrow(/No se pudo subir la foto del cliente/);
  });
});

// descargarFotoCliente (2026-10-29) — bug real corregido: el `webViewLink` guardado en
// `fotoUrl` es la página VISOR de Drive, nunca embebible en un <img>. Mismo patrón que
// drive-fichas.service.js#descargarImagenFicha.
describe('descargarFotoCliente', () => {
  test('descarga bien y devuelve un Buffer con el binario', async () => {
    filesGet.mockResolvedValue({ data: Buffer.from('contenido-binario') });

    const result = await descargarFotoCliente('file-123');

    expect(filesGet).toHaveBeenCalledWith(
      { fileId: 'file-123', alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer' },
    );
    expect(Buffer.isBuffer(result)).toBe(true);
    expect(result.toString()).toBe('contenido-binario');
  });

  test('propaga el error de Drive como DriveLoyaltyFotoError', async () => {
    filesGet.mockRejectedValue(new Error('archivo no encontrado'));

    await expect(descargarFotoCliente('file-123')).rejects.toBeInstanceOf(DriveLoyaltyFotoError);
    await expect(descargarFotoCliente('file-123')).rejects.toThrow(/No se pudo descargar la foto del cliente/);
  });
});
