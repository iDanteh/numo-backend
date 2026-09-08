'use strict';

// bank.service.ficha.test.js — setFicha()/deleteFicha() (BankMovement.model.js `ficha`,
// respaldo documental cargado a mano por el contador). Pedido explícito del usuario
// 2026-09-03: ambas funciones deben emitir 'bank:ficha-pendiente:changed' (emitToAll,
// cross-banco) SOLO cuando el movimiento tiene un erpLink origen:'transferencia-caja' (ver
// caja-transferencia-confirm.service.js) — cualquier otro movimiento con ficha (la mayoría)
// no le interesa a esa bandeja, y no debe disparar un refresco irrelevante.
//
// Mismo patrón que bank.service.setErpIds.test.js: solo se mockean las dependencias con
// I/O (BankMovement.model, shared/socket) — resolvePrimeraIdentificacion/aplicarLogicaErp
// son lógica interna pura, corren reales.
jest.mock('./BankMovement.model');
jest.mock('../../shared/socket');
jest.mock('./drive-fichas.service');

const BankMovement = require('./BankMovement.model');
const { emitToBanco, emitToAll } = require('../../shared/socket');
const { subirImagenFicha, eliminarImagenFicha, descargarImagenFicha } = require('./drive-fichas.service');
const bankService = require('./bank.service');

function fakeMov(overrides = {}) {
  return {
    // `folio` (consecutivo de NUMO, asignado por Counter) — base del nombre del
    // documento en Drive desde el 2026-09-04. Distinto de `ficha` (folio físico
    // que tipea el contador a mano).
    _id: 'mov-1', banco: 'BBVA', folio: '00123', ficha: null, fichaBy: null, fichaNombre: null, fichaAt: null,
    fichaDriveFileId: null, fichaDriveWebViewLink: null, fichaDriveMimeType: null,
    status: 'no_identificado', erpLinks: [], primeraIdentificacionAt: null, primeraIdentificacionPor: null,
    save: jest.fn(function () { return Promise.resolve(this); }),
    ...overrides,
  };
}

const USER = { _id: 'user-1', nombre: 'Ana', role: 'contabilidad' };

beforeEach(() => {
  jest.clearAllMocks();
});

describe('setFicha', () => {
  test('movimiento SIN erpLink transferencia-caja (caso normal, la mayoría): NO emite bank:ficha-pendiente:changed', async () => {
    const mov = fakeMov({ erpLinks: [] });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await bankService.setFicha('mov-1', '00123', USER);

    expect(emitToBanco).toHaveBeenCalledTimes(1);
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('movimiento CON erpLink origen:transferencia-caja: SÍ emite bank:ficha-pendiente:changed con el movementId', async () => {
    const mov = fakeMov({ erpLinks: [{ erpId: 'CAJA-abc123', origen: 'transferencia-caja' }] });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await bankService.setFicha('mov-1', '00123', USER);

    expect(emitToAll).toHaveBeenCalledTimes(1);
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
  });

  test('movimiento con erpLink de OTRO origen (no transferencia-caja): NO emite', async () => {
    const mov = fakeMov({ erpLinks: [{ erpId: 'CXC-1', origen: null }] });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await bankService.setFicha('mov-1', '00123', USER);

    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('movimiento inexistente: NotFoundError, no emite nada', async () => {
    BankMovement.findById = jest.fn().mockResolvedValue(null);
    await expect(bankService.setFicha('mov-x', '00123', USER)).rejects.toThrow('Movimiento');
    expect(emitToBanco).not.toHaveBeenCalled();
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('movimiento que YA tiene ficha: ConflictError, no emite nada', async () => {
    const mov = fakeMov({ ficha: '00099', erpLinks: [{ erpId: 'CAJA-abc123', origen: 'transferencia-caja' }] });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await expect(bankService.setFicha('mov-1', '00123', USER)).rejects.toThrow(/ya tiene una ficha/);
    expect(emitToAll).not.toHaveBeenCalled();
  });
});

describe('adjuntarImagenFicha', () => {
  const IMAGEN = { buffer: Buffer.from('img'), mimetype: 'image/png', originalname: 'ficha.png' };

  test('sube bien y setea fichaDriveFileId/fichaDriveWebViewLink/fichaDriveMimeType', async () => {
    const mov = fakeMov({ folio: '00123' });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);
    subirImagenFicha.mockResolvedValue({ driveFileId: 'file-1', driveWebViewLink: 'https://drive/file-1' });

    const result = await bankService.adjuntarImagenFicha('mov-1', IMAGEN);

    // CORRECCIÓN 2026-09-04: el nombre en Drive usa mov.folio (consecutivo de NUMO,
    // único y estable) + banco, NUNCA mov.ficha (texto libre, puede variar/repetirse).
    expect(subirImagenFicha).toHaveBeenCalledWith(IMAGEN.buffer, IMAGEN.mimetype, '00123 - BBVA.png');
    expect(mov.fichaDriveFileId).toBe('file-1');
    expect(mov.fichaDriveWebViewLink).toBe('https://drive/file-1');
    expect(mov.fichaDriveMimeType).toBe('image/png');
    expect(mov.save).toHaveBeenCalledTimes(1);
    expect(result).toEqual({
      _id: 'mov-1', fichaDriveFileId: 'file-1', fichaDriveWebViewLink: 'https://drive/file-1',
      fichaDriveMimeType: 'image/png',
    });
    expect(emitToBanco).toHaveBeenCalledWith('BBVA', 'bank:movement:updated', {
      _id: 'mov-1', fichaDriveFileId: 'file-1', fichaDriveWebViewLink: 'https://drive/file-1',
      fichaDriveMimeType: 'image/png',
    });
  });

  test('funciona aunque el movimiento NO tenga ficha registrada todavía (documento independiente)', async () => {
    const mov = fakeMov({ folio: '00123', ficha: null, fichaBy: null });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);
    subirImagenFicha.mockResolvedValue({ driveFileId: 'file-1', driveWebViewLink: null });

    const result = await bankService.adjuntarImagenFicha('mov-1', IMAGEN);

    expect(subirImagenFicha).toHaveBeenCalledWith(IMAGEN.buffer, IMAGEN.mimetype, '00123 - BBVA.png');
    expect(result.fichaDriveFileId).toBe('file-1');
  });

  test('movimiento legacy sin folio asignado: usa el _id como respaldo del nombre', async () => {
    const mov = fakeMov({ folio: null });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);
    subirImagenFicha.mockResolvedValue({ driveFileId: 'file-1', driveWebViewLink: null });

    await bankService.adjuntarImagenFicha('mov-1', IMAGEN);

    expect(subirImagenFicha).toHaveBeenCalledWith(IMAGEN.buffer, IMAGEN.mimetype, 'mov-1 - BBVA.png');
  });

  test('archivo sin extensión en el nombre original (ej. foto de cámara): usa la extensión del mimetype', async () => {
    const mov = fakeMov({ folio: '00123', banco: 'Santander' });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);
    subirImagenFicha.mockResolvedValue({ driveFileId: 'file-1', driveWebViewLink: null });
    const imagenSinExtension = { buffer: Buffer.from('img'), mimetype: 'image/jpeg', originalname: 'IMG20260904' };

    await bankService.adjuntarImagenFicha('mov-1', imagenSinExtension);

    expect(subirImagenFicha).toHaveBeenCalledWith(imagenSinExtension.buffer, imagenSinExtension.mimetype, '00123 - Santander.jpg');
  });

  test('folio con caracteres inválidos para un nombre de archivo: los quita antes de armar el nombre', async () => {
    const mov = fakeMov({ folio: '00/123:00' });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);
    subirImagenFicha.mockResolvedValue({ driveFileId: 'file-1', driveWebViewLink: null });

    await bankService.adjuntarImagenFicha('mov-1', IMAGEN);

    expect(subirImagenFicha).toHaveBeenCalledWith(IMAGEN.buffer, IMAGEN.mimetype, '0012300 - BBVA.png');
  });
});

describe('deleteFicha', () => {
  test('movimiento SIN erpLink transferencia-caja: NO emite bank:ficha-pendiente:changed', async () => {
    const mov = fakeMov({ ficha: '00123', fichaBy: 'user-1', erpLinks: [] });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await bankService.deleteFicha('mov-1', USER);

    expect(emitToBanco).toHaveBeenCalledTimes(1);
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('movimiento CON erpLink origen:transferencia-caja: SÍ emite bank:ficha-pendiente:changed con el movementId', async () => {
    const mov = fakeMov({
      ficha: '00123', fichaBy: 'user-1',
      erpLinks: [{ erpId: 'CAJA-abc123', origen: 'transferencia-caja' }],
    });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await bankService.deleteFicha('mov-1', USER);

    expect(emitToAll).toHaveBeenCalledTimes(1);
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
  });

  test('movimiento sin ficha registrada: BadRequestError, no emite nada', async () => {
    const mov = fakeMov({ ficha: null });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await expect(bankService.deleteFicha('mov-1', USER)).rejects.toThrow(/no tiene ficha registrada/);
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('usuario sin permiso (no admin, no autor): ForbiddenError, no emite nada', async () => {
    const mov = fakeMov({ ficha: '00123', fichaBy: 'otro-user', erpLinks: [{ erpId: 'CAJA-abc123', origen: 'transferencia-caja' }] });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await expect(bankService.deleteFicha('mov-1', USER)).rejects.toThrow(/Solo el usuario/);
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('CORRECCIÓN 2026-09-04: con un documento ya adjuntado, deleteFicha() NO lo toca — el documento es independiente de la ficha', async () => {
    const mov = fakeMov({
      ficha: '00123', fichaBy: 'user-1', fichaDriveFileId: 'file-1',
      fichaDriveWebViewLink: 'https://drive/file-1', fichaDriveMimeType: 'image/png',
    });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    const result = await bankService.deleteFicha('mov-1', USER);

    // El folio se borra (comportamiento de siempre), pero el documento SOBREVIVE —
    // ya no depende del campo `ficha` (que puede ser variable/no confiable), sino
    // de mov.folio (el consecutivo estable de NUMO).
    expect(mov.ficha).toBeNull();
    expect(mov.fichaDriveFileId).toBe('file-1');
    expect(mov.fichaDriveWebViewLink).toBe('https://drive/file-1');
    expect(mov.fichaDriveMimeType).toBe('image/png');
    expect(eliminarImagenFicha).not.toHaveBeenCalled();
    expect(result).not.toHaveProperty('fichaDriveFileId');
    expect(emitToBanco).toHaveBeenCalledWith('BBVA', 'bank:movement:updated',
      expect.not.objectContaining({ fichaDriveFileId: expect.anything() }));
  });
});

describe('quitarImagenFicha', () => {
  test('quita el documento sin tocar el folio/ficha, borra la imagen de Drive best-effort', async () => {
    const mov = fakeMov({
      ficha: '00123', fichaBy: 'user-1', fichaDriveFileId: 'file-1',
      fichaDriveWebViewLink: 'https://drive/file-1', fichaDriveMimeType: 'image/png',
    });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);
    eliminarImagenFicha.mockResolvedValue({});

    const result = await bankService.quitarImagenFicha('mov-1');

    // El folio y su autoría NO se tocan.
    expect(mov.ficha).toBe('00123');
    expect(mov.fichaBy).toBe('user-1');
    expect(mov.fichaDriveFileId).toBeNull();
    expect(mov.fichaDriveWebViewLink).toBeNull();
    expect(mov.fichaDriveMimeType).toBeNull();
    expect(mov.save).toHaveBeenCalledTimes(1);
    expect(eliminarImagenFicha).toHaveBeenCalledWith('file-1');
    expect(result).toEqual({
      _id: 'mov-1', fichaDriveFileId: null, fichaDriveWebViewLink: null, fichaDriveMimeType: null,
    });
    expect(emitToBanco).toHaveBeenCalledWith('BBVA', 'bank:movement:updated', {
      _id: 'mov-1', fichaDriveFileId: null, fichaDriveWebViewLink: null, fichaDriveMimeType: null,
    });
  });

  test('funciona sin ficha registrada (documento adjuntado antes de tipear el folio físico, sin fichaBy con quién comparar)', async () => {
    const mov = fakeMov({ ficha: null, fichaBy: null, fichaDriveFileId: 'file-1' });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);
    eliminarImagenFicha.mockResolvedValue({});

    const result = await bankService.quitarImagenFicha('mov-1');

    expect(result.fichaDriveFileId).toBeNull();
  });

  test('movimiento sin documento adjunto: BadRequestError, no borra nada', async () => {
    const mov = fakeMov({ ficha: '00123', fichaBy: 'user-1', fichaDriveFileId: null });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await expect(bankService.quitarImagenFicha('mov-1')).rejects.toThrow(/no tiene ningún documento adjunto/);
    expect(eliminarImagenFicha).not.toHaveBeenCalled();
    expect(mov.save).not.toHaveBeenCalled();
  });

  test('si el borrado de Drive falla, igual devuelve éxito (best-effort, no bloqueante)', async () => {
    const mov = fakeMov({ ficha: '00123', fichaBy: 'user-1', fichaDriveFileId: 'file-1' });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);
    eliminarImagenFicha.mockRejectedValue(new Error('Drive no disponible'));

    await expect(bankService.quitarImagenFicha('mov-1')).resolves.toEqual(
      expect.objectContaining({ _id: 'mov-1', fichaDriveFileId: null }),
    );

    await new Promise((resolve) => setImmediate(resolve));
    expect(eliminarImagenFicha).toHaveBeenCalledWith('file-1');
  });

  test('movimiento inexistente: NotFoundError', async () => {
    BankMovement.findById = jest.fn().mockResolvedValue(null);

    await expect(bankService.quitarImagenFicha('mov-x')).rejects.toThrow('Movimiento');
    expect(eliminarImagenFicha).not.toHaveBeenCalled();
  });
});

describe('obtenerImagenFicha', () => {
  test('descarga bien y devuelve { data, mimetype }', async () => {
    const mov = fakeMov({ fichaDriveFileId: 'file-1', fichaDriveMimeType: 'image/png' });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);
    descargarImagenFicha.mockResolvedValue(Buffer.from('contenido'));

    const result = await bankService.obtenerImagenFicha('mov-1');

    expect(descargarImagenFicha).toHaveBeenCalledWith('file-1');
    expect(Buffer.isBuffer(result.data)).toBe(true);
    expect(result.mimetype).toBe('image/png');
  });

  test('movimiento inexistente: NotFoundError', async () => {
    BankMovement.findById = jest.fn().mockResolvedValue(null);

    await expect(bankService.obtenerImagenFicha('mov-x')).rejects.toThrow('Movimiento');
    expect(descargarImagenFicha).not.toHaveBeenCalled();
  });

  test('movimiento sin fichaDriveFileId: NotFoundError, no descarga nada', async () => {
    const mov = fakeMov({ fichaDriveFileId: null });
    BankMovement.findById = jest.fn().mockResolvedValue(mov);

    await expect(bankService.obtenerImagenFicha('mov-1')).rejects.toThrow(/no tiene ninguna imagen/);
    expect(descargarImagenFicha).not.toHaveBeenCalled();
  });
});
