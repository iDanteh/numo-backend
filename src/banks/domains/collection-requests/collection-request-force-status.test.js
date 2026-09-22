'use strict';

// collection-request-force-status.test.js — forceStatus (2026-09-15, permiso
// collections:read:identificadas): list()/getById()/getComprobante()/
// analyzeStoredComprobantes() aceptan un 2do argumento { forceStatus } — list() lo usa
// para FORZAR el filtro de status (ignorando lo que pida la query), el resto tira
// ForbiddenError si la solicitud puntual no está en ese status. Mockea
// CollectionRequest.model (única dependencia real de estas 4 funciones) + drive-
// comprobantes.service/receipt.service, para confirmar que el guard corta ANTES de
// llegar a descargar el comprobante o correr OCR — mismo criterio de "mockear los
// límites de I/O" que collection-request-get-by-erp-id.test.js.
//
// ownerOnly/requestUserId (2026-09-22, bug real: rol Tienda, solo collections:read,
// quedaba bloqueado con 403 al ver su PROPIO detalle/comprobante/análisis) — getById()/
// getComprobante()/analyzeStoredComprobantes() ahora aceptan también { ownerOnly,
// requestUserId } vía _checkAccesoSolicitud(): sin restricción de status, pero solo si
// cr.solicitanteUserId === requestUserId. Ver describe()s "ownerOnly" de cada función.
jest.mock('./CollectionRequest.model');
jest.mock('./drive-comprobantes.service');
jest.mock('./receipt.service');

const CollectionRequest = require('./CollectionRequest.model');
const driveComprobantes  = require('./drive-comprobantes.service');
const { extractReceiptData, findMatchingMovements } = require('./receipt.service');
const { list, getById, getComprobante, analyzeStoredComprobantes } = require('./collection-request.service');

function mockLeanQuery(resolvedValue) {
  return {
    sort:     jest.fn().mockReturnThis(),
    skip:     jest.fn().mockReturnThis(),
    limit:    jest.fn().mockReturnThis(),
    select:   jest.fn().mockReturnThis(),
    populate: jest.fn().mockReturnThis(),
    lean:     jest.fn().mockResolvedValue(resolvedValue),
  };
}

// getComprobante()/analyzeStoredComprobantes() NO usan .lean() (ver comentario real en
// collection-request.service.js: necesitan el cast normal de Mongoose a Buffer) — el
// query resuelve directo en .select(...).
function mockSelectOnlyQuery(resolvedValue) {
  return { select: jest.fn().mockResolvedValue(resolvedValue) };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('list() — forceStatus fuerza el filtro, gana sobre filters.status', () => {
  test('sin forceStatus: usa filters.status tal cual (comportamiento de siempre)', async () => {
    CollectionRequest.find.mockReturnValue(mockLeanQuery([]));
    CollectionRequest.countDocuments.mockResolvedValue(0);

    await list({ status: 'pendiente' });

    expect(CollectionRequest.find).toHaveBeenCalledWith(expect.objectContaining({ status: 'pendiente' }));
  });

  test('con forceStatus=identificada: se usa AUNQUE filters.status pida otro status', async () => {
    CollectionRequest.find.mockReturnValue(mockLeanQuery([]));
    CollectionRequest.countDocuments.mockResolvedValue(0);

    await list({ status: 'pendiente' }, { forceStatus: 'identificada' });

    expect(CollectionRequest.find).toHaveBeenCalledWith(expect.objectContaining({ status: 'identificada' }));
  });

  test('con forceStatus, sin status en la query: igual filtra por forceStatus', async () => {
    CollectionRequest.find.mockReturnValue(mockLeanQuery([]));
    CollectionRequest.countDocuments.mockResolvedValue(0);

    await list({}, { forceStatus: 'identificada' });

    expect(CollectionRequest.find).toHaveBeenCalledWith(expect.objectContaining({ status: 'identificada' }));
  });
});

describe('getById() — forceStatus bloquea el detalle de una solicitud fuera de status', () => {
  test('sin forceStatus: devuelve la solicitud sin importar su status (comportamiento de siempre)', async () => {
    CollectionRequest.findById.mockReturnValue(mockLeanQuery({ _id: 'cr1', status: 'pendiente', comprobante: {} }));

    const resultado = await getById('cr1');

    expect(resultado._id).toBe('cr1');
  });

  test('forceStatus=identificada + status real identificada: pasa, devuelve la solicitud', async () => {
    CollectionRequest.findById.mockReturnValue(mockLeanQuery({ _id: 'cr1', status: 'identificada', comprobante: {} }));

    const resultado = await getById('cr1', { forceStatus: 'identificada' });

    expect(resultado._id).toBe('cr1');
  });

  test('forceStatus=identificada + status real pendiente: ForbiddenError (403), no NotFoundError', async () => {
    CollectionRequest.findById.mockReturnValue(mockLeanQuery({ _id: 'cr1', status: 'pendiente', comprobante: {} }));

    await expect(getById('cr1', { forceStatus: 'identificada' })).rejects.toMatchObject({
      name: 'ForbiddenError', statusCode: 403,
    });
  });

  test('ownerOnly + dueño real (mismo solicitanteUserId): pasa, sin importar el status', async () => {
    CollectionRequest.findById.mockReturnValue(mockLeanQuery({ _id: 'cr1', status: 'pendiente', solicitanteUserId: 'tienda-1', comprobante: {} }));

    const resultado = await getById('cr1', { ownerOnly: true, requestUserId: 'tienda-1' });

    expect(resultado._id).toBe('cr1');
  });

  test('ownerOnly + solicitud ajena: ForbiddenError (403), no NotFoundError', async () => {
    CollectionRequest.findById.mockReturnValue(mockLeanQuery({ _id: 'cr1', status: 'pendiente', solicitanteUserId: 'tienda-1', comprobante: {} }));

    await expect(getById('cr1', { ownerOnly: true, requestUserId: 'otra-tienda' })).rejects.toMatchObject({
      name: 'ForbiddenError', statusCode: 403,
    });
  });
});

describe('getComprobante() — mismo guard, corta ANTES de descargar el archivo de Drive', () => {
  test('status no coincide con forceStatus: ForbiddenError, driveComprobantes.descargarComprobante NUNCA se llama', async () => {
    CollectionRequest.findById.mockReturnValue(mockSelectOnlyQuery({ status: 'pendiente', comprobantes: [] }));

    await expect(getComprobante('cr1', 0, { forceStatus: 'identificada' })).rejects.toMatchObject({
      name: 'ForbiddenError', statusCode: 403,
    });
    expect(driveComprobantes.descargarComprobante).not.toHaveBeenCalled();
  });

  test('ownerOnly + solicitud ajena: ForbiddenError, driveComprobantes.descargarComprobante NUNCA se llama', async () => {
    CollectionRequest.findById.mockReturnValue(mockSelectOnlyQuery({ status: 'pendiente', solicitanteUserId: 'tienda-1', comprobantes: [] }));

    await expect(getComprobante('cr1', 0, { ownerOnly: true, requestUserId: 'otra-tienda' })).rejects.toMatchObject({
      name: 'ForbiddenError', statusCode: 403,
    });
    expect(driveComprobantes.descargarComprobante).not.toHaveBeenCalled();
  });

  test('ownerOnly + dueño real: pasa, sin importar el status (mismo alcance que ya tenía por /mias)', async () => {
    CollectionRequest.findById.mockReturnValue(mockSelectOnlyQuery({
      status: 'pendiente', solicitanteUserId: 'tienda-1',
      comprobantes: [{ storage: 'drive', driveFileId: 'f1', mimetype: 'image/png', originalName: 'a.png' }],
    }));

    const resultado = await getComprobante('cr1', 0, { ownerOnly: true, requestUserId: 'tienda-1' });

    expect(resultado.mimetype).toBe('image/png');
  });
});

describe('analyzeStoredComprobantes() — mismo guard, corta ANTES de correr OCR', () => {
  test('status no coincide con forceStatus: ForbiddenError, ni extractReceiptData ni findMatchingMovements se llaman', async () => {
    CollectionRequest.findById.mockReturnValue(mockSelectOnlyQuery({ status: 'pendiente', comprobantes: [] }));

    await expect(analyzeStoredComprobantes('cr1', { forceStatus: 'identificada' })).rejects.toMatchObject({
      name: 'ForbiddenError', statusCode: 403,
    });
    expect(extractReceiptData).not.toHaveBeenCalled();
    expect(findMatchingMovements).not.toHaveBeenCalled();
  });

  test('ownerOnly + solicitud ajena: ForbiddenError, ni extractReceiptData ni findMatchingMovements se llaman', async () => {
    CollectionRequest.findById.mockReturnValue(mockSelectOnlyQuery({ status: 'pendiente', solicitanteUserId: 'tienda-1', comprobantes: [] }));

    await expect(analyzeStoredComprobantes('cr1', { ownerOnly: true, requestUserId: 'otra-tienda' })).rejects.toMatchObject({
      name: 'ForbiddenError', statusCode: 403,
    });
    expect(extractReceiptData).not.toHaveBeenCalled();
    expect(findMatchingMovements).not.toHaveBeenCalled();
  });
});
