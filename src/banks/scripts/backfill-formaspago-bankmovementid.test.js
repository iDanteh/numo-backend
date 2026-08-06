'use strict';

// backfill-formaspago-bankmovementid.test.js — _necesitaBackfill (pura) +
// run() con CollectionRequest.model mockeado (automock, mismo patrón que
// collection-request-get-by-erp-id.test.js) y mongoose.connect/disconnect
// espiados sobre el módulo REAL (no reemplazado por completo, para no romper
// el require('mongoose') interno de CollectionRequest.model.js al definir su
// schema) — sin conexión real a Mongo en ningún caso.
jest.mock('../domains/collection-requests/CollectionRequest.model');

const mongoose          = require('mongoose');
const CollectionRequest = require('../domains/collection-requests/CollectionRequest.model');
const { _necesitaBackfill, run } = require('./backfill-formaspago-bankmovementid');

function mockFindQuery(resolvedValue) {
  return {
    select: jest.fn().mockReturnThis(),
    lean:   jest.fn().mockResolvedValue(resolvedValue),
  };
}

describe('_necesitaBackfill', () => {
  test('true cuando al menos 1 formaPago no tiene bankMovementId propio (null)', () => {
    const cr = { formasPago: [{ bankMovementId: 'mov-1' }, { bankMovementId: null }] };
    expect(_necesitaBackfill(cr)).toBe(true);
  });

  test('true cuando el campo está ausente (undefined), no solo null', () => {
    const cr = { formasPago: [{}] };
    expect(_necesitaBackfill(cr)).toBe(true);
  });

  test('false cuando TODAS las formasPago ya tienen su propio bankMovementId (ya migrada — idempotencia)', () => {
    const cr = { formasPago: [{ bankMovementId: 'mov-1' }, { bankMovementId: 'mov-2' }] };
    expect(_necesitaBackfill(cr)).toBe(false);
  });

  test('false cuando formasPago está vacío', () => {
    expect(_necesitaBackfill({ formasPago: [] })).toBe(false);
  });
});

describe('run()', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.spyOn(mongoose, 'connect').mockResolvedValue(undefined);
    jest.spyOn(mongoose, 'disconnect').mockResolvedValue(undefined);
  });

  afterEach(() => jest.restoreAllMocks());

  test('sin MONGODB_URI: no conecta, no consulta, no escribe', async () => {
    const exitSpy  = jest.spyOn(process, 'exit').mockImplementation(() => {});
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

    // null, no undefined: un mongodbUri undefined activaría el valor default
    // del parámetro (process.env.MONGODB_URI), que SÍ está seteado en .env de
    // este repo (mongodb://localhost:27017/cfdi_comparator) — null lo evita.
    await run({ mongodbUri: null });

    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining('MONGODB_URI'));
    expect(exitSpy).toHaveBeenCalledWith(1);
    expect(mongoose.connect).not.toHaveBeenCalled();
    expect(CollectionRequest.find).not.toHaveBeenCalled();
  });

  test('dry-run (default): selecciona candidatas pero NUNCA llama updateOne', async () => {
    CollectionRequest.find.mockReturnValue(mockFindQuery([
      { _id: 'cr-1', bankMovementId: 'mov-1', formasPago: [{ bankMovementId: null }] },   // candidata
      { _id: 'cr-2', bankMovementId: 'mov-2', formasPago: [{ bankMovementId: 'mov-2' }] }, // ya migrada
    ]));

    await run({ dryRun: true, mongodbUri: 'mongodb://fake' });

    expect(CollectionRequest.find).toHaveBeenCalledWith({ status: 'identificada', bankMovementId: { $ne: null } });
    expect(CollectionRequest.updateOne).not.toHaveBeenCalled();
    expect(mongoose.connect).toHaveBeenCalledWith('mongodb://fake');
    expect(mongoose.disconnect).toHaveBeenCalled();
  });

  test('--run: escribe SOLO en las candidatas, con el arrayFilters correcto, y NUNCA toca las ya migradas', async () => {
    CollectionRequest.find.mockReturnValue(mockFindQuery([
      { _id: 'cr-1', bankMovementId: 'mov-1', formasPago: [{ bankMovementId: null }, { bankMovementId: null }] },
      { _id: 'cr-2', bankMovementId: 'mov-2', formasPago: [{ bankMovementId: 'mov-2' }] },
    ]));
    CollectionRequest.updateOne.mockResolvedValue({ modifiedCount: 2 });

    await run({ dryRun: false, mongodbUri: 'mongodb://fake' });

    expect(CollectionRequest.updateOne).toHaveBeenCalledTimes(1);
    expect(CollectionRequest.updateOne).toHaveBeenCalledWith(
      { _id: 'cr-1' },
      { $set: { 'formasPago.$[f].bankMovementId': 'mov-1' } },
      { arrayFilters: [{ 'f.bankMovementId': null }] },
    );
  });

  test('--run: root bankMovementId de cada solicitud viaja intacto al $set (nunca se borra ni se transforma)', async () => {
    CollectionRequest.find.mockReturnValue(mockFindQuery([
      { _id: 'cr-1', bankMovementId: 'mov-XYZ', formasPago: [{ bankMovementId: null }] },
    ]));
    CollectionRequest.updateOne.mockResolvedValue({ modifiedCount: 1 });

    await run({ dryRun: false, mongodbUri: 'mongodb://fake' });

    const [, update] = CollectionRequest.updateOne.mock.calls[0];
    expect(update.$set['formasPago.$[f].bankMovementId']).toBe('mov-XYZ');
  });

  test('--run: si updateOne no modifica nada (carrera con otra corrida), no lanza y sigue con las demás', async () => {
    CollectionRequest.find.mockReturnValue(mockFindQuery([
      { _id: 'cr-1', bankMovementId: 'mov-1', formasPago: [{ bankMovementId: null }] },
      { _id: 'cr-2', bankMovementId: 'mov-2', formasPago: [{ bankMovementId: null }] },
    ]));
    CollectionRequest.updateOne.mockResolvedValue({ modifiedCount: 0 });

    await expect(run({ dryRun: false, mongodbUri: 'mongodb://fake' })).resolves.toBeUndefined();
    expect(CollectionRequest.updateOne).toHaveBeenCalledTimes(2);
  });

  test('--run: un error en una solicitud no detiene el backfill de las demás', async () => {
    CollectionRequest.find.mockReturnValue(mockFindQuery([
      { _id: 'cr-1', bankMovementId: 'mov-1', formasPago: [{ bankMovementId: null }] },
      { _id: 'cr-2', bankMovementId: 'mov-2', formasPago: [{ bankMovementId: null }] },
    ]));
    CollectionRequest.updateOne
      .mockRejectedValueOnce(new Error('boom'))
      .mockResolvedValueOnce({ modifiedCount: 1 });
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

    await run({ dryRun: false, mongodbUri: 'mongodb://fake' });

    expect(CollectionRequest.updateOne).toHaveBeenCalledTimes(2);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('boom'));
  });
});
