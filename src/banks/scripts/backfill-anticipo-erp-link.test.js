'use strict';

// backfill-anticipo-erp-link.test.js — _necesitaBackfill (pura) + run(), mismo
// patrón que backfill-formaspago-bankmovementid.test.js: modelos mockeados,
// mongoose.connect/disconnect espiados sobre el módulo real, sin conexión real
// a Mongo. _vincularAnticipoAlDeposito (anticipo-generado.service.js) se mockea
// completo — su comportamiento real ya se prueba en anticipo-generado.service.test.js.
jest.mock('../domains/collection-requests/AnticipoGenerado.model');
jest.mock('../domains/collection-requests/CollectionRequest.model');
jest.mock('../domains/banks/BankMovement.model');
jest.mock('../domains/collection-requests/anticipo-generado.service', () => ({
  _vincularAnticipoAlDeposito: jest.fn(),
}));

const mongoose = require('mongoose');
const AnticipoGenerado = require('../domains/collection-requests/AnticipoGenerado.model');
const CollectionRequest = require('../domains/collection-requests/CollectionRequest.model');
const BankMovement = require('../domains/banks/BankMovement.model');
const { _vincularAnticipoAlDeposito } = require('../domains/collection-requests/anticipo-generado.service');
const { _necesitaBackfill, run } = require('./backfill-anticipo-erp-link');

function mockFindQuery(resolvedValue) {
  return { select: jest.fn().mockReturnThis(), lean: jest.fn().mockResolvedValue(resolvedValue) };
}

describe('_necesitaBackfill', () => {
  test('true cuando ningún BankMovement tiene el erpLink del anticipo', () => {
    const anticipo = { anticipoIdErp: 'OPA-00370' };
    const bankMovements = [{ erpLinks: [{ erpId: 'CXC-1' }] }];
    expect(_necesitaBackfill(anticipo, bankMovements)).toBe(true);
  });

  test('true cuando AL MENOS UNO de varios movimientos (multi-bank-movement) no lo tiene', () => {
    const anticipo = { anticipoIdErp: 'OPA-00370' };
    const bankMovements = [{ erpLinks: [{ erpId: 'OPA-00370' }] }, { erpLinks: [] }];
    expect(_necesitaBackfill(anticipo, bankMovements)).toBe(true);
  });

  test('false cuando TODOS los movimientos ya tienen el erpLink (idempotencia)', () => {
    const anticipo = { anticipoIdErp: 'OPA-00370' };
    const bankMovements = [{ erpLinks: [{ erpId: 'CXC-1' }, { erpId: 'OPA-00370' }] }];
    expect(_necesitaBackfill(anticipo, bankMovements)).toBe(false);
  });

  test('false cuando no hay movimientos que revisar', () => {
    expect(_necesitaBackfill({ anticipoIdErp: 'OPA-00370' }, [])).toBe(false);
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

    await run({ mongodbUri: null });

    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining('MONGODB_URI'));
    expect(exitSpy).toHaveBeenCalledWith(1);
    expect(mongoose.connect).not.toHaveBeenCalled();
    expect(AnticipoGenerado.find).not.toHaveBeenCalled();
  });

  test('consulta con correlacionAutomatica:true y solicitudCobroId seteado', async () => {
    AnticipoGenerado.find.mockReturnValue(mockFindQuery([]));

    await run({ dryRun: true, mongodbUri: 'mongodb://fake' });

    expect(AnticipoGenerado.find).toHaveBeenCalledWith({
      correlacionAutomatica: true,
      solicitudCobroId: { $ne: null },
    });
  });

  test('dry-run (default): detecta el candidato pero NUNCA llama a _vincularAnticipoAlDeposito', async () => {
    AnticipoGenerado.find.mockReturnValue(mockFindQuery([
      { _id: 'a1', anticipoIdErp: 'OPA-00370', bankMovementIds: ['mov-1'], solicitudCobroId: 'cr-1' },
    ]));
    BankMovement.find.mockReturnValue(mockFindQuery([{ _id: 'mov-1', erpLinks: [{ erpId: 'CXC-1' }] }]));

    await run({ dryRun: true, mongodbUri: 'mongodb://fake' });

    expect(_vincularAnticipoAlDeposito).not.toHaveBeenCalled();
    expect(CollectionRequest.findById).not.toHaveBeenCalled();
  });

  test('--run: candidato real -> busca la CollectionRequest y llama a _vincularAnticipoAlDeposito', async () => {
    AnticipoGenerado.find.mockReturnValue(mockFindQuery([
      { _id: 'a1', anticipoIdErp: 'OPA-00370', bankMovementIds: ['mov-1'], solicitudCobroId: 'cr-1' },
    ]));
    BankMovement.find.mockReturnValue(mockFindQuery([{ _id: 'mov-1', erpLinks: [{ erpId: 'CXC-1' }] }]));
    const cr = { _id: 'cr-1', status: 'identificada' };
    CollectionRequest.findById.mockReturnValue(mockFindQuery(cr));
    _vincularAnticipoAlDeposito.mockResolvedValue(undefined);

    await run({ dryRun: false, mongodbUri: 'mongodb://fake' });

    expect(CollectionRequest.findById).toHaveBeenCalledWith('cr-1');
    expect(_vincularAnticipoAlDeposito).toHaveBeenCalledTimes(1);
    const [anticipoArg, crArg] = _vincularAnticipoAlDeposito.mock.calls[0];
    expect(anticipoArg.anticipoIdErp).toBe('OPA-00370');
    expect(crArg).toBe(cr);
  });

  test('ya vinculado (todos los movimientos ya tienen el erpLink): NO toca nada, ni en --run', async () => {
    AnticipoGenerado.find.mockReturnValue(mockFindQuery([
      { _id: 'a1', anticipoIdErp: 'OPA-00370', bankMovementIds: ['mov-1'], solicitudCobroId: 'cr-1' },
    ]));
    BankMovement.find.mockReturnValue(mockFindQuery([{ _id: 'mov-1', erpLinks: [{ erpId: 'OPA-00370' }] }]));

    await run({ dryRun: false, mongodbUri: 'mongodb://fake' });

    expect(CollectionRequest.findById).not.toHaveBeenCalled();
    expect(_vincularAnticipoAlDeposito).not.toHaveBeenCalled();
  });

  test('sin bankMovementIds (caso raro): se cuenta como sin cambio, no explota', async () => {
    AnticipoGenerado.find.mockReturnValue(mockFindQuery([
      { _id: 'a1', anticipoIdErp: 'OPA-00370', bankMovementIds: [], solicitudCobroId: 'cr-1' },
    ]));

    await expect(run({ dryRun: false, mongodbUri: 'mongodb://fake' })).resolves.toBeUndefined();
    expect(BankMovement.find).not.toHaveBeenCalled();
    expect(_vincularAnticipoAlDeposito).not.toHaveBeenCalled();
  });

  test('--run: la solicitudCobroId ya no existe (borrada) -> se cuenta como error, sigue con los demás', async () => {
    AnticipoGenerado.find.mockReturnValue(mockFindQuery([
      { _id: 'a1', anticipoIdErp: 'OPA-1', bankMovementIds: ['mov-1'], solicitudCobroId: 'cr-borrada' },
      { _id: 'a2', anticipoIdErp: 'OPA-2', bankMovementIds: ['mov-2'], solicitudCobroId: 'cr-2' },
    ]));
    BankMovement.find.mockReturnValue(mockFindQuery([{ _id: 'mov-x', erpLinks: [] }])); // ninguno tiene el link -> siempre candidato
    CollectionRequest.findById
      .mockReturnValueOnce(mockFindQuery(null))
      .mockReturnValueOnce(mockFindQuery({ _id: 'cr-2' }));
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

    await run({ dryRun: false, mongodbUri: 'mongodb://fake' });

    expect(_vincularAnticipoAlDeposito).toHaveBeenCalledTimes(1);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('cr-borrada'));
  });

  test('--run: un error en un anticipo no detiene el backfill de los demás', async () => {
    AnticipoGenerado.find.mockReturnValue(mockFindQuery([
      { _id: 'a1', anticipoIdErp: 'OPA-1', bankMovementIds: ['mov-1'], solicitudCobroId: 'cr-1' },
      { _id: 'a2', anticipoIdErp: 'OPA-2', bankMovementIds: ['mov-2'], solicitudCobroId: 'cr-2' },
    ]));
    BankMovement.find.mockReturnValue(mockFindQuery([{ _id: 'mov-x', erpLinks: [] }]));
    CollectionRequest.findById.mockReturnValue(mockFindQuery({ _id: 'cr-x' }));
    _vincularAnticipoAlDeposito
      .mockRejectedValueOnce(new Error('boom'))
      .mockResolvedValueOnce(undefined);
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

    await run({ dryRun: false, mongodbUri: 'mongodb://fake' });

    expect(_vincularAnticipoAlDeposito).toHaveBeenCalledTimes(2);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('boom'));
  });
});
