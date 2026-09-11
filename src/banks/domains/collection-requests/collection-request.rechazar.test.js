'use strict';

// collection-request.rechazar.test.js — rechazar(): no tenía NINGÚN test hasta
// ahora. Cobertura enfocada SOLO en el caso nuevo (detección de estatus real
// vía estatusActualDeErrorKore) — mismo patrón de mocks que
// collection-request.identificar.test.js (misma función vecina, mismos límites
// de I/O: 2 modelos Mongoose + koreCaja + socket).

jest.mock('./CollectionRequest.model');
jest.mock('../erp/kore-caja.service');
jest.mock('../../shared/socket');

const CollectionRequest = require('./CollectionRequest.model');
const koreCaja          = require('../erp/kore-caja.service');
const { emitToAll }      = require('../../shared/socket');

const service = require('./collection-request.service');

class KoreCajaError extends Error {}
koreCaja.KoreCajaError = KoreCajaError;

function makeCr({ id = 'cr-1', status = 'pendiente' } = {}) {
  return { _id: id, solicitudIdErp: 'SOL-1', status };
}

beforeEach(() => {
  jest.clearAllMocks();
  koreCaja.estatusActualDeErrorKore = jest.fn(() => null);
  koreCaja.obtenerTokenKore.mockResolvedValue('token-revisor');
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  console.warn.mockRestore();
});

describe('rechazar() — Kore ya en APLICADO (el cobro real ya se realizó)', () => {
  test('estatusActualDeErrorKore=APLICADO -> lanza BadRequestError explicando que hay que Identificar, y NO marca rechazada', async () => {
    const cr = makeCr();
    CollectionRequest.findOne.mockResolvedValue(cr);
    CollectionRequest.findOneAndUpdate = jest.fn();
    koreCaja.actualizarEstatusSolicitud.mockRejectedValue(
      new KoreCajaError('No puede cambiar el estatus de la solicitud con estatus: APLICADO'),
    );
    koreCaja.estatusActualDeErrorKore.mockReturnValue('APLICADO');

    await expect(
      service.rechazar('cr-1', 'motivo cualquiera', { _id: 'user-1' }),
    ).rejects.toThrow(/no se puede rechazar.*identificar/is);

    // Nunca llega a marcar la solicitud como rechazada en Mongo.
    expect(CollectionRequest.findOneAndUpdate).not.toHaveBeenCalled();
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('estatusActualDeErrorKore=RECHAZADO (reintento idempotente, comportamiento previo sin cambios) -> continúa y marca rechazada', async () => {
    const cr = makeCr();
    CollectionRequest.findOne.mockResolvedValue(cr);
    const actualizada = { ...cr, status: 'rechazada' };
    CollectionRequest.findOneAndUpdate = jest.fn().mockResolvedValue(actualizada);
    koreCaja.actualizarEstatusSolicitud.mockRejectedValue(
      new KoreCajaError('No puede cambiar el estatus de la solicitud con estatus: RECHAZADO'),
    );
    koreCaja.estatusActualDeErrorKore.mockReturnValue('RECHAZADO');

    const resultado = await service.rechazar('cr-1', 'motivo cualquiera', { _id: 'user-1' });

    expect(CollectionRequest.findOneAndUpdate).toHaveBeenCalledTimes(1);
    expect(resultado.status).toBe('rechazada');
  });
});
