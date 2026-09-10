'use strict';

// anticipo-generado.service.test.js — recepción y correlación de anticipos que
// Kore genera automáticamente por sobrepago de una CxC cobrada vía Solicitudes de
// Cobro. Foco: la correlación por origenCuentaId (1:1, 0 candidatas, ambigüedad
// 2+ con desempate por fecha) y la idempotencia por anticipoIdErp — NUNCA se
// toca BankMovement/erpLinks acá (solo se lee movimientosDe(cr) para el snapshot).

jest.mock('./AnticipoGenerado.model');
jest.mock('./CollectionRequest.model');
jest.mock('./collection-request-asignaciones', () => ({ movimientosDe: jest.fn() }));
jest.mock('../../shared/socket', () => ({ emitToAll: jest.fn() }));

const AnticipoGenerado  = require('./AnticipoGenerado.model');
const CollectionRequest = require('./CollectionRequest.model');
const { movimientosDe } = require('./collection-request-asignaciones');
const { emitToAll }     = require('../../shared/socket');
const { registrarAnticipoGenerado } = require('./anticipo-generado.service');

const PAYLOAD_BASE = {
  id: 'kore-anticipo-1',
  serie: 'OPA', folio: '260900024', serieExterna: 'OPA', folioExterno: '00362',
  total: 536.17,
  fechaCreacion: '2026-09-10T15:23:10.6757Z',
  personaId: '1134', nombrePersona: 'ONESIMO ANTONIO RENDON ILESCAS',
  anotacion: 'Anticipo generado por el excedente cobrado en la venta A0-260900171',
  origenCuentaId: '6aa2c549694b720001f9ce51',
};

function fakeCR(overrides = {}) {
  return { _id: 'cr-1', status: 'identificada', resueltoAt: '2026-09-10T15:20:00.000Z', ...overrides };
}

function mockFindChain(result) {
  CollectionRequest.find.mockReturnValue({
    sort: jest.fn().mockReturnThis(),
    lean: jest.fn().mockResolvedValue(result),
  });
}

beforeEach(() => {
  jest.clearAllMocks();
  AnticipoGenerado.findOne.mockResolvedValue(null);
  movimientosDe.mockReturnValue(['mov-1']);
});

describe('registrarAnticipoGenerado — validación de campos', () => {
  test('rechaza sin id', async () => {
    await expect(registrarAnticipoGenerado({ ...PAYLOAD_BASE, id: undefined })).rejects.toThrow(/id/i);
  });
  test('rechaza sin total > 0', async () => {
    await expect(registrarAnticipoGenerado({ ...PAYLOAD_BASE, total: 0 })).rejects.toThrow(/total/i);
  });
  test('rechaza sin origenCuentaId', async () => {
    await expect(registrarAnticipoGenerado({ ...PAYLOAD_BASE, origenCuentaId: undefined })).rejects.toThrow(/origenCuentaId/i);
  });
});

describe('registrarAnticipoGenerado — idempotencia', () => {
  test('si ya existe un documento con ese anticipoIdErp, lo devuelve tal cual y NO vuelve a correlacionar ni crear', async () => {
    const existente = { toObject: () => ({ _id: 'a-existente', anticipoIdErp: PAYLOAD_BASE.id }) };
    AnticipoGenerado.findOne.mockResolvedValue(existente);

    const resultado = await registrarAnticipoGenerado(PAYLOAD_BASE);

    expect(resultado).toEqual({ _id: 'a-existente', anticipoIdErp: PAYLOAD_BASE.id });
    expect(CollectionRequest.find).not.toHaveBeenCalled();
    expect(AnticipoGenerado.create).not.toHaveBeenCalled();
  });

  test('race condition (E11000 en create) recupera el ganador por anticipoIdErp en vez de propagar el error', async () => {
    mockFindChain([]);
    const err = new Error('duplicate key'); err.code = 11000;
    AnticipoGenerado.create.mockRejectedValue(err);
    const ganador = { toObject: () => ({ _id: 'a-ganador' }) };
    AnticipoGenerado.findOne
      .mockResolvedValueOnce(null)       // primer chequeo de idempotencia
      .mockResolvedValueOnce(ganador);   // recuperación tras el 11000

    const resultado = await registrarAnticipoGenerado(PAYLOAD_BASE);

    expect(resultado).toEqual({ _id: 'a-ganador' });
  });
});

describe('registrarAnticipoGenerado — correlación', () => {
  test('1 sola CollectionRequest identificada con ese cxcs.erpId -> correlacionAutomatica:true, bankMovementIds desde movimientosDe(cr)', async () => {
    mockFindChain([fakeCR()]);
    movimientosDe.mockReturnValue(['mov-1', 'mov-2']);
    const creado = { _id: 'a1', toObject: () => ({ _id: 'a1', correlacionAutomatica: true }) };
    AnticipoGenerado.create.mockResolvedValue(creado);

    await registrarAnticipoGenerado(PAYLOAD_BASE);

    expect(CollectionRequest.find).toHaveBeenCalledWith({
      'cxcs.erpId': PAYLOAD_BASE.origenCuentaId,
      status: 'identificada',
    });
    const args = AnticipoGenerado.create.mock.calls[0][0];
    expect(args.solicitudCobroId).toBe('cr-1');
    expect(args.bankMovementIds).toEqual(['mov-1', 'mov-2']);
    expect(args.correlacionAutomatica).toBe(true);
    expect(args.motivoSinCorrelacion).toBeNull();
    expect(emitToAll).toHaveBeenCalledWith('collection-request:anticipo-generado', { anticipoId: 'a1' });
  });

  test('0 candidatas -> correlacionAutomatica:false, motivoSinCorrelacion descriptivo, sin bankMovementIds', async () => {
    mockFindChain([]);
    const creado = { _id: 'a1', toObject: () => ({ _id: 'a1' }) };
    AnticipoGenerado.create.mockResolvedValue(creado);

    await registrarAnticipoGenerado(PAYLOAD_BASE);

    const args = AnticipoGenerado.create.mock.calls[0][0];
    expect(args.solicitudCobroId).toBeNull();
    expect(args.bankMovementIds).toEqual([]);
    expect(args.correlacionAutomatica).toBe(false);
    expect(args.motivoSinCorrelacion).toMatch(/no se encontró/i);
    expect(movimientosDe).not.toHaveBeenCalled();
  });

  test('2+ candidatas, exactamente 1 resuelta ANTES de la fecha del anticipo -> se elige esa, sin ambigüedad', async () => {
    const antes    = fakeCR({ _id: 'cr-antes', resueltoAt: '2026-09-10T15:00:00.000Z' });
    const despues  = fakeCR({ _id: 'cr-despues', resueltoAt: '2026-09-10T16:00:00.000Z' }); // posterior al anticipo (15:23)
    mockFindChain([despues, antes]); // orden desc por resueltoAt, como haría el .sort real
    const creado = { _id: 'a1', toObject: () => ({ _id: 'a1' }) };
    AnticipoGenerado.create.mockResolvedValue(creado);

    await registrarAnticipoGenerado(PAYLOAD_BASE);

    const args = AnticipoGenerado.create.mock.calls[0][0];
    expect(args.solicitudCobroId).toBe('cr-antes');
    expect(args.correlacionAutomatica).toBe(true);
  });

  test('2+ candidatas, ninguna (o más de una) resuelta antes de la fecha del anticipo -> ambiguo, sin correlación automática', async () => {
    const c1 = fakeCR({ _id: 'cr-1', resueltoAt: '2026-09-10T16:00:00.000Z' }); // ambas posteriores
    const c2 = fakeCR({ _id: 'cr-2', resueltoAt: '2026-09-10T17:00:00.000Z' });
    mockFindChain([c2, c1]);
    const creado = { _id: 'a1', toObject: () => ({ _id: 'a1' }) };
    AnticipoGenerado.create.mockResolvedValue(creado);

    await registrarAnticipoGenerado(PAYLOAD_BASE);

    const args = AnticipoGenerado.create.mock.calls[0][0];
    expect(args.solicitudCobroId).toBeNull();
    expect(args.correlacionAutomatica).toBe(false);
    expect(args.motivoSinCorrelacion).toMatch(/ambiguo/i);
  });
});
