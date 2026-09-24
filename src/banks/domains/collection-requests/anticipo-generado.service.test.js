'use strict';

// anticipo-generado.service.test.js — recepción y correlación de anticipos que
// Kore genera automáticamente por sobrepago de una CxC cobrada vía Solicitudes de
// Cobro. Foco: la correlación por origenCuentaId (1:1, 0 candidatas, ambigüedad
// 2+ con desempate por fecha) y la idempotencia por anticipoIdErp — NUNCA se
// toca BankMovement/erpLinks acá (solo se lee movimientosDe(cr) para el snapshot).

jest.mock('./AnticipoGenerado.model');
jest.mock('./CollectionRequest.model');
jest.mock('../banks/BankMovement.model');
jest.mock('../banks/bank.service', () => ({ setErpIds: jest.fn() }));
jest.mock('./collection-request-asignaciones', () => ({ movimientosDe: jest.fn() }));
jest.mock('../../shared/socket', () => ({ emitToAll: jest.fn() }));
jest.mock('../../shared/utils/logger', () => ({ logger: { error: jest.fn() } }));

const AnticipoGenerado  = require('./AnticipoGenerado.model');
const CollectionRequest = require('./CollectionRequest.model');
const BankMovement      = require('../banks/BankMovement.model');
const bankService       = require('../banks/bank.service');
const { movimientosDe } = require('./collection-request-asignaciones');
const { emitToAll }     = require('../../shared/socket');
const { logger }        = require('../../shared/utils/logger');
const {
  registrarAnticipoGenerado, reconciliarAnticiposPendientes, _vincularAnticipoAlDeposito,
} = require('./anticipo-generado.service');

// Cadena por default (mov sin ningún erpLink previo) — los tests que necesiten
// preservar entradas existentes la pisan explícitamente con su propio mock.
function mockBankMovementFindById(erpLinksExistentes = []) {
  BankMovement.findById.mockReturnValue({
    select: jest.fn().mockReturnThis(),
    lean: jest.fn().mockResolvedValue({ erpLinks: erpLinksExistentes }),
  });
}

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
  mockBankMovementFindById([]); // sin erpLinks previos por default
  bankService.setErpIds.mockResolvedValue({});
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

  test('correlación exitosa también vincula el erpLink del anticipo en el/los BankMovement (bankService.setErpIds)', async () => {
    mockFindChain([fakeCR()]);
    movimientosDe.mockReturnValue(['mov-1']);
    mockBankMovementFindById([{ erpId: 'CXC-ORIGINAL', total: 1000 }]); // link de la CxC ya presente
    const creado = {
      _id: 'a1', anticipoIdErp: PAYLOAD_BASE.id, monto: PAYLOAD_BASE.total,
      anticipoSerieExterna: PAYLOAD_BASE.serieExterna, anticipoFolioExterno: PAYLOAD_BASE.folioExterno,
      toObject: () => ({ _id: 'a1', correlacionAutomatica: true }),
    };
    AnticipoGenerado.create.mockResolvedValue(creado);

    await registrarAnticipoGenerado(PAYLOAD_BASE);

    expect(bankService.setErpIds).toHaveBeenCalledTimes(1);
    const [movId, erpLinksFinales] = bankService.setErpIds.mock.calls[0];
    expect(movId).toBe('mov-1');
    expect(erpLinksFinales).toContainEqual({ erpId: 'CXC-ORIGINAL', total: 1000 }); // NO se tocó
    expect(erpLinksFinales.find(l => l.erpId === PAYLOAD_BASE.id)).toMatchObject({
      saldoActual: PAYLOAD_BASE.total, saldoPagadoTotal: PAYLOAD_BASE.total, origen: 'anticipo',
    });
  });

  test('un fallo al vincular el erpLink NO impide que el AnticipoGenerado quede creado con correlacionAutomatica:true', async () => {
    mockFindChain([fakeCR()]);
    bankService.setErpIds.mockRejectedValue(new Error('Mongo caído'));
    const creado = {
      _id: 'a1', anticipoIdErp: PAYLOAD_BASE.id, monto: PAYLOAD_BASE.total,
      toObject: () => ({ _id: 'a1', correlacionAutomatica: true }),
    };
    AnticipoGenerado.create.mockResolvedValue(creado);

    const resultado = await registrarAnticipoGenerado(PAYLOAD_BASE);

    expect(resultado.correlacionAutomatica).toBe(true);
    expect(logger.error).toHaveBeenCalled();
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

// reconciliarAnticiposPendientes — Fase 2 (2026-09-24): job de respaldo para la
// carrera de tiempos real (webhook de Kore llega antes que Numo termine su
// propio identificar()) confirmada con datos de Test. _resolverCorrelacion() se
// reusa TAL CUAL (ya probada arriba) — acá solo se prueba el reintento sobre
// AnticipoGenerado y el aislamiento por documento.
describe('reconciliarAnticiposPendientes', () => {
  function anticipoPendiente(overrides = {}) {
    return {
      _id: 'ant-1',
      anticipoIdErp: 'kore-anticipo-pend-1',
      monto: 250,
      anticipoSerieExterna: 'OPA',
      anticipoFolioExterno: '00370',
      origenCuentaIdErp: PAYLOAD_BASE.origenCuentaId,
      fechaCreacionKore: new Date('2026-09-10T15:23:10.675Z'),
      motivoSinCorrelacion: `No se encontró ninguna solicitud de cobro identificada con cxcs.erpId=${PAYLOAD_BASE.origenCuentaId}.`,
      ...overrides,
    };
  }

  function mockPendientesChain(result) {
    AnticipoGenerado.find.mockReturnValue({ lean: jest.fn().mockResolvedValue(result) });
  }

  beforeEach(() => {
    AnticipoGenerado.updateOne.mockResolvedValue({});
  });

  test('consulta con correlacionAutomatica:false, solicitudCobroId:null y createdAt >= ahora-24h', async () => {
    mockPendientesChain([]);
    const antesMs = Date.now();

    await reconciliarAnticiposPendientes();

    const filtro = AnticipoGenerado.find.mock.calls[0][0];
    expect(filtro.correlacionAutomatica).toBe(false);
    expect(filtro.solicitudCobroId).toBeNull();
    const cutoffEsperado = antesMs - 24 * 60 * 60 * 1000;
    expect(Math.abs(filtro.createdAt.$gte.getTime() - cutoffEsperado)).toBeLessThan(1000);
  });

  test('ahora SÍ encuentra match: actualiza el documento (solicitud, movimientos, correlacionAutomatica) y emite el socket', async () => {
    mockPendientesChain([anticipoPendiente()]);
    mockFindChain([fakeCR()]);
    movimientosDe.mockReturnValue(['mov-1']);

    await reconciliarAnticiposPendientes();

    expect(AnticipoGenerado.updateOne).toHaveBeenCalledWith(
      { _id: 'ant-1' },
      { $set: { solicitudCobroId: 'cr-1', bankMovementIds: ['mov-1'], correlacionAutomatica: true, motivoSinCorrelacion: null } },
    );
    expect(emitToAll).toHaveBeenCalledWith('collection-request:anticipo-generado', { anticipoId: 'ant-1' });
  });

  test('ahora SÍ encuentra match: también vincula el erpLink del anticipo en el/los BankMovement (bankService.setErpIds)', async () => {
    mockPendientesChain([anticipoPendiente()]);
    mockFindChain([fakeCR()]);
    movimientosDe.mockReturnValue(['mov-1']);
    mockBankMovementFindById([{ erpId: 'CXC-ORIGINAL', total: 1000 }]);

    await reconciliarAnticiposPendientes();

    expect(bankService.setErpIds).toHaveBeenCalledTimes(1);
    const erpLinksFinales = bankService.setErpIds.mock.calls[0][1];
    expect(erpLinksFinales).toContainEqual({ erpId: 'CXC-ORIGINAL', total: 1000 }); // link de la CxC, intacto
    expect(erpLinksFinales.find(l => l.erpId === 'kore-anticipo-pend-1')).toMatchObject({
      saldoActual: 250, saldoPagadoTotal: 250, total: 250, origen: 'anticipo',
    });
  });

  test('sigue sin match (mismo motivo): no toca el documento ni emite', async () => {
    mockPendientesChain([anticipoPendiente()]);
    mockFindChain([]); // 0 candidatas -> mismo texto de motivo que ya tenía guardado

    await reconciliarAnticiposPendientes();

    expect(AnticipoGenerado.updateOne).not.toHaveBeenCalled();
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('el motivo cambia de "no encontrada" a "ambiguo": actualiza SOLO motivoSinCorrelacion, sin emitir', async () => {
    mockPendientesChain([anticipoPendiente()]); // motivo guardado: "No se encontró..."
    const c1 = fakeCR({ _id: 'cr-1', resueltoAt: '2026-09-10T16:00:00.000Z' });
    const c2 = fakeCR({ _id: 'cr-2', resueltoAt: '2026-09-10T17:00:00.000Z' });
    mockFindChain([c2, c1]); // 2 candidatas, ambas posteriores al anticipo -> ambiguo

    await reconciliarAnticiposPendientes();

    expect(AnticipoGenerado.updateOne).toHaveBeenCalledWith(
      { _id: 'ant-1' },
      { $set: { motivoSinCorrelacion: expect.stringMatching(/ambiguo/i) } },
    );
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('un documento que falla no frena el procesamiento de los demás del mismo batch', async () => {
    mockPendientesChain([anticipoPendiente({ _id: 'ant-1' }), anticipoPendiente({ _id: 'ant-2' })]);
    movimientosDe.mockReturnValue(['mov-x']);

    let llamada = 0;
    CollectionRequest.find.mockImplementation(() => {
      llamada += 1;
      if (llamada === 1) {
        return { sort: jest.fn().mockReturnThis(), lean: jest.fn().mockRejectedValue(new Error('Mongo caído')) };
      }
      return { sort: jest.fn().mockReturnThis(), lean: jest.fn().mockResolvedValue([fakeCR({ _id: 'cr-2' })]) };
    });

    await expect(reconciliarAnticiposPendientes()).resolves.not.toThrow();

    expect(AnticipoGenerado.updateOne).toHaveBeenCalledTimes(1);
    expect(AnticipoGenerado.updateOne).toHaveBeenCalledWith(
      { _id: 'ant-2' },
      expect.objectContaining({ $set: expect.objectContaining({ solicitudCobroId: 'cr-2' }) }),
    );
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(logger.error.mock.calls[0][0]).toContain('ant-1');
  });
});

// _vincularAnticipoAlDeposito — helper compartido (2026-09-24): agrega un erpLink
// automático al depósito bancario cuando un anticipo se correlaciona (recién
// registrado o vía reconciliación), reusando setErpIds()/aplicarLogicaErp() ya
// existentes. Mismo camino que un humano vinculando a mano desde el modal ERP.
describe('_vincularAnticipoAlDeposito', () => {
  function anticipoDoc(overrides = {}) {
    return {
      anticipoIdErp: 'kore-anticipo-1',
      anticipoSerieExterna: 'OPA',
      anticipoFolioExterno: '00362',
      monto: 536.17,
      ...overrides,
    };
  }

  test('agrega el erpLink del anticipo SIN tocar el link de la CxC ya existente', async () => {
    movimientosDe.mockReturnValue(['mov-1']);
    mockBankMovementFindById([{ erpId: 'CXC-ORIGINAL', total: 1000 }]);

    await _vincularAnticipoAlDeposito(anticipoDoc(), fakeCR());

    expect(BankMovement.findById).toHaveBeenCalledWith('mov-1');
    const [movId, erpLinksFinales, user] = bankService.setErpIds.mock.calls[0];
    expect(movId).toBe('mov-1');
    expect(erpLinksFinales).toHaveLength(2);
    expect(erpLinksFinales[0]).toEqual({ erpId: 'CXC-ORIGINAL', total: 1000 }); // preservado byte a byte
    expect(erpLinksFinales[1]).toEqual({
      erpId: 'kore-anticipo-1',
      saldoActual: 536.17,
      saldoPagado: null,
      saldoPagadoTotal: 536.17,
      folioFiscal: null,
      total: 536.17,
      serie: 'OPA',
      folioExterno: '00362',
      tipoPago: null,
      desglosePorFormaPago: [],
      origen: 'anticipo',
    });
    expect(user).toMatchObject({ role: 'admin' }); // resuelve el permiso de setErpIds vía el wildcard
  });

  test('upsert por erpId: llamarlo de nuevo para el MISMO anticipo reemplaza la entrada, no la duplica', async () => {
    movimientosDe.mockReturnValue(['mov-1']);
    mockBankMovementFindById([
      { erpId: 'CXC-ORIGINAL', total: 1000 },
      { erpId: 'kore-anticipo-1', total: 999, saldoActual: 999 }, // entrada vieja del mismo anticipo
    ]);

    await _vincularAnticipoAlDeposito(anticipoDoc(), fakeCR());

    const erpLinksFinales = bankService.setErpIds.mock.calls[0][1];
    const delAnticipo = erpLinksFinales.filter(l => l.erpId === 'kore-anticipo-1');
    expect(delAnticipo).toHaveLength(1); // no duplicó
    expect(delAnticipo[0].saldoActual).toBe(536.17); // ganó la versión nueva, no la vieja (999)
    expect(erpLinksFinales.some(l => l.erpId === 'CXC-ORIGINAL')).toBe(true); // preservado
  });

  test('multi-bank-movement: vincula el erpLink en CADA movimiento de la solicitud', async () => {
    movimientosDe.mockReturnValue(['mov-1', 'mov-2']);
    mockBankMovementFindById([]);

    await _vincularAnticipoAlDeposito(anticipoDoc(), fakeCR());

    expect(bankService.setErpIds).toHaveBeenCalledTimes(2);
    expect(bankService.setErpIds.mock.calls[0][0]).toBe('mov-1');
    expect(bankService.setErpIds.mock.calls[1][0]).toBe('mov-2');
  });

  test('un fallo en setErpIds se loguea y NO propaga', async () => {
    movimientosDe.mockReturnValue(['mov-1']);
    bankService.setErpIds.mockRejectedValue(new Error('Mongo caído'));

    await expect(_vincularAnticipoAlDeposito(anticipoDoc(), fakeCR())).resolves.not.toThrow();
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(logger.error.mock.calls[0][0]).toContain('kore-anticipo-1');
  });

  test('un movimiento roto (findById falla) no impide vincular los demás del mismo anticipo', async () => {
    movimientosDe.mockReturnValue(['mov-1', 'mov-2']);
    let llamada = 0;
    BankMovement.findById.mockImplementation(() => {
      llamada += 1;
      if (llamada === 1) {
        return { select: jest.fn().mockReturnThis(), lean: jest.fn().mockRejectedValue(new Error('boom')) };
      }
      return { select: jest.fn().mockReturnThis(), lean: jest.fn().mockResolvedValue({ erpLinks: [] }) };
    });

    await _vincularAnticipoAlDeposito(anticipoDoc(), fakeCR());

    expect(bankService.setErpIds).toHaveBeenCalledTimes(1);
    expect(bankService.setErpIds.mock.calls[0][0]).toBe('mov-2');
    expect(logger.error).toHaveBeenCalledTimes(1);
  });
});
