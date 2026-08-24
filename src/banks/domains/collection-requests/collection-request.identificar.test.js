'use strict';

// collection-request.identificar.test.js — identificar(): capa de integración
// (design's "Testing Strategy" table). No hay Mongo/Kore reales disponibles en
// este repo (ver sdd/numo-backend/testing-capabilities) — se mockean los
// límites de I/O (2 modelos Mongoose, bankService, koreCaja, erpRoutes,
// mongo-tx, socket, logger). 8 mocks para una función de orquestación que
// cruza 2 colecciones Mongo + 1 API externa (Kore) + un wrapper transaccional
// es una excepción deliberada a la regla de higiene de mocks (strict-tdd.md:
// "OR move the test to integration/E2E layer where real dependencies exist")
// — es EXACTAMENTE la capa de integración que el propio design prescribe para
// esta pieza, no hay forma de bajar el conteo sin infraestructura de Mongo/
// Kore de prueba que no existe en este repo.

jest.mock('./CollectionRequest.model');
jest.mock('../banks/BankMovement.model');
jest.mock('../banks/bank.service');
jest.mock('../erp/kore-caja.service');
jest.mock('../erp/erp.routes');
jest.mock('../../shared/utils/mongo-tx');
jest.mock('../../shared/socket');
jest.mock('../../shared/utils/logger');

const CollectionRequest = require('./CollectionRequest.model');
const BankMovement      = require('../banks/BankMovement.model');
const bankService       = require('../banks/bank.service');
const koreCaja          = require('../erp/kore-caja.service');
const erpRoutes         = require('../erp/erp.routes');
const { conTransaccion } = require('../../shared/utils/mongo-tx');
const { emitToAll, emitToBanco } = require('../../shared/socket');

const service = require('./collection-request.service');

// ── Helpers de fixtures ──────────────────────────────────────────────────────

class KoreCajaError extends Error {}
koreCaja.KoreCajaError = KoreCajaError;
koreCaja.esErrorYaEnEstatus = jest.fn(() => false);

function formaPago(id, descripcion, importe, extra = {}) {
  return {
    _id: id,
    formaPagoId: `fp-${id}`,
    formaPagoDescripcion: descripcion,
    importe,
    toObject() { return { _id: id, formaPagoId: `fp-${id}`, formaPagoDescripcion: descripcion, importe, ...extra }; },
    ...extra,
  };
}

function makeCr({ id = 'cr-1', formasPago, cxcs, monto, conceptoId = 'concepto-1', status = 'pendiente' }) {
  return {
    _id: id,
    solicitudIdErp: 'SOL-1',
    solicitanteUserId: 'cajero-1',
    conceptoId,
    status,
    formasPago,
    cxcs,
    monto,
    save: jest.fn().mockResolvedValue(undefined),
  };
}

// `fecha` (2026-08-14): valor por default fijo para que fecha_real_pago sea
// determinístico en los tests que no la sobreescriben — mismo criterio de
// fixtures fijas que el resto del archivo (folio/numeroAutorizacion).
function bankMovement(id, overrides = {}) {
  return { _id: id, folio: `F-${id}`, banco: 'BBVA', numeroAutorizacion: `AUT-${id}`, erpLinks: [], fecha: new Date('2026-08-01T00:00:00.000Z'), ...overrides };
}

function setupHappyKore() {
  koreCaja.obtenerSesionCaja.mockResolvedValue({ sesionId: 'sesion-1', koreToken: 'token-1' });
  koreCaja.obtenerCuentasKore.mockResolvedValue([{ id: 'CXC-1', saldoActual: 100000, total: 100000 }]);
  koreCaja.obtenerTokenKore.mockResolvedValue('token-revisor');
  koreCaja.actualizarEstatusSolicitud.mockResolvedValue({ ok: true });
  koreCaja.listarFormasPago.mockResolvedValue([{ id: 'fp-f1', claveSAT: '03' }, { id: 'fp-f2', claveSAT: '03' }]);
  koreCaja.listarBancos.mockResolvedValue([{ id: 'banco-bbva', claveBanco: 'BBVA', descripcion: 'BBVA' }]);
  koreCaja.aplicarSolicitudOperacion.mockResolvedValue({ ok: true, folio: 'KORE-1' });
  erpRoutes._rangoDesdeFollo.mockReturnValue(null); // sin rescate de folioFiscal en estos tests
  bankService.setErpIds.mockImplementation(async (id, erpLinks, user, opts = {}) => ({
    _id: id, banco: 'BBVA', erpLinks, erpIds: erpLinks.map(l => l.erpId), status: 'identificado',
  }));
}

beforeEach(() => {
  jest.clearAllMocks();
  koreCaja.esErrorYaEnEstatus = jest.fn(() => false);
  conTransaccion.mockImplementation((fn) => fn(null)); // standalone: sin sesión real
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  console.log.mockRestore();
  console.warn.mockRestore();
});

describe('identificar() — N=1 (atajo escalar, comportamiento sin cambios)', () => {
  test('1 sola BankMovement.find, 1 solo buildErpLinksParaCobro/setErpIds, 1 sola llamada a Kore', async () => {
    const f1 = formaPago('f1', 'Transferencia', 100000);
    const cr = makeCr({ formasPago: [f1], cxcs: [{ erpId: 'CXC-1', total: 100000 }], monto: 100000 });
    CollectionRequest.findById.mockResolvedValue(cr);
    BankMovement.find.mockResolvedValue([bankMovement('mov-1')]);
    setupHappyKore();

    const resultado = await service.identificar('cr-1', { bankMovementId: 'mov-1' }, { _id: 'user-1', nombre: 'Ana' });

    expect(BankMovement.find).toHaveBeenCalledTimes(1);
    expect(koreCaja.aplicarSolicitudOperacion).toHaveBeenCalledTimes(1);
    expect(bankService.setErpIds).toHaveBeenCalledTimes(1);
    expect(cr.status).toBe('identificada');
    expect(resultado.reconciliacion).toBeDefined();
    expect(emitToAll).toHaveBeenCalledWith('collection-request:updated', expect.objectContaining({ _id: 'cr-1' }));

    // 2026-08-14: fecha_real_pago viaja como campo hermano de FormaPagoID, con
    // la fecha del movimiento asignado (2026-08-01) + el ajuste de offset.
    // 2026-08-20: Kore renombró el campo a snake_case y cambió el offset a
    // +10hrs (antes fechaRealPago/+7hrs) — solo para Solicitudes de Cobro.
    const [, , , datosAdicionales, fechaRealPagoRaiz] = koreCaja.aplicarSolicitudOperacion.mock.calls[0];
    expect(datosAdicionales[0].fecha_real_pago).toBe('2026-08-01T10:00:00Z');
    // 2026-08-20 (mismo día): Kore también exige fecha_real_pago A NIVEL RAÍZ
    // del body (rechazo real: "obligatorio para Tipo=REVISION_CONTABLE").
    expect(fechaRealPagoRaiz).toBe('2026-08-01T10:00:00Z');
  });
});

describe('identificar() — N=2 (asignaciones explícitas, multi-bank-movement)', () => {
  test('EXACTAMENTE 1 llamada a aplicarSolicitudOperacion para N=2 movimientos, con Aut/Numo propios de cada uno', async () => {
    const f1 = formaPago('f1', 'Transferencia', 60000);
    const f2 = formaPago('f2', 'Transferencia', 40000);
    const cr = makeCr({ formasPago: [f1, f2], cxcs: [{ erpId: 'CXC-1', total: 100000 }], monto: 100000 });
    CollectionRequest.findById.mockResolvedValue(cr);
    // Fechas distintas por movimiento (2026-08-14): confirma que cada forma de
    // pago manda SU PROPIA fecha_real_pago, no una fecha global de la solicitud.
    BankMovement.find.mockResolvedValue([
      bankMovement('mov-A', { fecha: new Date('2026-08-10T00:00:00.000Z') }),
      bankMovement('mov-B', { fecha: new Date('2026-08-12T00:00:00.000Z') }),
    ]);
    setupHappyKore();

    await service.identificar(
      'cr-1',
      { asignaciones: [{ formaPagoDocId: 'f1', bankMovementId: 'mov-A' }, { formaPagoDocId: 'f2', bankMovementId: 'mov-B' }] },
      { _id: 'user-1', nombre: 'Ana' },
    );

    // La regla central del spec/design: NUNCA dos llamadas a Kore, sin importar N.
    expect(koreCaja.aplicarSolicitudOperacion).toHaveBeenCalledTimes(1);
    const [, , , datosAdicionales, fechaRealPagoRaiz] = koreCaja.aplicarSolicitudOperacion.mock.calls[0];
    expect(datosAdicionales).toHaveLength(2);
    expect(datosAdicionales.find(d => d.FormaPagoID === 'fp-f1').DatosAdicionales).toEqual(
      expect.arrayContaining([{ Nombre: 'Aut', Valor: 'F-mov-A' }, { Nombre: 'Numo', Valor: 'AUT-mov-A' }]),
    );
    expect(datosAdicionales.find(d => d.FormaPagoID === 'fp-f2').DatosAdicionales).toEqual(
      expect.arrayContaining([{ Nombre: 'Aut', Valor: 'F-mov-B' }, { Nombre: 'Numo', Valor: 'AUT-mov-B' }]),
    );
    expect(datosAdicionales.find(d => d.FormaPagoID === 'fp-f1').fecha_real_pago).toBe('2026-08-10T10:00:00Z');
    expect(datosAdicionales.find(d => d.FormaPagoID === 'fp-f2').fecha_real_pago).toBe('2026-08-12T10:00:00Z');
    // 2026-08-20: fecha_real_pago de raíz usa el PRIMER movimiento asignado
    // (mov-A, 08-10) — decisión confirmada con el usuario, el desglose real
    // por depósito sigue viviendo en cada elemento del arreglo (arriba).
    expect(fechaRealPagoRaiz).toBe('2026-08-10T10:00:00Z');

    // Dos movimientos distintos -> dos setErpIds, uno por grupo.
    expect(bankService.setErpIds).toHaveBeenCalledTimes(2);
    expect(bankService.setErpIds.mock.calls.map(c => c[0]).sort()).toEqual(['mov-A', 'mov-B']);

    // Ambas formasPago persisten su propio bankMovementId.
    expect(cr.formasPago.find(f => f._id === 'f1').bankMovementId).toBe('mov-A');
    expect(cr.formasPago.find(f => f._id === 'f2').bankMovementId).toBe('mov-B');
    expect(cr.save).toHaveBeenCalledTimes(1);

    // El aviso en tiempo real ('collection-request:updated') lleva AMBOS
    // movimientos distintos, no solo el primero.
    const evento = emitToAll.mock.calls.find(c => c[0] === 'collection-request:updated')[1];
    expect(evento.bankMovements.map(m => m._id)).toEqual(['mov-A', 'mov-B']);
    expect(evento.bankMovementId._id).toBe('mov-A'); // compat: primer movimiento
  });
});

describe('identificar() — Depósito en efectivo manda "Num Recibo" en vez de Aut/Numo', () => {
  test('DatosAdicionales trae un solo tag Num Recibo con el folio consecutivo de Numo, sin BancoID', async () => {
    const f1 = formaPago('f1', 'Depósito en efectivo', 100000);
    const cr = makeCr({ formasPago: [f1], cxcs: [{ erpId: 'CXC-1', total: 100000 }], monto: 100000 });
    CollectionRequest.findById.mockResolvedValue(cr);
    BankMovement.find.mockResolvedValue([bankMovement('mov-1')]);
    setupHappyKore();
    // Depósito en efectivo no tiene claveSAT propia en Kore (se reporta como
    // Efectivo, '01') — se distingue por nombre, igual que el resto del código.
    koreCaja.listarFormasPago.mockResolvedValue([{ id: 'fp-f1', claveSAT: '01', nombre: 'Depósito en efectivo' }]);

    await service.identificar('cr-1', { bankMovementId: 'mov-1' }, { _id: 'user-1', nombre: 'Ana' });

    expect(koreCaja.aplicarSolicitudOperacion).toHaveBeenCalledTimes(1);
    const [, , , datosAdicionales, fechaRealPagoRaiz] = koreCaja.aplicarSolicitudOperacion.mock.calls[0];
    expect(datosAdicionales).toHaveLength(1);
    expect(datosAdicionales[0].BancoID).toBeUndefined();
    expect(datosAdicionales[0].DatosAdicionales).toEqual([{ Nombre: 'Num Recibo', Valor: 'F-mov-1' }]);
    // 2026-08-14: fecha_real_pago se manda igual para depósito en efectivo, sin
    // condición de tipo (a diferencia de BancoID/DatosAdicionales).
    expect(datosAdicionales[0].fecha_real_pago).toBe('2026-08-01T10:00:00Z');
    expect(fechaRealPagoRaiz).toBe('2026-08-01T10:00:00Z');
  });
});

describe('identificar() — otra forma de pago (cheque/saldo a favor) sin BancoID ni DatosAdicionales', () => {
  test('fecha_real_pago SÍ se manda aunque no haya BancoID ni DatosAdicionales', async () => {
    const f1 = formaPago('f1', 'Cheque', 100000);
    const cr = makeCr({ formasPago: [f1], cxcs: [{ erpId: 'CXC-1', total: 100000 }], monto: 100000 });
    CollectionRequest.findById.mockResolvedValue(cr);
    BankMovement.find.mockResolvedValue([bankMovement('mov-1')]);
    setupHappyKore();
    // Cheque: ni claveSAT '03' (transferencia) ni "DEPOSITO EFECTIVO" por
    // nombre — no dispara ni BancoID ni DatosAdicionales.
    koreCaja.listarFormasPago.mockResolvedValue([{ id: 'fp-f1', claveSAT: '02', nombre: 'Cheque' }]);

    await service.identificar('cr-1', { bankMovementId: 'mov-1' }, { _id: 'user-1', nombre: 'Ana' });

    expect(koreCaja.aplicarSolicitudOperacion).toHaveBeenCalledTimes(1);
    const [, , , datosAdicionales, fechaRealPagoRaiz] = koreCaja.aplicarSolicitudOperacion.mock.calls[0];
    expect(datosAdicionales).toHaveLength(1);
    expect(datosAdicionales[0].BancoID).toBeUndefined();
    expect(datosAdicionales[0].DatosAdicionales).toBeUndefined();
    expect(datosAdicionales[0].fecha_real_pago).toBe('2026-08-01T10:00:00Z');
    expect(fechaRealPagoRaiz).toBe('2026-08-01T10:00:00Z');
  });
});

describe('identificar() — todo-o-nada (spec: rechazo antes de Kore/Mongo)', () => {
  test('1 de 2 formasPago sin asignar -> BadRequestError, NINGÚN Kore/Mongo tocado', async () => {
    const f1 = formaPago('f1', 'Transferencia', 60000);
    const f2 = formaPago('f2', 'Efectivo', 40000);
    const cr = makeCr({ formasPago: [f1, f2], cxcs: [{ erpId: 'CXC-1', total: 100000 }], monto: 100000 });
    CollectionRequest.findById.mockResolvedValue(cr);
    setupHappyKore();

    await expect(
      service.identificar('cr-1', { asignaciones: [{ formaPagoDocId: 'f1', bankMovementId: 'mov-A' }] }, { _id: 'user-1' }),
    ).rejects.toThrow(/Faltan 1 de 2/);

    expect(BankMovement.find).not.toHaveBeenCalled();
    expect(koreCaja.obtenerSesionCaja).not.toHaveBeenCalled();
    expect(koreCaja.aplicarSolicitudOperacion).not.toHaveBeenCalled();
    expect(bankService.setErpIds).not.toHaveBeenCalled();
  });
});

describe('identificar() — reconciliación advisory (nunca bloquea)', () => {
  test('abono parcial: identificar() completa OK y devuelve reconciliacion.mensaje con el faltante', async () => {
    const f1 = formaPago('f1', 'Transferencia', 70000);
    const cr = makeCr({ formasPago: [f1], cxcs: [{ erpId: 'CXC-1', total: 100000 }], monto: 100000 });
    CollectionRequest.findById.mockResolvedValue(cr);
    BankMovement.find.mockResolvedValue([bankMovement('mov-1', { deposito: 70000 })]);
    setupHappyKore();

    const resultado = await service.identificar('cr-1', { bankMovementId: 'mov-1' }, { _id: 'user-1' });

    expect(cr.status).toBe('identificada'); // NUNCA bloquea
    expect(resultado.reconciliacion.cubreParcial).toBe(true);
    expect(resultado.reconciliacion.mensaje).toBe('cubre $70,000 de $100,000 — quedan $30,000 pendientes');
  });
});

// Bug real 2026-08-24: el filtro de BankMovement.find() usaba `uuidXML: null`
// (= "sin NINGÚN erpLink con folioFiscal"), lo que bloqueaba reutilizar un
// movimiento con un abono parcial previo para agregar OTRO abono parcial
// legítimo contra la MISMA CxC vía una solicitud de cobro distinta. Regla de
// negocio confirmada: un movimiento sigue siendo candidato mientras su
// `status` sea 'no_identificado' o 'reclasificado' — 'identificado'/'otros' lo
// excluyen. Estos tests fijan el filtro real usado (antes no había cobertura
// de este guard en absoluto).
describe('identificar() — guard de disponibilidad del movimiento (status, no uuidXML)', () => {
  test('BankMovement.find() filtra por status no_identificado/reclasificado, nunca por uuidXML', async () => {
    const f1 = formaPago('f1', 'Transferencia', 10000);
    const cr = makeCr({ formasPago: [f1], cxcs: [{ erpId: 'CXC-1', total: 220336 }], monto: 10000 });
    CollectionRequest.findById.mockResolvedValue(cr);
    // Movimiento con un erpLink previo (abono parcial de otra solicitud) — uuidXML
    // ya no-nulo en un escenario real, pero eso no debe ser lo que se filtra.
    BankMovement.find.mockResolvedValue([bankMovement('mov-1', {
      uuidXML: 'FOLIO-FISCAL-EXISTENTE',
      erpLinks: [{ erpId: 'CXC-1', saldoPagado: 50000, saldoPagadoTotal: 50000, folioFiscal: 'FOLIO-FISCAL-EXISTENTE' }],
    })]);
    setupHappyKore();

    await service.identificar('cr-1', { bankMovementId: 'mov-1' }, { _id: 'user-1' });

    expect(BankMovement.find).toHaveBeenCalledWith({
      _id: { $in: ['mov-1'] },
      status: { $in: ['no_identificado', 'reclasificado'] },
    });
  });

  test('movimiento excluido por la query (ej. status identificado/otros) -> NotFoundError', async () => {
    const f1 = formaPago('f1', 'Transferencia', 10000);
    const cr = makeCr({ formasPago: [f1], cxcs: [{ erpId: 'CXC-1', total: 100000 }], monto: 10000 });
    CollectionRequest.findById.mockResolvedValue(cr);
    // Simula lo que Mongo devolvería para un movimiento con status 'identificado'/'otros':
    // no matchea el filtro, así que el find real ya lo excluye del arreglo.
    BankMovement.find.mockResolvedValue([]);
    setupHappyKore();

    await expect(
      service.identificar('cr-1', { bankMovementId: 'mov-1' }, { _id: 'user-1' }),
    ).rejects.toThrow('Movimiento bancario');

    expect(koreCaja.obtenerSesionCaja).not.toHaveBeenCalled();
  });
});

describe('identificar() — abort post-Kore (D4: inconsistenciaPostKore, NO reintentar)', () => {
  test('Kore acepta pero el 2do setErpIds falla -> marca inconsistenciaPostKore y NO reintenta', async () => {
    const f1 = formaPago('f1', 'Transferencia', 60000);
    const f2 = formaPago('f2', 'Transferencia', 40000);
    const cr = makeCr({ formasPago: [f1, f2], cxcs: [{ erpId: 'CXC-1', total: 100000 }], monto: 100000 });
    CollectionRequest.findById.mockResolvedValue(cr);
    CollectionRequest.updateOne = jest.fn().mockResolvedValue({ acknowledged: true });
    BankMovement.find.mockResolvedValue([bankMovement('mov-A'), bankMovement('mov-B')]);
    setupHappyKore();
    bankService.setErpIds
      .mockResolvedValueOnce({ _id: 'mov-A', banco: 'BBVA', erpLinks: [], erpIds: [] })
      .mockRejectedValueOnce(new Error('Mongo write failed mid-transaction'));

    await expect(
      service.identificar(
        'cr-1',
        { asignaciones: [{ formaPagoDocId: 'f1', bankMovementId: 'mov-A' }, { formaPagoDocId: 'f2', bankMovementId: 'mov-B' }] },
        { _id: 'user-1' },
      ),
    ).rejects.toThrow(/NO reintentar/);

    // Kore YA aceptó el cobro antes de que el segundo setErpIds fallara — el
    // punto entero de D4 es que este hecho es irreversible.
    expect(koreCaja.aplicarSolicitudOperacion).toHaveBeenCalledTimes(1);
    expect(CollectionRequest.updateOne).toHaveBeenCalledWith(
      { _id: 'cr-1' },
      expect.objectContaining({
        inconsistenciaPostKore: expect.objectContaining({
          movimientosPendientes: ['mov-A', 'mov-B'],
        }),
      }),
    );
    // cr.save() nunca se completa dentro del bloque que abortó.
    expect(cr.save).not.toHaveBeenCalled();
  });

  // Incidente real 2026-08-19: un error de RED (sin `response`, ej. timeout) al
  // llamar aplicarSolicitudOperacion dejaba la CxC cobrada en Kore SIN ningún
  // rastro en Numo — ni inconsistenciaPostKore ni bankMovementId — porque el
  // catch de ese paso solo reconocía KoreCajaError (Kore respondió) y relanzaba
  // cualquier otro error tal cual. Este test cubre el fix: un error de red en
  // ESE paso específico también debe marcar inconsistenciaPostKore.
  test('aplicarSolicitudOperacion falla por error de RED (sin response, no KoreCajaError) -> marca inconsistenciaPostKore igual', async () => {
    const f1 = formaPago('f1', 'Transferencia', 60000);
    const cr = makeCr({ formasPago: [f1], cxcs: [{ erpId: 'CXC-1', total: 100000 }], monto: 100000 });
    CollectionRequest.findById.mockResolvedValue(cr);
    CollectionRequest.updateOne = jest.fn().mockResolvedValue({ acknowledged: true });
    BankMovement.find.mockResolvedValue([bankMovement('mov-A')]);
    setupHappyKore();
    const errorDeRed = new Error('timeout of 15000ms exceeded');
    errorDeRed.code = 'ECONNABORTED'; // sin `response` — no es instancia de KoreCajaError
    koreCaja.aplicarSolicitudOperacion.mockRejectedValue(errorDeRed);

    await expect(
      service.identificar('cr-1', { bankMovementId: 'mov-A' }, { _id: 'user-1' }),
    ).rejects.toThrow(/no se pudo confirmar si Kore aplicó el cobro/i);

    expect(koreCaja.aplicarSolicitudOperacion).toHaveBeenCalledTimes(1);
    // Nunca se llega a setErpIds/cr.save — el catch corta antes de la transacción.
    expect(bankService.setErpIds).not.toHaveBeenCalled();
    expect(cr.save).not.toHaveBeenCalled();
    expect(CollectionRequest.updateOne).toHaveBeenCalledWith(
      { _id: 'cr-1' },
      expect.objectContaining({
        inconsistenciaPostKore: expect.objectContaining({
          movimientosPendientes: ['mov-A'],
          mensaje: expect.stringContaining('Error de red'),
        }),
      }),
    );
  });
});
