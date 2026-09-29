'use strict';

// netpay-reporte.service.test.js — Implementación 1 de "Netpay: carga manual del reporte
// como fuente de verdad". A diferencia del matching automático (netpay-match-confirm.service),
// acá la conciliación es 1:1 (un reporte == a lo sumo UN BankMovement) — el reporte YA trae
// el monto exacto depositado por Netpay, sin la comisión errónea de Kore de por medio.
// parseNetpayReporte se mockea (tiene sus propios tests unitarios en
// netpay-reporte-parser.service.test.js, incluida contra los 2 archivos reales del repo).
jest.mock('../banks/BankMovement.model');
jest.mock('./NetpayReporte.model');
jest.mock('./NetpayMatch.model');
jest.mock('./NetpayFolioRegistro.model');
jest.mock('./netpay-reporte-parser.service');
jest.mock('./kore-caja.service', () => ({ buscarTransaccionesNetpay: jest.fn() }));
jest.mock('./netpay-match.service', () => ({
  _ventanaDiasNetpay: jest.fn(),
  _montosIguales: jest.requireActual('./netpay-match.service')._montosIguales,
}));
jest.mock('../../../shared/services/global-config.service');
jest.mock('../../shared/socket', () => ({ emitToBanco: jest.fn(), emitToAll: jest.fn() }));
jest.mock('../banks/bank.service', () => {
  const real = jest.requireActual('../banks/bank.service');
  return { setErpIds: jest.fn(), ERP_TOLERANCE: real.ERP_TOLERANCE, registerErpUnlinkHook: jest.fn() };
});

const BankMovement = require('../banks/BankMovement.model');
const NetpayReporte = require('./NetpayReporte.model');
const NetpayMatch = require('./NetpayMatch.model');
const NetpayFolioRegistro = require('./NetpayFolioRegistro.model');
const { parseNetpayReporte } = require('./netpay-reporte-parser.service');
const { buscarTransaccionesNetpay } = require('./kore-caja.service');
const { _ventanaDiasNetpay } = require('./netpay-match.service');
const { setErpIds } = require('../banks/bank.service');
const { emitToBanco, emitToAll } = require('../../shared/socket');
const { BadRequestError, NotFoundError, ConflictError } = require('../../shared/errors/AppError');
const {
  cargarReporte, listar, obtenerDetalle, obtenerPorMovimiento, buscarCandidatos,
  resolverReporte, rechazarReporte, consultarFolioKore, consultarFoliosPendientes, evaluarReporte,
  eliminarReporte, restaurarReporte,
} = require('./netpay-reporte.service');

const USER = { _id: 'user-1', nombre: 'Ana' };

function fakeFind(result) {
  return { lean: jest.fn().mockResolvedValue(result) };
}

function parsedFixture(overrides = {}) {
  return {
    claveRastreo: 'CLAVE-1',
    cuentaDeposito: '0126100010',
    fechaMovimiento: new Date('2026-09-25T00:00:00.000Z'),
    periodoDesde: new Date('2026-09-25T00:00:00.000Z'),
    periodoHasta: new Date('2026-09-25T00:00:00.000Z'),
    montoDepositoTotal: 1000,
    resumenVentas: { montoTransaccionado: 1050, comisiones: 40, iva: 6.4, montoDepositado: 1000 },
    folios: [{ referencia: 'F1', terminalID: 'T1' }],
    ...overrides,
  };
}

function fakeReporteRecienCreado(overrides = {}) {
  return {
    _id: 'rep-1', claveRastreo: 'CLAVE-1', montoDepositoTotal: 1000,
    folios: [{ referencia: 'F1', orderId: 'ORD-1', duplicadoDeReporteId: null }],
    estatus: 'discrepancia', motivoDiscrepancia: null, vinculo: null, movementIdConfirmado: null,
    eliminado: false,
    save: jest.fn().mockResolvedValue(undefined),
    toObject: jest.fn(function () { return { ...this }; }),
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  _ventanaDiasNetpay.mockResolvedValue(2);
  BankMovement.find = jest.fn(() => fakeFind([]));
  NetpayMatch.find = jest.fn(() => fakeFind([]));
  NetpayFolioRegistro.insertMany = jest.fn().mockResolvedValue([]);
});

// cargarReporte — netpay-matching-v2 (design.md "(a) Report present"): ya NO deja el
// reporte en 'pendiente' (ese valor no existe en el enum v2, ver NetpayReporte.model.js) —
// registra sus folios en NetpayFolioRegistro (idempotencia) y llama evaluarReporte() para
// decidir su estado automáticamente en el mismo paso.
describe('cargarReporte', () => {
  test('parser tira BadRequestError: se propaga tal cual (nunca 500)', async () => {
    parseNetpayReporte.mockRejectedValue(new BadRequestError('El archivo no contiene la hoja "Resumen"'));
    await expect(cargarReporte(Buffer.from(''), 'x.xlsx', USER)).rejects.toThrow(/hoja "Resumen"/);
  });

  test('parser tira un error NO tipado: se envuelve en BadRequestError legible', async () => {
    parseNetpayReporte.mockRejectedValue(new Error('boom interno de ExcelJS'));
    await expect(cargarReporte(Buffer.from(''), 'x.xlsx', USER)).rejects.toThrow(/Error al leer el archivo/);
  });

  test('claveRastreo ya existe: ConflictError, no llega a crear', async () => {
    parseNetpayReporte.mockResolvedValue(parsedFixture());
    NetpayReporte.findOne = jest.fn(() => fakeFind({ _id: 'existente' }));
    NetpayReporte.create = jest.fn();

    await expect(cargarReporte(Buffer.from(''), 'x.xlsx', USER)).rejects.toThrow(/Ya existe un reporte cargado/);
    expect(NetpayReporte.create).not.toHaveBeenCalled();
  });

  test('condición de carrera (índice único choca en el create pese al findOne previo): ConflictError, no un 500 crudo', async () => {
    parseNetpayReporte.mockResolvedValue(parsedFixture());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const err = new Error('E11000 duplicate key');
    err.code = 11000;
    NetpayReporte.create = jest.fn().mockRejectedValue(err);

    await expect(cargarReporte(Buffer.from(''), 'x.xlsx', USER)).rejects.toThrow(ConflictError);
  });

  test('creado en discrepancia (nunca pendiente — ese valor ya no existe en el enum v2), luego evaluarReporte decide', async () => {
    parseNetpayReporte.mockResolvedValue(parsedFixture());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    BankMovement.find = jest.fn(() => fakeFind([]));
    const creado = fakeReporteRecienCreado({ estatus: 'discrepancia' });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn().mockResolvedValue(creado);

    const { reporte, candidatos } = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(candidatos).toEqual([]);
    expect(reporte.estatus).toBe('discrepancia');
    expect(reporte.motivoDiscrepancia).toBe('sin_candidato');
    expect(NetpayReporte.create).toHaveBeenCalledWith(expect.objectContaining({
      claveRastreo: 'CLAVE-1', estatus: 'discrepancia', nombreArchivoOriginal: 'archivo.xlsx',
      cargadoPor: { userId: 'user-1', nombre: 'Ana' },
    }));
  });

  test('1 candidato BBVA cuyo monto cuadra dentro de tolerancia: auto-vincula (resuelto_por_reporte, vinculo:erp-link)', async () => {
    parseNetpayReporte.mockResolvedValue(parsedFixture({ montoDepositoTotal: 1000 }));
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const mov = { _id: 'mov-1', banco: 'BBVA', deposito: 1000.5 };
    BankMovement.find = jest.fn(() => fakeFind([mov]));
    const creado = fakeReporteRecienCreado({ montoDepositoTotal: 1000 });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn().mockResolvedValue(creado);
    setErpIds.mockResolvedValue({ _id: 'mov-1', banco: 'BBVA' });

    const { reporte, candidatos } = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(candidatos).toEqual([mov]);
    expect(reporte.estatus).toBe('resuelto_por_reporte');
    expect(reporte.vinculo).toBe('erp-link');
    expect(setErpIds).toHaveBeenCalledWith(
      'mov-1',
      [expect.objectContaining({ erpId: 'NETPAYRPT-CLAVE-1', origen: 'netpay-reporte' })],
      expect.objectContaining({ role: 'admin' }),
      { guardSinVinculos: true },
    );
  });

  test('3 folios (mismo depósito, distintas transacciones): se persisten los 3 en el arreglo folios y se registran en NetpayFolioRegistro', async () => {
    const folios = [{ referencia: 'F1', orderId: null }, { referencia: 'F2', orderId: null }, { referencia: 'F3', orderId: null }];
    parseNetpayReporte.mockResolvedValue(parsedFixture({ folios }));
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const creado = fakeReporteRecienCreado({ folios: folios.map(f => ({ ...f, duplicadoDeReporteId: null })) });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn().mockResolvedValue(creado);

    await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(NetpayReporte.create).toHaveBeenCalledWith(expect.objectContaining({ folios }));
    expect(NetpayFolioRegistro.insertMany).toHaveBeenCalledWith(
      [
        { clave: 'REF:F1', reporteId: 'rep-1', orderId: null, referencia: 'F1' },
        { clave: 'REF:F2', reporteId: 'rep-1', orderId: null, referencia: 'F2' },
        { clave: 'REF:F3', reporteId: 'rep-1', orderId: null, referencia: 'F3' },
      ],
      { ordered: false },
    );
  });

  // design.md "Idempotent folios": un folio (por orderId, o 'REF:'+referencia si no hay
  // orderId) ya registrado por OTRO reporte -> E11000 en insertMany(ordered:false) -> la
  // fila queda marcada (duplicadoDeReporteId) pero el resto del reporte sigue su curso
  // normal (nunca se descarta el reporte entero solo por esto).
  test('folio duplicado (E11000 en insertMany): marca folios[i].duplicadoDeReporteId, NO aborta el resto', async () => {
    const folios = [{ referencia: 'F1', orderId: 'ORD-1' }, { referencia: 'F2', orderId: 'ORD-2' }];
    parseNetpayReporte.mockResolvedValue(parsedFixture({ folios }));
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const creado = fakeReporteRecienCreado({
      folios: folios.map(f => ({ ...f, duplicadoDeReporteId: null })),
    });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn().mockResolvedValue(creado);

    const err = new Error('E11000 duplicate key on folio_registros');
    err.writeErrors = [{ index: 0, code: 11000 }];
    NetpayFolioRegistro.insertMany = jest.fn().mockRejectedValue(err);
    NetpayFolioRegistro.findOne = jest.fn(() => fakeFind({ reporteId: 'rep-ORIGINAL' }));

    await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(creado.folios[0].duplicadoDeReporteId).toBe('rep-ORIGINAL');
    expect(creado.folios[1].duplicadoDeReporteId).toBeNull();
    expect(creado.save).toHaveBeenCalled();
  });

  test('insertMany falla con un error NO relacionado a duplicados (ej. de red): se propaga, nunca se absorbe en silencio', async () => {
    parseNetpayReporte.mockResolvedValue(parsedFixture());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const creado = fakeReporteRecienCreado();
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn().mockResolvedValue(creado);

    const err = new Error('conexión perdida a Mongo');
    NetpayFolioRegistro.insertMany = jest.fn().mockRejectedValue(err);

    await expect(cargarReporte(Buffer.from(''), 'archivo.xlsx', USER)).rejects.toThrow('conexión perdida a Mongo');
  });
});

// evaluarReporte — netpay-matching-v2 (design.md "(a) Report present"): decide (o
// re-decide, ej. tras Reevaluar) el estatus de un reporte YA persistido, contra el pool de
// BankMovement libres, o contra un bucket confirmado_automatico ya vinculado con el mismo
// monto (corroboración, sin crear un link nuevo).
describe('evaluarReporte', () => {
  test('reporte no existe: NotFoundError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(null);
    await expect(evaluarReporte('rep-1')).rejects.toThrow(NotFoundError);
  });

  test('reporte eliminado (oculto): se salta por completo, no evalúa ni toca nada', async () => {
    const reporte = fakeReporteRecienCreado({ eliminado: true });
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);

    const { candidatos } = await evaluarReporte('rep-1');

    expect(candidatos).toEqual([]);
    expect(BankMovement.find).not.toHaveBeenCalled();
    expect(reporte.save).not.toHaveBeenCalled();
  });

  test('0 candidatos libres y ningún bucket corroborable: discrepancia/sin_candidato', async () => {
    const reporte = fakeReporteRecienCreado();
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
    BankMovement.find = jest.fn(() => fakeFind([]));
    NetpayMatch.find = jest.fn(() => fakeFind([]));

    await evaluarReporte('rep-1');

    expect(reporte.estatus).toBe('discrepancia');
    expect(reporte.motivoDiscrepancia).toBe('sin_candidato');
    expect(setErpIds).not.toHaveBeenCalled();
  });

  test('2+ candidatos libres dentro de tolerancia: discrepancia/multiples_candidatos', async () => {
    const reporte = fakeReporteRecienCreado({ montoDepositoTotal: 1000 });
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
    BankMovement.find = jest.fn(() => fakeFind([
      { _id: 'mov-1', banco: 'BBVA', deposito: 1000 },
      { _id: 'mov-2', banco: 'BBVA', deposito: 1000.2 },
    ]));

    await evaluarReporte('rep-1');

    expect(reporte.estatus).toBe('discrepancia');
    expect(reporte.motivoDiscrepancia).toBe('multiples_candidatos');
  });

  test('1 candidato libre exacto: auto-vincula (erp-link), emite sockets, cierra buckets cubiertos', async () => {
    const reporte = fakeReporteRecienCreado({ montoDepositoTotal: 1000 });
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
    const mov = { _id: 'mov-1', banco: 'BBVA', deposito: 1000 };
    BankMovement.find = jest.fn(() => fakeFind([mov]));
    const movActualizado = { _id: 'mov-1', banco: 'BBVA' };
    setErpIds.mockResolvedValue(movActualizado);
    NetpayMatch.find = jest.fn(() => fakeFind([]));

    await evaluarReporte('rep-1');

    expect(reporte.estatus).toBe('resuelto_por_reporte');
    expect(reporte.vinculo).toBe('erp-link');
    expect(reporte.movementIdConfirmado).toBe('mov-1');
    expect(emitToBanco).toHaveBeenCalledWith('BBVA', 'bank:movement:updated', movActualizado);
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
  });

  // design.md "(a) Report present": "0 candidates, but the folios cover a
  // confirmado_automatico bucket linked to M with the same amount -> resuelto_por_reporte,
  // vinculo:'corroborado' (no new link)".
  test('0 candidatos libres, pero YA hay un bucket confirmado_automatico vinculado con el MISMO monto: corroboración, sin crear un link nuevo', async () => {
    const reporte = fakeReporteRecienCreado({ montoDepositoTotal: 1000 });
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
    BankMovement.find = jest.fn(() => fakeFind([])); // sin candidatos LIBRES (ya está vinculado)
    NetpayMatch.find = jest.fn(() => fakeFind([
      { _id: 'nm-1', estatusMatch: 'confirmado_automatico', movementIdsConfirmados: ['mov-YA-VINCULADO'] },
    ]));
    BankMovement.findById = jest.fn(() => fakeFind({ _id: 'mov-YA-VINCULADO', banco: 'BBVA', deposito: 1000 }));

    await evaluarReporte('rep-1');

    expect(reporte.estatus).toBe('resuelto_por_reporte');
    expect(reporte.vinculo).toBe('corroborado');
    expect(reporte.movementIdConfirmado).toBe('mov-YA-VINCULADO');
    expect(setErpIds).not.toHaveBeenCalled(); // NUNCA crea un link nuevo en la corroboración
  });

  // design.md: "for each bucket in {pendiente_por_marca, discrepancia(!revertido)}: all
  // snapshot folios ⊆ R's non-duplicate folios -> resuelto_por_reporte ... only some
  // folios covered -> discrepancia/cobertura_parcial".
  describe('cierre de buckets cubiertos (pendiente_por_marca / discrepancia) tras resolver el reporte', () => {
    test('bucket pendiente_por_marca (AMEX) cuyo ÚNICO folio aparece en el reporte: se cierra a resuelto_por_reporte', async () => {
      const reporte = fakeReporteRecienCreado({
        montoDepositoTotal: 1000,
        folios: [{ referencia: 'F-AMEX-1', orderId: 'ORD-AMEX-1', duplicadoDeReporteId: null }],
      });
      NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
      const mov = { _id: 'mov-1', banco: 'BBVA', deposito: 1000 };
      BankMovement.find = jest.fn(() => fakeFind([mov]));
      setErpIds.mockResolvedValue({ _id: 'mov-1', banco: 'BBVA' });

      const bucketAmex = {
        _id: 'nm-amex', estatusMatch: 'pendiente_por_marca',
        snapshot: { folios: [{ orderId: 'ORD-AMEX-1', referencia: 'F-AMEX-1' }] },
      };
      NetpayMatch.find = jest.fn(() => fakeFind([bucketAmex]));
      NetpayMatch.findOneAndUpdate = jest.fn().mockResolvedValue({ ...bucketAmex, estatusMatch: 'resuelto_por_reporte' });

      await evaluarReporte('rep-1');

      expect(NetpayMatch.findOneAndUpdate).toHaveBeenCalledWith(
        { _id: 'nm-amex', estatusMatch: 'pendiente_por_marca' },
        expect.objectContaining({ $set: expect.objectContaining({ estatusMatch: 'resuelto_por_reporte', motivoDiscrepancia: null }) }),
      );
    });

    test('bucket discrepancia con SOLO ALGUNOS de sus folios cubiertos por el reporte: discrepancia/cobertura_parcial', async () => {
      const reporte = fakeReporteRecienCreado({
        montoDepositoTotal: 1000,
        folios: [{ referencia: 'F-1', orderId: 'ORD-1', duplicadoDeReporteId: null }],
      });
      NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
      const mov = { _id: 'mov-1', banco: 'BBVA', deposito: 1000 };
      BankMovement.find = jest.fn(() => fakeFind([mov]));
      setErpIds.mockResolvedValue({ _id: 'mov-1', banco: 'BBVA' });

      const bucketParcial = {
        _id: 'nm-parcial', estatusMatch: 'discrepancia', motivoDiscrepancia: 'sin_candidato',
        snapshot: { folios: [{ orderId: 'ORD-1', referencia: 'F-1' }, { orderId: 'ORD-2', referencia: 'F-2' }] },
      };
      NetpayMatch.find = jest.fn(() => fakeFind([bucketParcial]));
      NetpayMatch.findOneAndUpdate = jest.fn().mockResolvedValue({ ...bucketParcial, estatusMatch: 'discrepancia', motivoDiscrepancia: 'cobertura_parcial' });

      await evaluarReporte('rep-1');

      expect(NetpayMatch.findOneAndUpdate).toHaveBeenCalledWith(
        { _id: 'nm-parcial', estatusMatch: 'discrepancia' },
        { $set: { estatusMatch: 'discrepancia', motivoDiscrepancia: 'cobertura_parcial' } },
      );
    });

    test('bucket discrepancia/revertido: NUNCA se toca, ni siquiera si sus folios aparecen en el reporte', async () => {
      const reporte = fakeReporteRecienCreado({
        montoDepositoTotal: 1000,
        folios: [{ referencia: 'F-1', orderId: 'ORD-1', duplicadoDeReporteId: null }],
      });
      NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
      const mov = { _id: 'mov-1', banco: 'BBVA', deposito: 1000 };
      BankMovement.find = jest.fn(() => fakeFind([mov]));
      setErpIds.mockResolvedValue({ _id: 'mov-1', banco: 'BBVA' });

      const bucketRevertido = {
        _id: 'nm-revertido', estatusMatch: 'discrepancia', motivoDiscrepancia: 'revertido',
        snapshot: { folios: [{ orderId: 'ORD-1', referencia: 'F-1' }] },
      };
      NetpayMatch.find = jest.fn(() => fakeFind([bucketRevertido]));
      NetpayMatch.findOneAndUpdate = jest.fn();

      await evaluarReporte('rep-1');

      expect(NetpayMatch.findOneAndUpdate).not.toHaveBeenCalled();
    });

    // design.md "Idempotent folios": un folio marcado duplicadoDeReporteId (E11000) NO
    // cuenta como "cubierto por este reporte" para cerrar un bucket ajeno.
    test('folio duplicado (duplicadoDeReporteId seteado): se EXCLUYE del cálculo de cobertura', async () => {
      const reporte = fakeReporteRecienCreado({
        montoDepositoTotal: 1000,
        folios: [{ referencia: 'F-1', orderId: 'ORD-1', duplicadoDeReporteId: 'otro-reporte' }],
      });
      NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
      BankMovement.find = jest.fn(() => fakeFind([{ _id: 'mov-1', banco: 'BBVA', deposito: 1000 }]));
      setErpIds.mockResolvedValue({ _id: 'mov-1', banco: 'BBVA' });

      const bucket = {
        _id: 'nm-1', estatusMatch: 'discrepancia', motivoDiscrepancia: 'sin_candidato',
        snapshot: { folios: [{ orderId: 'ORD-1', referencia: 'F-1' }] },
      };
      NetpayMatch.find = jest.fn(() => fakeFind([bucket]));
      NetpayMatch.findOneAndUpdate = jest.fn();

      await evaluarReporte('rep-1');

      // El único folio del bucket (ORD-1) está marcado duplicado en el reporte -> ninguna
      // clave "válida" lo cubre -> el bucket NO se toca en absoluto (0 de 0 cubiertos, no
      // hay folios que evaluar realmente).
      expect(NetpayMatch.findOneAndUpdate).not.toHaveBeenCalled();
    });
  });
});

describe('eliminarReporte / restaurarReporte (soft-delete)', () => {
  function fakeReporteDoc(overrides = {}) {
    return {
      _id: 'rep-1', eliminado: false, eliminadoPor: null, eliminadoEn: null, eliminadoMotivo: null,
      estatus: 'resuelto_por_reporte', movementIdConfirmado: 'mov-1',
      save: jest.fn().mockResolvedValue(undefined),
      toObject: jest.fn(function () { return { ...this }; }),
      ...overrides,
    };
  }

  test('eliminarReporte: no existe -> NotFoundError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(null);
    await expect(eliminarReporte('rep-1', 'motivo', USER)).rejects.toThrow(NotFoundError);
  });

  // design.md "Report Soft-Delete and Snapshot Integrity": ocultar un reporte NUNCA cambia
  // su estatus/links — solo lo oculta de listados/cierres.
  test('eliminarReporte: marca eliminado+audit, NUNCA cambia estatus ni movementIdConfirmado', async () => {
    const reporte = fakeReporteDoc();
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);

    await eliminarReporte('rep-1', 'cargado por error', USER);

    expect(reporte.eliminado).toBe(true);
    expect(reporte.eliminadoPor).toEqual({ userId: 'user-1', nombre: 'Ana' });
    expect(reporte.eliminadoEn).toBeInstanceOf(Date);
    expect(reporte.eliminadoMotivo).toBe('cargado por error');
    expect(reporte.estatus).toBe('resuelto_por_reporte'); // sin cambios
    expect(reporte.movementIdConfirmado).toBe('mov-1');   // sin cambios
    expect(reporte.save).toHaveBeenCalled();
  });

  test('restaurarReporte: no existe -> NotFoundError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(null);
    await expect(restaurarReporte('rep-1')).rejects.toThrow(NotFoundError);
  });

  // design.md "Restoring a hidden report": "clears eliminado/eliminadoPor/En/Motivo; no
  // other field changes" — estatus y links quedan EXACTAMENTE igual.
  test('restaurarReporte: limpia SOLO eliminado+audit, estatus y movementIdConfirmado quedan intactos', async () => {
    const reporte = fakeReporteDoc({
      eliminado: true,
      eliminadoPor: { userId: 'user-2', nombre: 'Otro' },
      eliminadoEn: new Date('2026-09-01T00:00:00.000Z'),
      eliminadoMotivo: 'por error',
    });
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);

    await restaurarReporte('rep-1');

    expect(reporte.eliminado).toBe(false);
    expect(reporte.eliminadoPor).toBeNull();
    expect(reporte.eliminadoEn).toBeNull();
    expect(reporte.eliminadoMotivo).toBeNull();
    expect(reporte.estatus).toBe('resuelto_por_reporte');
    expect(reporte.movementIdConfirmado).toBe('mov-1');
    expect(reporte.save).toHaveBeenCalled();
  });
});

describe('listar', () => {
  test('sin filtro: trae todos NO eliminados (oculta eliminado por default), ordenado por fechaMovimiento desc', async () => {
    const sortFn = jest.fn(() => fakeFind([{ _id: 'r1' }]));
    NetpayReporte.find = jest.fn(() => ({ sort: sortFn }));

    const { reportes } = await listar();

    expect(NetpayReporte.find).toHaveBeenCalledWith({ eliminado: { $ne: true } });
    expect(sortFn).toHaveBeenCalledWith({ fechaMovimiento: -1 });
    expect(reportes).toEqual([{ _id: 'r1' }]);
  });

  test('con estatus: filtra por él, ADEMÁS de ocultar eliminado', async () => {
    const sortFn = jest.fn(() => fakeFind([]));
    NetpayReporte.find = jest.fn(() => ({ sort: sortFn }));

    await listar({ estatus: 'resuelto_por_reporte' });

    expect(NetpayReporte.find).toHaveBeenCalledWith({ estatus: 'resuelto_por_reporte', eliminado: { $ne: true } });
  });

  // design.md API table: "GET /netpay/reporte?estatus&incluirEliminados | Hides eliminado
  // by default" — con incluirEliminados:true, el filtro eliminado desaparece por completo.
  test('incluirEliminados:true: NO filtra por eliminado, trae todo', async () => {
    const sortFn = jest.fn(() => fakeFind([]));
    NetpayReporte.find = jest.fn(() => ({ sort: sortFn }));

    await listar({ incluirEliminados: true });

    expect(NetpayReporte.find).toHaveBeenCalledWith({});
  });
});

describe('obtenerDetalle', () => {
  test('no existe: NotFoundError', async () => {
    NetpayReporte.findById = jest.fn(() => fakeFind(null));
    await expect(obtenerDetalle('x')).rejects.toThrow(NotFoundError);
  });

  test('existe: lo devuelve', async () => {
    NetpayReporte.findById = jest.fn(() => fakeFind({ _id: 'r1' }));
    const { reporte } = await obtenerDetalle('r1');
    expect(reporte).toEqual({ _id: 'r1' });
  });
});

// buscarCandidatos — recalcula EN VIVO los mismos candidatos que ya calculó cargarReporte al
// momento de la carga, para un reporte YA persistido (ver comentario en el service). Mismo
// criterio de búsqueda (banco BBVA, monto ≈ montoDepositoTotal con tolerancia, ventana de
// días) — reusa _buscarCandidatosParaReporte, ya ejercitado indirectamente por los tests de
// cargarReporte de arriba.
describe('buscarCandidatos', () => {
  test('reporte no existe: NotFoundError', async () => {
    NetpayReporte.findById = jest.fn(() => fakeFind(null));
    await expect(buscarCandidatos('rep-1')).rejects.toThrow(NotFoundError);
  });

  test('0 candidatos: arreglo vacío', async () => {
    NetpayReporte.findById = jest.fn(() => fakeFind({
      _id: 'rep-1', fechaMovimiento: new Date('2026-09-25T00:00:00.000Z'), montoDepositoTotal: 1000,
    }));
    BankMovement.find = jest.fn(() => fakeFind([]));

    const { candidatos } = await buscarCandidatos('rep-1');

    expect(candidatos).toEqual([]);
  });

  test('1 candidato dentro de tolerancia: lo devuelve', async () => {
    NetpayReporte.findById = jest.fn(() => fakeFind({
      _id: 'rep-1', fechaMovimiento: new Date('2026-09-25T00:00:00.000Z'), montoDepositoTotal: 1000,
    }));
    BankMovement.find = jest.fn(() => fakeFind([{ _id: 'mov-1', banco: 'BBVA', deposito: 1000.5 }]));

    const { candidatos } = await buscarCandidatos('rep-1');

    expect(candidatos).toEqual([{ _id: 'mov-1', banco: 'BBVA', deposito: 1000.5 }]);
  });

  test('>1 candidatos (ambigüedad): devuelve todos, sin resolverla acá', async () => {
    NetpayReporte.findById = jest.fn(() => fakeFind({
      _id: 'rep-1', fechaMovimiento: new Date('2026-09-25T00:00:00.000Z'), montoDepositoTotal: 1000,
    }));
    BankMovement.find = jest.fn(() => fakeFind([
      { _id: 'mov-1', banco: 'BBVA', deposito: 1000 },
      { _id: 'mov-2', banco: 'BBVA', deposito: 1000.2 },
    ]));

    const { candidatos } = await buscarCandidatos('rep-1');

    expect(candidatos).toHaveLength(2);
    expect(candidatos.map(c => c._id)).toEqual(['mov-1', 'mov-2']);
  });

  test('usa la misma ventana de días que cargarReporte (_ventanaDiasNetpay) para acotar la búsqueda', async () => {
    NetpayReporte.findById = jest.fn(() => fakeFind({
      _id: 'rep-1', fechaMovimiento: new Date('2026-09-25T00:00:00.000Z'), montoDepositoTotal: 1000,
    }));
    _ventanaDiasNetpay.mockResolvedValue(3);
    BankMovement.find = jest.fn(() => fakeFind([]));

    await buscarCandidatos('rep-1');

    const filtro = BankMovement.find.mock.calls[0][0];
    const msVentana = 3 * 24 * 60 * 60 * 1000;
    expect(filtro.fecha.$gte).toEqual(new Date(new Date('2026-09-25T00:00:00.000Z').getTime() - msVentana));
    expect(filtro.fecha.$lte).toEqual(new Date(new Date('2026-09-25T00:00:00.000Z').getTime() + msVentana));
  });

  // design.md API table: "GET /netpay/reporte/:id/candidatos?modo=ventana | Adds the window
  // mode" — a diferencia del modo default (filtra por _montosIguales contra
  // montoDepositoTotal), modo='ventana' devuelve TODOS los elegibles de la ventana sin
  // filtrar por monto, ordenados por |diferencia| ascendente (mismo criterio EXACTO que
  // netpay-resolver.service.js#candidatos, para el diálogo de resolver manual de un reporte
  // en discrepancia).
  test('modo="ventana": devuelve TODOS los elegibles sin filtrar por monto, ordenados por |diferencia|', async () => {
    NetpayReporte.findById = jest.fn(() => fakeFind({
      _id: 'rep-1', fechaMovimiento: new Date('2026-09-25T00:00:00.000Z'), montoDepositoTotal: 1000,
    }));
    BankMovement.find = jest.fn(() => fakeFind([
      { _id: 'mov-lejano', banco: 'BBVA', deposito: 1200 },  // diferencia 200
      { _id: 'mov-cercano', banco: 'BBVA', deposito: 1005 }, // diferencia 5 (fuera de ERP_TOLERANCE, igual se incluye)
    ]));

    const { candidatos } = await buscarCandidatos('rep-1', 'ventana');

    expect(candidatos.map(c => c._id)).toEqual(['mov-cercano', 'mov-lejano']);
    expect(candidatos.find(c => c._id === 'mov-cercano').diferencia).toBe(5);
  });
});

// resolverReporte / rechazarReporte — netpay-matching-v2 (design.md API table:
// "POST /netpay/reporte/:id/{resolver,...}": New. "POST /netpay/reporte/:id/descartar":
// "Maps to rechazado, also allowed from discrepancia"). Reemplazan confirmarReporte/
// descartarReporte (Implementación 1, ELIMINADOS de este archivo — su guardia
// `estatus !== 'pendiente'` nunca puede calzar contra un documento v2 real, ya que
// 'pendiente' no existe en el enum nuevo, ver NetpayReporte.model.js). Mismas reglas de
// validación que el resolver/rechazar a nivel bucket (netpay-resolver.service.js): 400 en
// justificación vacía, 409 fuera de estado, campos de auditoría registrados. A diferencia
// del bucket (que admite un split de 1-2 BankMovement vía movementIdsConfirmados[]), el
// modelo NetpayReporte solo tiene UN campo movementIdConfirmado (1 reporte == a lo sumo 1
// depósito) — se acepta como mucho 1 movementId acá (desviación documentada respecto a la
// decisión de arquitectura genérica "1-2", acotada por el modelo de datos real).
describe('resolverReporte', () => {
  function fakeReporteDoc(overrides = {}) {
    return {
      _id: 'rep-1', estatus: 'discrepancia', claveRastreo: 'CLAVE-1', montoDepositoTotal: 1000,
      motivoDiscrepancia: 'sin_candidato', justificacion: null, resueltoManualPor: null, resueltoManualEn: null,
      movementIdConfirmado: null,
      save: jest.fn().mockResolvedValue(undefined),
      toObject: jest.fn(function () { return { ...this }; }),
      ...overrides,
    };
  }

  test('sin justificación (vacía/blanco): BadRequestError, ni siquiera busca el reporte', async () => {
    await expect(resolverReporte('rep-1', { justificacion: '' }, USER)).rejects.toThrow(/justificación/);
    await expect(resolverReporte('rep-1', { justificacion: '   ' }, USER)).rejects.toThrow(/justificación/);
    expect(NetpayReporte.findById).not.toHaveBeenCalled();
  });

  test('más de 1 movementId: BadRequestError (el modelo solo soporta 1 movimiento por reporte)', async () => {
    await expect(resolverReporte('rep-1', { justificacion: 'ok', movementIds: ['a', 'b'] }, USER))
      .rejects.toThrow(/a lo sumo 1/);
  });

  test('reporte no existe: NotFoundError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(null);
    await expect(resolverReporte('rep-1', { justificacion: 'ok' }, USER)).rejects.toThrow(NotFoundError);
  });

  test('reporte NO está en discrepancia: ConflictError (409)', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc({ estatus: 'resuelto_por_reporte' }));
    await expect(resolverReporte('rep-1', { justificacion: 'ok' }, USER)).rejects.toThrow(/no está en discrepancia/);
  });

  test('sin movementIds (solo justificación): resuelve sin tocar BankMovement/setErpIds', async () => {
    const reporte = fakeReporteDoc();
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);

    await resolverReporte('rep-1', { justificacion: 'Confirmado a mano contra el estado de cuenta' }, USER);

    expect(setErpIds).not.toHaveBeenCalled();
    expect(reporte.estatus).toBe('resuelto_manual');
    expect(reporte.motivoDiscrepancia).toBeNull();
    expect(reporte.justificacion).toBe('Confirmado a mano contra el estado de cuenta');
    expect(reporte.resueltoManualPor).toEqual({ userId: 'user-1', nombre: 'Ana' });
    expect(reporte.resueltoManualEn).toBeInstanceOf(Date);
  });

  test('movimiento no existe: NotFoundError, nunca llega a llamar setErpIds', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc());
    BankMovement.find = jest.fn(() => fakeFind([]));

    await expect(resolverReporte('rep-1', { justificacion: 'ok', movementIds: ['mov-1'] }, USER)).rejects.toThrow(NotFoundError);
    expect(setErpIds).not.toHaveBeenCalled();
  });

  test('movimiento no es de BBVA: ConflictError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc());
    BankMovement.find = jest.fn(() => fakeFind([{ _id: 'mov-1', banco: 'Banamex', erpLinks: [], deposito: 1000 }]));
    await expect(resolverReporte('rep-1', { justificacion: 'ok', movementIds: ['mov-1'] }, USER)).rejects.toThrow(/no es de BBVA/);
  });

  test('movimiento ya tiene erpLinks: ConflictError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc());
    BankMovement.find = jest.fn(() => fakeFind([{ _id: 'mov-1', banco: 'BBVA', erpLinks: [{ erpId: 'X' }], deposito: 1000 }]));
    await expect(resolverReporte('rep-1', { justificacion: 'ok', movementIds: ['mov-1'] }, USER)).rejects.toThrow(/ya tiene un ID ERP vinculado/);
  });

  test('con 1 movementId válido: vincula vía setErpIds (erpId NETPAYRPT-<claveRastreo>-MANUAL), audita, emite sockets', async () => {
    const reporte = fakeReporteDoc();
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
    const mov = { _id: 'mov-1', banco: 'BBVA', erpLinks: [], deposito: 1000 };
    BankMovement.find = jest.fn(() => fakeFind([mov]));
    const movActualizado = { _id: 'mov-1', banco: 'BBVA' };
    setErpIds.mockResolvedValue(movActualizado);

    const res = await resolverReporte('rep-1', { justificacion: 'ok', movementIds: ['mov-1'] }, USER);

    expect(setErpIds).toHaveBeenCalledWith(
      'mov-1',
      [expect.objectContaining({ erpId: 'NETPAYRPT-CLAVE-1-MANUAL', origen: 'netpay-reporte-manual' })],
      USER,
    );
    expect(reporte.movementIdConfirmado).toBe('mov-1');
    expect(emitToBanco).toHaveBeenCalledWith('BBVA', 'bank:movement:updated', movActualizado);
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
    expect(res.movimientos).toEqual([movActualizado]);
  });

  // spec.md "Manual action cannot force auto-confirmed" — guard de diseño no negociable,
  // aplica IGUAL a nivel reporte que a nivel bucket.
  test('el resultado NUNCA es confirmado_automatico ni resuelto_por_reporte', async () => {
    const reporte = fakeReporteDoc();
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
    BankMovement.find = jest.fn(() => fakeFind([{ _id: 'mov-1', banco: 'BBVA', erpLinks: [], deposito: 1000 }]));
    setErpIds.mockResolvedValue({ _id: 'mov-1', banco: 'BBVA' });

    await resolverReporte('rep-1', { justificacion: 'ok', movementIds: ['mov-1'] }, USER);

    expect(reporte.estatus).not.toBe('confirmado_automatico');
    expect(reporte.estatus).not.toBe('resuelto_por_reporte');
    expect(reporte.estatus).toBe('resuelto_manual');
  });
});

describe('rechazarReporte', () => {
  function fakeReporteDoc(overrides = {}) {
    return {
      _id: 'rep-1', estatus: 'discrepancia', descartadoPor: null, descartadoEn: null, descartadoMotivo: null,
      save: jest.fn().mockResolvedValue(undefined), toObject: jest.fn(() => ({ _id: 'rep-1' })),
      ...overrides,
    };
  }

  test('no existe: NotFoundError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(null);
    await expect(rechazarReporte('rep-1', { motivo: 'x' }, USER)).rejects.toThrow(NotFoundError);
  });

  test('reporte ya rechazado: ConflictError (estado terminal)', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc({ estatus: 'rechazado' }));
    await expect(rechazarReporte('rep-1', { motivo: 'x' }, USER)).rejects.toThrow(ConflictError);
  });

  test('reporte ya resuelto_manual: ConflictError (estado terminal)', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc({ estatus: 'resuelto_manual' }));
    await expect(rechazarReporte('rep-1', { motivo: 'x' }, USER)).rejects.toThrow(ConflictError);
  });

  // design.md API table: "Maps to rechazado, also allowed from discrepancia".
  test('desde discrepancia: pasa a rechazado, audita motivo/quién/cuándo', async () => {
    const reporte = fakeReporteDoc();
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);

    await rechazarReporte('rep-1', { motivo: '  ya identificado a mano  ' }, USER);

    expect(reporte.estatus).toBe('rechazado');
    expect(reporte.descartadoMotivo).toBe('ya identificado a mano');
    expect(reporte.descartadoPor).toEqual({ userId: 'user-1', nombre: 'Ana' });
    expect(reporte.descartadoEn).toBeInstanceOf(Date);
    expect(reporte.save).toHaveBeenCalled();
  });

  test('desde resuelto_por_reporte: también permitido (spec.md "any active state -> rechazado")', async () => {
    const reporte = fakeReporteDoc({ estatus: 'resuelto_por_reporte' });
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);

    await rechazarReporte('rep-1', { motivo: 'ya no aplica' }, USER);

    expect(reporte.estatus).toBe('rechazado');
  });

  test('sin motivo: descartadoMotivo queda null (no se exige)', async () => {
    const reporte = fakeReporteDoc();
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);

    await rechazarReporte('rep-1', {}, USER);

    expect(reporte.descartadoMotivo).toBeNull();
  });
});

describe('consultarFolioKore', () => {
  function fakeReporteDoc(overrides = {}) {
    return {
      _id: 'rep-1',
      folios: [{ referencia: 'F1', koreCache: null }],
      save: jest.fn().mockResolvedValue(undefined),
      ...overrides,
    };
  }

  test('sin referencia: BadRequestError', async () => {
    await expect(consultarFolioKore('rep-1', undefined)).rejects.toThrow('Se requiere referencia');
  });

  test('reporte no existe: NotFoundError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(null);
    await expect(consultarFolioKore('rep-1', 'F1')).rejects.toThrow(NotFoundError);
  });

  test('el folio no pertenece a este reporte: NotFoundError, ni siquiera consulta Kore', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc());
    await expect(consultarFolioKore('rep-1', 'NO-EXISTE')).rejects.toThrow(/Folio NO-EXISTE/);
    expect(buscarTransaccionesNetpay).not.toHaveBeenCalled();
  });

  test('Kore no devuelve ninguna transacción con cuentas: NotFoundError, no cachea nada', async () => {
    const reporteDoc = fakeReporteDoc();
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporteDoc);
    buscarTransaccionesNetpay.mockResolvedValue({ raw: { Data: { transactions: [] } } });

    await expect(consultarFolioKore('rep-1', 'F1')).rejects.toThrow(/No se encontró la cuenta en Kore/);
    expect(reporteDoc.save).not.toHaveBeenCalled();
  });

  test('Kore devuelve la transacción con cuentas: cachea tal cual (sin remapear) en folios[].koreCache', async () => {
    const reporteDoc = fakeReporteDoc();
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporteDoc);
    const cuentaCruda = { SerieExterna: 'H0', FolioExterno: '260100639', Total: 488.73 };
    buscarTransaccionesNetpay.mockResolvedValue({
      raw: { Data: { transactions: [{ folio: 'F1', cuentas: [cuentaCruda] }] } },
    });

    const res = await consultarFolioKore('rep-1', 'F1');

    expect(buscarTransaccionesNetpay).toHaveBeenCalledWith({ folio: 'F1', withAccountInfo: true, status: 'completed' });
    expect(res.cuenta).toBe(cuentaCruda);
    expect(reporteDoc.folios[0].koreCache.cuenta).toBe(cuentaCruda);
    expect(reporteDoc.folios[0].koreCache.consultadoEn).toBeInstanceOf(Date);
    expect(reporteDoc.save).toHaveBeenCalled();
  });
});

// obtenerPorMovimiento — Fix 3 (2026-09-25): ver folios relacionados desde el modal ERP de
// Bancos (erp-modal.component.ts#esErpIdNetpayReporte) sin depender de abrir el panel de
// Netpay ni de exportar el Excel.
describe('obtenerPorMovimiento', () => {
  test('no hay ningún reporte para este movimiento: NotFoundError (404 legible, no un array vacío)', async () => {
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    await expect(obtenerPorMovimiento('mov-sin-reporte')).rejects.toThrow(NotFoundError);
    expect(NetpayReporte.findOne).toHaveBeenCalledWith({ movementIdConfirmado: 'mov-sin-reporte' });
  });

  test('existe un reporte confirmado contra ese movimiento: lo devuelve', async () => {
    const reporte = { _id: 'rep-1', movementIdConfirmado: 'mov-1', estatus: 'confirmado' };
    NetpayReporte.findOne = jest.fn(() => fakeFind(reporte));

    const res = await obtenerPorMovimiento('mov-1');

    expect(res.reporte).toEqual(reporte);
  });
});

// consultarFoliosPendientes — Fix 2a (2026-09-25): antes de exportar el Excel, consulta
// contra Kore SOLO los folios que todavía no tienen koreCache.cuenta, secuencialmente (nunca
// Promise.all/paralelo), reusando consultarFolioKore folio por folio — con el mismo patrón de
// reintento ante 429 que erp-sync.service.js#_getConReintento (backoff fijo leído de "retry
// after: X" en el cuerpo del 429). Un folio individual que falle (404 sin match, red, 429
// agotado) se acumula en `fallos` sin abortar el resto (fallo parcial, no todo-o-nada).
describe('consultarFoliosPendientes', () => {
  // La misma Query "thenable + .lean()" para simular tanto el findById(id).lean() propio de
  // consultarFoliosPendientes como el findById(id) SIN .lean() que hace consultarFolioKore
  // por dentro (mismo mock de NetpayReporte.findById para ambos casos, ver comentario del
  // helper de arriba en este archivo: fakeFind ya cubre .lean(), acá además hace falta que
  // sea awaitable directo).
  function fakeQuery(result) {
    return {
      lean: jest.fn().mockResolvedValue(result),
      then: (resolve, reject) => Promise.resolve(result).then(resolve, reject),
    };
  }

  afterEach(() => {
    jest.useRealTimers();
  });

  test('reporte no existe: NotFoundError', async () => {
    NetpayReporte.findById = jest.fn(() => fakeQuery(null));
    await expect(consultarFoliosPendientes('rep-1')).rejects.toThrow(NotFoundError);
  });

  test('folios mixtos: salta los ya cacheados, reintenta ante un 429 simulado, y un fallo individual no aborta el resto', async () => {
    jest.useFakeTimers();

    const docState = {
      _id: 'rep-1',
      folios: [
        { referencia: 'F1', koreCache: { cuenta: { Total: 100 } } }, // ya cacheado — se salta, nunca se consulta
        { referencia: 'F2', koreCache: null },                       // 429 en el 1er intento, éxito en el 2do
        { referencia: 'F3', koreCache: null },                       // éxito directo
        { referencia: 'F4', koreCache: null },                       // Kore no devuelve cuenta -> falla, no aborta el resto
      ],
      save: jest.fn().mockResolvedValue(undefined),
    };
    NetpayReporte.findById = jest.fn(() => fakeQuery(docState));

    let intentosF2 = 0;
    buscarTransaccionesNetpay.mockImplementation(({ folio }) => {
      if (folio === 'F2') {
        intentosF2 += 1;
        if (intentosF2 === 1) {
          const err = new Error('Too Many Requests');
          err.statusCode = 429;
          err.koreBody = { Data: 'retry after: 0.5 segundos' };
          return Promise.reject(err);
        }
        return Promise.resolve({ raw: { Data: { transactions: [{ folio: 'F2', cuentas: [{ Total: 500 }] }] } } });
      }
      if (folio === 'F3') {
        return Promise.resolve({ raw: { Data: { transactions: [{ folio: 'F3', cuentas: [{ Total: 300 }] }] } } });
      }
      if (folio === 'F4') {
        return Promise.resolve({ raw: { Data: { transactions: [] } } }); // sin cuenta -> NotFoundError, no cachea
      }
      return Promise.reject(new Error(`folio inesperado en el test: ${folio}`));
    });

    const promise = consultarFoliosPendientes('rep-1');
    await jest.runAllTimersAsync();
    const resultado = await promise;

    // F1 (ya cacheado) NUNCA se consulta.
    expect(buscarTransaccionesNetpay).not.toHaveBeenCalledWith(expect.objectContaining({ folio: 'F1' }));
    // F2 reintentó tras el 429 (2 intentos en total).
    expect(intentosF2).toBe(2);
    // F2 y F3 resueltos; F4 falló sin abortar el recorrido.
    expect(resultado.consultados).toBe(2);
    expect(resultado.fallos).toEqual([{ referencia: 'F4', error: expect.stringContaining('No se encontró la cuenta en Kore') }]);
    // F2 y F3 quedaron cacheados de verdad (secuencial, reusando consultarFolioKore).
    expect(docState.folios.find(f => f.referencia === 'F2').koreCache.cuenta).toEqual({ Total: 500 });
    expect(docState.folios.find(f => f.referencia === 'F3').koreCache.cuenta).toEqual({ Total: 300 });
  });

  // Fix perf (2026-09-29, pedido explícito del usuario — "es normal que demore la bandeja de
  // Netpay - Reporte manual"): el recorrido SECUENCIAL de un folio a la vez (con 400ms de
  // pausa entre cada uno) es un diseño consciente para no saturar Kore, pero con reportes de
  // 30-50 folios sin consultar hace que el export tarde varios minutos. Se paraleliza en lotes
  // de tamaño fijo (CONSULTAR_FOLIOS_CONCURRENCIA=5, ver comentario en la función real) usando
  // Promise.allSettled por lote — la pausa de 400ms pasa a ser ENTRE LOTES, no entre cada
  // llamada individual. Mantiene el mismo contrato: reintento 429 por folio (sin cambios),
  // fallo parcial sin abortar el resto.
  test('respeta el límite de concurrencia por lotes: nunca dispara más de N llamadas a Kore en simultáneo', async () => {
    const CONCURRENCIA = 5; // debe matchear CONSULTAR_FOLIOS_CONCURRENCIA en netpay-reporte.service.js
    jest.useFakeTimers();

    const referencias = Array.from({ length: 7 }, (_, i) => `F${i + 1}`);
    const docState = {
      _id: 'rep-1',
      folios: referencias.map(referencia => ({ referencia, koreCache: null })),
      save: jest.fn().mockResolvedValue(undefined),
    };
    NetpayReporte.findById = jest.fn(() => fakeQuery(docState));

    const resolvers = {};
    buscarTransaccionesNetpay.mockImplementation(({ folio }) => new Promise((resolve) => {
      resolvers[folio] = () => resolve({ raw: { Data: { transactions: [{ folio, cuentas: [{ Total: 1 }] }] } } });
    }));

    const promise = consultarFoliosPendientes('rep-1');

    // Flush de microtasks para que el PRIMER lote llegue a disparar sus llamadas a Kore
    // (findById externo -> por cada folio: findById interno de consultarFolioKore ->
    // buscarTransaccionesNetpay), sin resolver ninguna todavía. Cadena de awaits más profunda
    // que un simple Promise.resolve() único, por eso se repite varias veces.
    for (let i = 0; i < 20; i++) await Promise.resolve();

    const llamadasPrimerLote = Object.keys(resolvers).length;
    expect(llamadasPrimerLote).toBe(CONCURRENCIA); // exactamente 5 de 7, no las 7 de una — prueba el límite real
    expect(referencias.slice(CONCURRENCIA)).not.toContain(Object.keys(resolvers)[CONCURRENCIA]); // F6/F7 todavía no llamados

    // Resolvemos el primer lote completo y avanzamos la pausa entre lotes (fake timer).
    Object.values(resolvers).forEach(r => r());
    await jest.advanceTimersByTimeAsync(400);
    for (let i = 0; i < 20; i++) await Promise.resolve();

    expect(Object.keys(resolvers).length).toBe(7); // el 2do lote (F6, F7) ya se disparó

    Object.values(resolvers).forEach(r => r());
    await jest.runAllTimersAsync();
    const resultado = await promise;

    expect(buscarTransaccionesNetpay).toHaveBeenCalledTimes(7);
    expect(resultado).toEqual({ consultados: 7, fallos: [] });
  });

  test('sin folios pendientes (todos ya cacheados): no consulta Kore, 0 fallos', async () => {
    const docState = {
      _id: 'rep-1',
      folios: [{ referencia: 'F1', koreCache: { cuenta: { Total: 100 } } }],
      save: jest.fn().mockResolvedValue(undefined),
    };
    NetpayReporte.findById = jest.fn(() => fakeQuery(docState));

    const resultado = await consultarFoliosPendientes('rep-1');

    expect(buscarTransaccionesNetpay).not.toHaveBeenCalled();
    expect(resultado).toEqual({ consultados: 0, fallos: [] });
  });
});
