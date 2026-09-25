'use strict';

// netpay-reporte.service.test.js — Implementación 1 de "Netpay: carga manual del reporte
// como fuente de verdad". A diferencia del matching automático (netpay-match-confirm.service),
// acá la conciliación es 1:1 (un reporte == a lo sumo UN BankMovement) — el reporte YA trae
// el monto exacto depositado por Netpay, sin la comisión errónea de Kore de por medio.
// parseNetpayReporte se mockea (tiene sus propios tests unitarios en
// netpay-reporte-parser.service.test.js, incluida contra los 2 archivos reales del repo).
jest.mock('../banks/BankMovement.model');
jest.mock('./NetpayReporte.model');
jest.mock('./netpay-reporte-parser.service');
jest.mock('./kore-caja.service', () => ({ buscarTransaccionesNetpay: jest.fn() }));
jest.mock('./netpay-match.service', () => ({ _ventanaDiasNetpay: jest.fn() }));
jest.mock('../../../shared/services/global-config.service');
jest.mock('../../shared/socket', () => ({ emitToBanco: jest.fn(), emitToAll: jest.fn() }));
jest.mock('../banks/bank.service', () => {
  const real = jest.requireActual('../banks/bank.service');
  return { setErpIds: jest.fn(), ERP_TOLERANCE: real.ERP_TOLERANCE, registerErpUnlinkHook: jest.fn() };
});

const BankMovement = require('../banks/BankMovement.model');
const NetpayReporte = require('./NetpayReporte.model');
const { parseNetpayReporte } = require('./netpay-reporte-parser.service');
const { buscarTransaccionesNetpay } = require('./kore-caja.service');
const { _ventanaDiasNetpay } = require('./netpay-match.service');
const { setErpIds } = require('../banks/bank.service');
const { emitToBanco, emitToAll } = require('../../shared/socket');
const { BadRequestError, NotFoundError, ConflictError } = require('../../shared/errors/AppError');
const {
  cargarReporte, listar, obtenerDetalle, obtenerPorMovimiento, buscarCandidatos, confirmarReporte,
  descartarReporte, consultarFolioKore, consultarFoliosPendientes,
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

beforeEach(() => {
  jest.clearAllMocks();
  _ventanaDiasNetpay.mockResolvedValue(2);
  BankMovement.find = jest.fn(() => fakeFind([]));
});

describe('cargarReporte', () => {
  test('parser tira BadRequestError: se propaga tal cual (nunca 500)', async () => {
    parseNetpayReporte.mockRejectedValue(new BadRequestError('El archivo no contiene la hoja "Resumen"'));
    await expect(cargarReporte(Buffer.from(''), 'x.xlsx', USER)).rejects.toThrow(/hoja "Resumen"/);
  });

  test('parser tira un error NO tipado: se envuelve en BadRequestError legible', async () => {
    parseNetpayReporte.mockRejectedValue(new Error('boom interno de ExcelJS'));
    await expect(cargarReporte(Buffer.from(''), 'x.xlsx', USER)).rejects.toThrow(/Error al leer el archivo/);
  });

  test('claveRastreo ya existe: ConflictError, no llega a buscar candidatos ni a crear', async () => {
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

  test('1 folio, 0 candidatos BBVA: queda pendiente, candidatos vacío', async () => {
    parseNetpayReporte.mockResolvedValue(parsedFixture());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    BankMovement.find = jest.fn(() => fakeFind([]));
    const creado = { _id: 'rep-1', estatus: 'pendiente' };
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);

    const { reporte, candidatos } = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(candidatos).toEqual([]);
    expect(reporte).toBe(creado);
    expect(NetpayReporte.create).toHaveBeenCalledWith(expect.objectContaining({
      claveRastreo: 'CLAVE-1', estatus: 'pendiente', nombreArchivoOriginal: 'archivo.xlsx',
      cargadoPor: { userId: 'user-1', nombre: 'Ana' },
    }));
  });

  test('1 candidato BBVA cuyo monto cuadra dentro de tolerancia: aparece en candidatos', async () => {
    parseNetpayReporte.mockResolvedValue(parsedFixture({ montoDepositoTotal: 1000 }));
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    BankMovement.find = jest.fn(() => fakeFind([{ _id: 'mov-1', banco: 'BBVA', deposito: 1000.5 }]));
    NetpayReporte.create = jest.fn().mockResolvedValue({ _id: 'rep-1' });

    const { candidatos } = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(candidatos).toEqual([{ _id: 'mov-1', banco: 'BBVA', deposito: 1000.5 }]);
  });

  test('3 folios (mismo depósito, distintas transacciones): se persisten los 3 en el arreglo folios', async () => {
    const folios = [{ referencia: 'F1' }, { referencia: 'F2' }, { referencia: 'F3' }];
    parseNetpayReporte.mockResolvedValue(parsedFixture({ folios }));
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    NetpayReporte.create = jest.fn().mockResolvedValue({ _id: 'rep-1' });

    await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(NetpayReporte.create).toHaveBeenCalledWith(expect.objectContaining({ folios }));
  });
});

describe('listar', () => {
  test('sin filtro: trae todos, ordenado por fechaMovimiento desc', async () => {
    const sortFn = jest.fn(() => fakeFind([{ _id: 'r1' }]));
    NetpayReporte.find = jest.fn(() => ({ sort: sortFn }));

    const { reportes } = await listar();

    expect(NetpayReporte.find).toHaveBeenCalledWith({});
    expect(sortFn).toHaveBeenCalledWith({ fechaMovimiento: -1 });
    expect(reportes).toEqual([{ _id: 'r1' }]);
  });

  test('con estatus: filtra por él', async () => {
    const sortFn = jest.fn(() => fakeFind([]));
    NetpayReporte.find = jest.fn(() => ({ sort: sortFn }));

    await listar({ estatus: 'confirmado' });

    expect(NetpayReporte.find).toHaveBeenCalledWith({ estatus: 'confirmado' });
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
});

describe('confirmarReporte', () => {
  function fakeReporteDoc(overrides = {}) {
    return {
      _id: 'rep-1', estatus: 'pendiente', claveRastreo: 'CLAVE-1', montoDepositoTotal: 1000,
      save: jest.fn().mockResolvedValue(undefined),
      toObject: jest.fn(function () { return { _id: this._id, estatus: this.estatus }; }),
      ...overrides,
    };
  }

  test('sin movementId: BadRequestError', async () => {
    await expect(confirmarReporte('rep-1', undefined, USER)).rejects.toThrow('Se requiere movementId');
  });

  test('reporte no existe: NotFoundError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(null);
    await expect(confirmarReporte('rep-1', 'mov-1', USER)).rejects.toThrow(NotFoundError);
  });

  test('reporte ya no está pendiente: ConflictError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc({ estatus: 'confirmado' }));
    await expect(confirmarReporte('rep-1', 'mov-1', USER)).rejects.toThrow(/ya no está pendiente/);
  });

  test('movimiento no existe: NotFoundError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc());
    BankMovement.findById = jest.fn().mockResolvedValue(null);
    await expect(confirmarReporte('rep-1', 'mov-1', USER)).rejects.toThrow(NotFoundError);
  });

  test('movimiento no es de BBVA: ConflictError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc());
    BankMovement.findById = jest.fn().mockResolvedValue({ _id: 'mov-1', banco: 'Banamex', erpLinks: [], deposito: 1000 });
    await expect(confirmarReporte('rep-1', 'mov-1', USER)).rejects.toThrow(/no es de BBVA/);
  });

  test('movimiento ya tiene erpLinks: ConflictError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc());
    BankMovement.findById = jest.fn().mockResolvedValue({ _id: 'mov-1', banco: 'BBVA', erpLinks: [{ erpId: 'X' }], deposito: 1000 });
    await expect(confirmarReporte('rep-1', 'mov-1', USER)).rejects.toThrow(/ya tiene un ID ERP vinculado/);
  });

  test('monto no coincide con el reporte: ConflictError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc({ montoDepositoTotal: 1000 }));
    BankMovement.findById = jest.fn().mockResolvedValue({ _id: 'mov-1', banco: 'BBVA', erpLinks: [], deposito: 500 });
    await expect(confirmarReporte('rep-1', 'mov-1', USER)).rejects.toThrow(/no coincide con el depósito del reporte/);
  });

  test('caso válido: setErpIds con erpId NETPAYRPT-<claveRastreo>, guarda el reporte como confirmado, emite sockets', async () => {
    const reporteDoc = fakeReporteDoc();
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporteDoc);
    const mov = { _id: 'mov-1', banco: 'BBVA', erpLinks: [], deposito: 1000 };
    BankMovement.findById = jest.fn().mockResolvedValue(mov);
    const movActualizado = { _id: 'mov-1', banco: 'BBVA' };
    setErpIds.mockResolvedValue(movActualizado);

    const res = await confirmarReporte('rep-1', 'mov-1', USER);

    expect(setErpIds).toHaveBeenCalledWith('mov-1', [{
      erpId: 'NETPAYRPT-CLAVE-1', origen: 'netpay-reporte',
      saldoPagadoTotal: 1000, saldoPagado: 1000, total: 1000,
    }], USER, expect.objectContaining({ session: null }));

    expect(reporteDoc.estatus).toBe('confirmado');
    expect(reporteDoc.movementIdConfirmado).toBe('mov-1');
    expect(reporteDoc.confirmadoPor).toEqual({ userId: 'user-1', nombre: 'Ana' });
    expect(reporteDoc.save).toHaveBeenCalled();

    expect(emitToBanco).toHaveBeenCalledWith('BBVA', 'bank:movement:updated', movActualizado);
    expect(emitToAll).toHaveBeenCalledWith('bank:ficha-pendiente:changed', { movementId: 'mov-1' });
    expect(res.movimiento).toBe(movActualizado);
  });
});

describe('descartarReporte', () => {
  function fakeReporteDoc(overrides = {}) {
    return { _id: 'rep-1', estatus: 'pendiente', save: jest.fn().mockResolvedValue(undefined), toObject: jest.fn(() => ({ _id: 'rep-1' })), ...overrides };
  }

  test('no existe: NotFoundError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(null);
    await expect(descartarReporte('rep-1', 'motivo', USER)).rejects.toThrow(NotFoundError);
  });

  test('ya no está pendiente: ConflictError', async () => {
    NetpayReporte.findById = jest.fn().mockResolvedValue(fakeReporteDoc({ estatus: 'descartado' }));
    await expect(descartarReporte('rep-1', 'motivo', USER)).rejects.toThrow(/ya no está pendiente/);
  });

  test('caso válido: guarda estatus descartado + motivo + descartadoPor', async () => {
    const reporteDoc = fakeReporteDoc();
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporteDoc);

    await descartarReporte('rep-1', '  ya identificado a mano  ', USER);

    expect(reporteDoc.estatus).toBe('descartado');
    expect(reporteDoc.descartadoMotivo).toBe('ya identificado a mano');
    expect(reporteDoc.descartadoPor).toEqual({ userId: 'user-1', nombre: 'Ana' });
    expect(reporteDoc.save).toHaveBeenCalled();
  });

  test('sin motivo: descartadoMotivo queda null (no se exige)', async () => {
    const reporteDoc = fakeReporteDoc();
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporteDoc);

    await descartarReporte('rep-1', undefined, USER);

    expect(reporteDoc.descartadoMotivo).toBeNull();
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
