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
jest.mock('./netpay-comision-sync.service');
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
const { sincronizarComisiones } = require('./netpay-comision-sync.service');
const { emitToBanco, emitToAll } = require('../../shared/socket');
const { BadRequestError, NotFoundError, ConflictError } = require('../../shared/errors/AppError');
const {
  cargarReporte, listar, obtenerUltimaCarga, obtenerDetalle, obtenerPorMovimiento, buscarCandidatos,
  resolverReporte, rechazarReporte, consultarFolioKore, consultarFoliosPendientes,
  consultarFoliosPendientesDeLote, evaluarReporte,
  eliminarReporte, restaurarReporte, _poblarMovimientoVinculado,
} = require('./netpay-reporte.service');

const USER = { _id: 'user-1', nombre: 'Ana' };

function fakeFind(result) {
  return { lean: jest.fn().mockResolvedValue(result) };
}

// 2026-10-06: dentro de cargarReporte, _crearYEvaluar ahora llama a NetpayReporte.findById en
// 2 estilos en el mismo flujo — evaluarReporte() lo usa directo, sin .lean() (necesita el
// documento Mongoose real para poder reporte.save()), y consultarFoliosPendientes() lo usa
// encadenado con .lean() (solo lectura, ver netpay-reporte.service.js). Un mock que sea
// thenable Y tenga .lean() cubre ambos estilos con el mismo fixture, sin duplicar nada por
// test (mismo helper que ya usa, más abajo, el describe de consultarFoliosPendientes).
function fakeQuery(result) {
  return {
    lean: jest.fn().mockResolvedValue(result),
    then: (resolve, reject) => Promise.resolve(result).then(resolve, reject),
  };
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
    folios: [{ referencia: 'F1', terminalID: 'T1', sucursal: 'SUC-1' }],
    ...overrides,
  };
}

// netpay-reporte-global (design.md): parseNetpayReporte ahora devuelve SIEMPRE
// `{ depositos: [...] }` — este helper arma el caso N=1 (compat), el más usado por los
// tests YA existentes de cargarReporte (ver describe de abajo).
function parsedN1(overrides = {}) {
  return { depositos: [parsedFixture(overrides)] };
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
  // default: ningún test se preocupa por el movimiento vinculado salvo que lo pisen —
  // _poblarMovimientoVinculado(reporte) llama findById SIEMPRE que haya
  // movementIdConfirmado, así que sin esto cualquier fixture con ese campo poblado
  // rompería con "Cannot read properties of undefined (reading 'lean')".
  BankMovement.findById = jest.fn(() => fakeFind(null));
  NetpayMatch.find = jest.fn(() => fakeFind([]));
  NetpayFolioRegistro.insertMany = jest.fn().mockResolvedValue([]);
  // Default (2026-10-06): _crearYEvaluar ahora llama a consultarFoliosPendientes() para TODO
  // reporte recién creado (ver netpay-reporte.service.js) — sin este default, cualquier test
  // de cargarReporte que no mockee Kore explícitamente revienta la destructuración de `raw`
  // dentro de consultarFolioKore (best-effort, no rompe el test, pero ensucia los logs). Forma
  // realista de "sin transacciones" en vez de `undefined` — cae al mismo NotFoundError ya
  // manejado ("puede que ya no esté disponible") que un test explícito usaría a propósito.
  buscarTransaccionesNetpay.mockResolvedValue({ raw: { Data: { transactions: [] } } });
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
    parseNetpayReporte.mockResolvedValue(parsedN1());
    NetpayReporte.findOne = jest.fn(() => fakeFind({ _id: 'existente' }));
    NetpayReporte.create = jest.fn();

    await expect(cargarReporte(Buffer.from(''), 'x.xlsx', USER)).rejects.toThrow(/Ya existe un reporte cargado/);
    expect(NetpayReporte.create).not.toHaveBeenCalled();
  });

  test('condición de carrera (índice único choca en el create pese al findOne previo): ConflictError, no un 500 crudo', async () => {
    parseNetpayReporte.mockResolvedValue(parsedN1());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const err = new Error('E11000 duplicate key');
    err.code = 11000;
    NetpayReporte.create = jest.fn().mockRejectedValue(err);

    await expect(cargarReporte(Buffer.from(''), 'x.xlsx', USER)).rejects.toThrow(ConflictError);
  });

  test('creado en discrepancia (nunca pendiente — ese valor ya no existe en el enum v2), luego evaluarReporte decide', async () => {
    parseNetpayReporte.mockResolvedValue(parsedN1());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    BankMovement.find = jest.fn(() => fakeFind([]));
    const creado = fakeReporteRecienCreado({ estatus: 'discrepancia' });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creado));

    const { reporte, candidatos } = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(candidatos).toEqual([]);
    expect(reporte.estatus).toBe('discrepancia');
    expect(reporte.motivoDiscrepancia).toBe('sin_candidato');
    expect(NetpayReporte.create).toHaveBeenCalledWith(expect.objectContaining({
      claveRastreo: 'CLAVE-1', estatus: 'discrepancia', nombreArchivoOriginal: 'archivo.xlsx',
      cargadoPor: { userId: 'user-1', nombre: 'Ana' },
    }));
  });

  // Recordatorio de carga (pedido explícito del usuario, 2026-10-08): avisa EN VIVO a quien
  // tenga Bancos abierto — un emit por ARCHIVO subido, sin importar cuántos depósitos traiga.
  test('emite netpay-reporte:cargado (N=1) con quién y cuándo', async () => {
    parseNetpayReporte.mockResolvedValue(parsedN1());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    BankMovement.find = jest.fn(() => fakeFind([]));
    const creado = fakeReporteRecienCreado({ estatus: 'discrepancia' });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creado));

    await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(emitToAll).toHaveBeenCalledWith('netpay-reporte:cargado', expect.objectContaining({
      cargadoPor: { userId: 'user-1', nombre: 'Ana' },
      nombreArchivoOriginal: 'archivo.xlsx',
    }));
  });

  // 2026-10-06, pedido explícito del usuario: koreCache ya NO depende de que alguien abra un
  // folio a mano o exporte el Excel — se completa acá mismo, durante la carga.
  test('completa koreCache de los folios durante la carga, antes de responder', async () => {
    parseNetpayReporte.mockResolvedValue(parsedN1());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    BankMovement.find = jest.fn(() => fakeFind([]));
    const creado = fakeReporteRecienCreado({ estatus: 'discrepancia' });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creado));
    const cuentaKore = { Id: 'CXC-1', Nombre: 'Cliente X' };
    buscarTransaccionesNetpay.mockResolvedValue({
      raw: { Data: { transactions: [{ folio: 'F1', cuentas: [cuentaKore] }] } },
    });

    await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(buscarTransaccionesNetpay).toHaveBeenCalledWith(
      expect.objectContaining({ folio: 'F1', withAccountInfo: true, status: 'completed' }),
    );
    expect(creado.folios[0].koreCache.cuenta).toEqual(cuentaKore);
    expect(creado.save).toHaveBeenCalled();
  });

  test('si Kore falla al completar koreCache durante la carga, la carga sigue siendo exitosa (best-effort)', async () => {
    parseNetpayReporte.mockResolvedValue(parsedN1());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    BankMovement.find = jest.fn(() => fakeFind([]));
    const creado = fakeReporteRecienCreado({ estatus: 'discrepancia' });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creado));
    buscarTransaccionesNetpay.mockRejectedValue(new Error('Kore caído'));

    const { reporte } = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(reporte.estatus).toBe('discrepancia');
    expect(creado.folios[0].koreCache?.cuenta).toBeFalsy();
  });

  // 2026-10-07, pedido explícito del usuario: sincroniza la comisión detectada hacia
  // Configuraciones Globales (netpay-comision-sync.service.js) — va ANTES de consultar Kore,
  // con el documento recién creado tal cual sale de NetpayReporte.create().
  test('sincroniza comisiones hacia Configuraciones Globales con el reporte recién creado', async () => {
    parseNetpayReporte.mockResolvedValue(parsedN1());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    BankMovement.find = jest.fn(() => fakeFind([]));
    const creado = fakeReporteRecienCreado({ estatus: 'discrepancia' });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creado));

    await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(sincronizarComisiones).toHaveBeenCalledWith(creado);
  });

  test('si sincronizarComisiones falla, la carga del reporte sigue siendo exitosa (best-effort)', async () => {
    parseNetpayReporte.mockResolvedValue(parsedN1());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    BankMovement.find = jest.fn(() => fakeFind([]));
    const creado = fakeReporteRecienCreado({ estatus: 'discrepancia' });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creado));
    sincronizarComisiones.mockRejectedValueOnce(new Error('Postgres caído'));

    const { reporte } = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(reporte.estatus).toBe('discrepancia');
  });

  test('1 candidato BBVA cuyo monto cuadra dentro de tolerancia: auto-vincula (resuelto_por_reporte, vinculo:erp-link)', async () => {
    parseNetpayReporte.mockResolvedValue(parsedN1({ montoDepositoTotal: 1000 }));
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const mov = { _id: 'mov-1', banco: 'BBVA', deposito: 1000.5 };
    BankMovement.find = jest.fn(() => fakeFind([mov]));
    const creado = fakeReporteRecienCreado({ montoDepositoTotal: 1000 });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creado));
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
    parseNetpayReporte.mockResolvedValue(parsedN1({ folios }));
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const creado = fakeReporteRecienCreado({ folios: folios.map(f => ({ ...f, duplicadoDeReporteId: null })) });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creado));

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
    parseNetpayReporte.mockResolvedValue(parsedN1({ folios }));
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const creado = fakeReporteRecienCreado({
      folios: folios.map(f => ({ ...f, duplicadoDeReporteId: null })),
    });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creado));

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
    parseNetpayReporte.mockResolvedValue(parsedN1());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const creado = fakeReporteRecienCreado();
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creado));

    const err = new Error('conexión perdida a Mongo');
    NetpayFolioRegistro.insertMany = jest.fn().mockRejectedValue(err);

    await expect(cargarReporte(Buffer.from(''), 'archivo.xlsx', USER)).rejects.toThrow('conexión perdida a Mongo');
  });

  // spec.md "Backward-compatible response shape for N=1": el `reporte` top-level se
  // mantiene para no romper consumidores existentes, PERO además viaja `reportes[]` con la
  // MISMA forma que usa el caso N>1 (un solo elemento, estatusCarga:'creado').
  test('N=1: la respuesta incluye TANTO reporte/candidatos (legacy) COMO reportes[] (forma nueva, 1 elemento)', async () => {
    parseNetpayReporte.mockResolvedValue(parsedN1());
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const creado = fakeReporteRecienCreado();
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creado));

    const resultado = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(resultado.reporte).toBeDefined();
    expect(resultado.reportes).toHaveLength(1);
    expect(resultado.reportes[0]).toEqual(expect.objectContaining({
      claveRastreo: 'CLAVE-1', estatusCarga: 'creado', sucursales: ['SUC-1'],
    }));
  });
});

// netpay-reporte-global (design.md "Loop" + "Duplicate" + "Per-deposit failure"):
// cargarReporte con N>1 depósitos — procesamiento SECUENCIAL e independiente por depósito,
// nunca 409/rethrow a nivel archivo (eso es exclusivo del camino N=1 de arriba).
describe('cargarReporte — múltiples depósitos (N>1)', () => {
  function unidad(overrides = {}) {
    return parsedFixture({
      claveRastreo: 'C1',
      folios: [{ referencia: 'F1', orderId: 'ORD-1', terminalID: 'T1', sucursal: 'SUC-1' }],
      ...overrides,
    });
  }

  test('mezcla creado/ya_cargado/error en una sola carga: cada depósito se clasifica independientemente, SIEMPRE 200 (nunca lanza)', async () => {
    const u1 = unidad({ claveRastreo: 'C1', montoDepositoTotal: 100 });
    const u2 = unidad({ claveRastreo: 'C2', montoDepositoTotal: 200 });
    const u3 = unidad({ claveRastreo: 'C3', montoDepositoTotal: 300 });
    parseNetpayReporte.mockResolvedValue({ depositos: [u1, u2, u3] });

    // C1: no existe, se crea con éxito.
    // C2: ya existe (ya_cargado).
    // C3: no existe, pero create() revienta con un error de negocio NO relacionado a duplicados.
    NetpayReporte.findOne = jest.fn(({ claveRastreo }) => {
      if (claveRastreo === 'C2') return fakeFind({ _id: 'rep-C2-existente' });
      return fakeFind(null);
    });
    const creadoC1 = fakeReporteRecienCreado({ _id: 'rep-C1', claveRastreo: 'C1' });
    NetpayReporte.create = jest.fn(async (doc) => {
      if (doc.claveRastreo === 'C3') throw new Error('validación de Mongo falló');
      return creadoC1;
    });
    NetpayReporte.findById = jest.fn(() => fakeQuery(creadoC1));

    const resultado = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(resultado.reportes).toHaveLength(3);
    const porClave = Object.fromEntries(resultado.reportes.map(r => [r.claveRastreo, r]));
    expect(porClave.C1.estatusCarga).toBe('creado');
    expect(porClave.C1.reporte).toBeDefined();
    expect(porClave.C2.estatusCarga).toBe('ya_cargado');
    expect(porClave.C2.reporteId).toBe('rep-C2-existente');
    expect(porClave.C3.estatusCarga).toBe('error');
    expect(porClave.C3.error).toMatch(/validación de Mongo falló/);

    expect(resultado.resumen).toEqual({ total: 3, creados: 1, yaCargados: 1, errores: 1 });
    // N>1 nunca expone el shape legacy de 1 solo reporte.
    expect(resultado.reporte).toBeUndefined();
    expect(resultado.candidatos).toBeUndefined();
    // Recordatorio de carga (2026-10-08): UN solo emit por archivo, sin importar cuántos
    // depósitos trajo ni cómo se clasificó cada uno.
    expect(emitToAll).toHaveBeenCalledTimes(1);
    expect(emitToAll).toHaveBeenCalledWith('netpay-reporte:cargado', expect.objectContaining({
      nombreArchivoOriginal: 'archivo.xlsx',
    }));
  });

  test('una falla en el depósito #2 NO bloquea al #3 — cada uno sigue su propio try/catch', async () => {
    const u1 = unidad({ claveRastreo: 'C1' });
    const u2 = unidad({ claveRastreo: 'C2' });
    const u3 = unidad({ claveRastreo: 'C3' });
    parseNetpayReporte.mockResolvedValue({ depositos: [u1, u2, u3] });

    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const creadoOk = fakeReporteRecienCreado({ _id: 'rep-ok' });
    NetpayReporte.create = jest.fn(async (doc) => {
      if (doc.claveRastreo === 'C2') throw new Error('boom en C2');
      return creadoOk;
    });
    NetpayReporte.findById = jest.fn(() => fakeQuery(creadoOk));

    const resultado = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(resultado.reportes.map(r => r.estatusCarga)).toEqual(['creado', 'error', 'creado']);
    expect(NetpayReporte.create).toHaveBeenCalledTimes(3); // C3 SÍ se intentó pese a la falla de C2
  });

  // design.md "Duplicate": findOne primero; si no encuentra nada pero el create choca con
  // el índice único (carrera entre dos cargas casi simultáneas), se reclasifica a
  // ya_cargado en vez de abortar ese depósito.
  test('condición de carrera (E11000 en create pese al findOne previo): se reclasifica a ya_cargado con el _id real', async () => {
    // 2 depósitos para forzar el camino N>1 — con N=1 esta misma carrera da 409 (ver
    // describe de arriba, test "condición de carrera" dentro de cargarReporte N=1).
    parseNetpayReporte.mockResolvedValue({ depositos: [unidad({ claveRastreo: 'C1' }), unidad({ claveRastreo: 'C2' })] });

    let primeraLlamadaC1 = true;
    NetpayReporte.findOne = jest.fn(({ claveRastreo }) => {
      if (claveRastreo === 'C2') return fakeFind(null);
      if (primeraLlamadaC1) { primeraLlamadaC1 = false; return fakeFind(null); }
      return fakeFind({ _id: 'rep-GANADOR-DE-LA-CARRERA' }); // 2da llamada para C1: tras el E11000
    });
    const creadoC2 = fakeReporteRecienCreado({ _id: 'rep-C2', claveRastreo: 'C2' });
    NetpayReporte.create = jest.fn(async (doc) => {
      if (doc.claveRastreo === 'C1') {
        const err = new Error('E11000 duplicate key');
        err.code = 11000;
        throw err;
      }
      return creadoC2;
    });
    NetpayReporte.findById = jest.fn(() => fakeQuery(creadoC2));

    const resultado = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    const c1 = resultado.reportes.find(r => r.claveRastreo === 'C1');
    expect(c1.estatusCarga).toBe('ya_cargado');
    expect(c1.reporteId).toBe('rep-GANADOR-DE-LA-CARRERA');
  });

  // design.md "Deposit with 0 folios": error POR DEPÓSITO ("sin folios"), zero writes para
  // ESE depósito — nunca rechazo de archivo (eso ya lo decide el parser para huérfanos, no
  // acá) y nunca bloquea al resto.
  test('depósito sin folios: error "sin folios", NUNCA llega a NetpayReporte.create', async () => {
    // 2 depósitos para forzar el camino N>1 (con N=1 el archivo completo ni siquiera tiene
    // folios que parsear, es un caso de error distinto cubierto por el parser).
    const sinFolios = unidad({ claveRastreo: 'C-VACIO', folios: [] });
    const otro = unidad({ claveRastreo: 'C-OK' });
    parseNetpayReporte.mockResolvedValue({ depositos: [sinFolios, otro] });
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const creadoOk = fakeReporteRecienCreado({ _id: 'rep-ok', claveRastreo: 'C-OK' });
    NetpayReporte.create = jest.fn().mockResolvedValue(creadoOk);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creadoOk));

    const resultado = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    const vacio = resultado.reportes.find(r => r.claveRastreo === 'C-VACIO');
    expect(vacio.estatusCarga).toBe('error');
    expect(vacio.error).toMatch(/folios/i);
    expect(NetpayReporte.create).toHaveBeenCalledTimes(1); // solo por C-OK, nunca por C-VACIO
    expect(NetpayReporte.findOne).toHaveBeenCalledTimes(1); // solo por C-OK — C-VACIO ni busca duplicado
  });

  test('sucursales/terminalIDs derivados de los folios del depósito, únicos y sin null', async () => {
    const u = unidad({
      claveRastreo: 'C1',
      folios: [
        { referencia: 'F1', terminalID: 'T1', sucursal: 'SUC-A' },
        { referencia: 'F2', terminalID: 'T1', sucursal: 'SUC-A' },
        { referencia: 'F3', terminalID: 'T2', sucursal: null },
      ],
    });
    parseNetpayReporte.mockResolvedValue({ depositos: [u] });
    NetpayReporte.findOne = jest.fn(() => fakeFind(null));
    const creado = fakeReporteRecienCreado({ _id: 'rep-1' });
    NetpayReporte.create = jest.fn().mockResolvedValue(creado);
    NetpayReporte.findById = jest.fn(() => fakeQuery(creado));

    const resultado = await cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    expect(resultado.reportes[0].sucursales).toEqual(['SUC-A']);
    expect(resultado.reportes[0].terminalIDs).toEqual(['T1', 'T2']);
  });

  // design.md "Loop": Sequential for...of, NUNCA Promise.all — "two deposits with the same
  // amount in the same window could both claim the same BankMovement" si corrieran en
  // paralelo. Se prueba dejando colgado el procesamiento del #1 (create() nunca resuelve
  // hasta que se libera a mano) y confirmando que el #2 NUNCA arranca mientras tanto.
  test('procesa los depósitos en orden SECUENCIAL — el #2 nunca arranca mientras el #1 sigue pendiente', async () => {
    const u1 = unidad({ claveRastreo: 'C1' });
    const u2 = unidad({ claveRastreo: 'C2' });
    parseNetpayReporte.mockResolvedValue({ depositos: [u1, u2] });

    const orden = [];
    NetpayReporte.findOne = jest.fn(({ claveRastreo }) => {
      orden.push(`findOne-${claveRastreo}`);
      return fakeFind(null);
    });

    let liberarC1;
    const creadoC1 = fakeReporteRecienCreado({ _id: 'rep-C1', claveRastreo: 'C1' });
    const creadoC2 = fakeReporteRecienCreado({ _id: 'rep-C2', claveRastreo: 'C2' });
    NetpayReporte.create = jest.fn(async (doc) => {
      orden.push(`create-start-${doc.claveRastreo}`);
      if (doc.claveRastreo === 'C1') {
        await new Promise((resolve) => { liberarC1 = resolve; });
      }
      orden.push(`create-end-${doc.claveRastreo}`);
      return doc.claveRastreo === 'C1' ? creadoC1 : creadoC2;
    });
    NetpayReporte.findById = jest.fn((id) => fakeQuery(id === 'rep-C1' ? creadoC1 : creadoC2));

    const promise = cargarReporte(Buffer.from(''), 'archivo.xlsx', USER);

    // Deja correr microtasks suficientes para que arranque el procesamiento del #1 y quede
    // colgado esperando `liberarC1` — sin que el #2 haya arrancado todavía.
    for (let i = 0; i < 10; i++) await Promise.resolve();
    expect(orden).toContain('create-start-C1');
    expect(orden).not.toContain('findOne-C2'); // la prueba real: Promise.all ya lo habría disparado acá

    liberarC1();
    const resultado = await promise;

    expect(orden.indexOf('create-end-C1')).toBeLessThan(orden.indexOf('findOne-C2'));
    expect(resultado.reportes.map(r => r.claveRastreo)).toEqual(['C1', 'C2']);
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

  // Fix 2026-10-07: antes de este fix, un reporte ya resuelto se tumbaba a discrepancia al
  // reevaluarlo porque su propio movimiento vinculado (erpLinks ya no vacío) quedaba fuera del
  // pool de "candidatos libres" de _buscarCandidatosParaReporte.
  test('reporte ya resuelto_por_reporte: Reevaluar es un no-op, no vuelve a buscar candidatos ni toca el estatus', async () => {
    const reporte = fakeReporteRecienCreado({
      estatus: 'resuelto_por_reporte', vinculo: 'erp-link', movementIdConfirmado: 'mov-1',
    });
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
    BankMovement.findById = jest.fn(() => fakeFind({ _id: 'mov-1', banco: 'BBVA', deposito: 1000 }));

    const { candidatos } = await evaluarReporte('rep-1');

    expect(candidatos).toEqual([]);
    expect(BankMovement.find).not.toHaveBeenCalled(); // nunca llega a _buscarCandidatosParaReporte
    expect(reporte.save).not.toHaveBeenCalled();
    expect(reporte.estatus).toBe('resuelto_por_reporte'); // intacto
    expect(reporte.movementIdConfirmado).toBe('mov-1'); // intacto
  });

  test('reporte ya resuelto_manual: Reevaluar también es un no-op', async () => {
    const reporte = fakeReporteRecienCreado({
      estatus: 'resuelto_manual', movementIdConfirmado: 'mov-1', justificacion: 'ya resuelto a mano',
    });
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
    BankMovement.findById = jest.fn(() => fakeFind({ _id: 'mov-1', banco: 'BBVA', deposito: 1000 }));

    await evaluarReporte('rep-1');

    expect(BankMovement.find).not.toHaveBeenCalled();
    expect(reporte.save).not.toHaveBeenCalled();
    expect(reporte.estatus).toBe('resuelto_manual');
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

  // "Identificado por" en Bancos (pedido explícito del usuario, 2026-10-08): debe mostrar a
  // quien SUBIÓ el reporte, no el nombre sintético del motor — pero el rol que viaja a
  // setErpIds sigue siendo 'admin' (bypass de permisos de la escritura interna/automática,
  // nunca el rol real del uploader).
  test('auto-vincula con el nombre/id de quien SUBIÓ el reporte (cargadoPor), rol admin preservado para el bypass interno', async () => {
    const reporte = fakeReporteRecienCreado({
      montoDepositoTotal: 1000,
      cargadoPor: { userId: 'auth0|uploader-1', nombre: 'jesuscruz' },
    });
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
    const mov = { _id: 'mov-1', banco: 'BBVA', deposito: 1000 };
    BankMovement.find = jest.fn(() => fakeFind([mov]));
    setErpIds.mockResolvedValue({ _id: 'mov-1', banco: 'BBVA' });
    NetpayMatch.find = jest.fn(() => fakeFind([]));

    await evaluarReporte('rep-1');

    expect(setErpIds).toHaveBeenCalledWith(
      'mov-1',
      expect.any(Array),
      { _id: 'auth0|uploader-1', role: 'admin', nombre: 'jesuscruz' },
      { guardSinVinculos: true },
    );
    // confirmadoPor del REPORTE sigue documentando que lo resolvió el motor automático —
    // eso es un hecho distinto de "quién aparece en Bancos" y no debe cambiar.
    expect(reporte.confirmadoPor.nombre).toBe('Motor de Evaluación de Reportes Netpay (automático)');
  });

  test('reporte sin cargadoPor (histórico/legacy): cae al nombre sintético del motor, sin romper', async () => {
    const reporte = fakeReporteRecienCreado({ montoDepositoTotal: 1000 });
    NetpayReporte.findById = jest.fn().mockResolvedValue(reporte);
    const mov = { _id: 'mov-1', banco: 'BBVA', deposito: 1000 };
    BankMovement.find = jest.fn(() => fakeFind([mov]));
    setErpIds.mockResolvedValue({ _id: 'mov-1', banco: 'BBVA' });
    NetpayMatch.find = jest.fn(() => fakeFind([]));

    await evaluarReporte('rep-1');

    expect(setErpIds).toHaveBeenCalledWith(
      'mov-1',
      expect.any(Array),
      { _id: 'motor-netpay-reporte', role: 'admin', nombre: 'Motor de Evaluación de Reportes Netpay (automático)' },
      { guardSinVinculos: true },
    );
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

  // dateFrom/dateTo (pedido explícito del usuario, 2026-10-07): filtra por fechaMovimiento,
  // $lte a FIN de día (no medianoche) porque fechaMovimiento puede traer hora real.
  test('dateFrom/dateTo: filtra por fechaMovimiento, combinado con estatus/incluirEliminados', async () => {
    const sortFn = jest.fn(() => fakeFind([]));
    NetpayReporte.find = jest.fn(() => ({ sort: sortFn }));

    await listar({
      estatus: 'resuelto_por_reporte', incluirEliminados: true, dateFrom: '2026-09-01', dateTo: '2026-09-30',
    });

    expect(NetpayReporte.find).toHaveBeenCalledWith({
      estatus: 'resuelto_por_reporte',
      fechaMovimiento: {
        $gte: new Date('2026-09-01T00:00:00.000Z'),
        $lte: new Date('2026-09-30T23:59:59.999Z'),
      },
    });
  });

  // Fix de revisión de riesgo (2026-10-07): antes un dateFrom/dateTo no parseable pasaba como
  // Invalid Date directo al filtro Mongo, sin aviso.
  test('dateFrom inválido: BadRequestError, nunca llega a tocar Mongo', async () => {
    NetpayReporte.find = jest.fn();

    await expect(listar({ dateFrom: 'no-es-una-fecha' })).rejects.toThrow(BadRequestError);
    expect(NetpayReporte.find).not.toHaveBeenCalled();
  });

  test('dateTo inválido: BadRequestError, nunca llega a tocar Mongo', async () => {
    NetpayReporte.find = jest.fn();

    await expect(listar({ dateTo: 'tampoco-una-fecha' })).rejects.toThrow(BadRequestError);
    expect(NetpayReporte.find).not.toHaveBeenCalled();
  });

  // search (pedido explícito del usuario, 2026-10-08): clave de rastreo o importe, necesario
  // una vez que la lista se agrupa por archivo en el frontend.
  describe('search', () => {
    test('texto: filtra por claveRastreo (regex, case-insensitive), combinado con el resto', async () => {
      const sortFn = jest.fn(() => fakeFind([]));
      NetpayReporte.find = jest.fn(() => ({ sort: sortFn }));

      await listar({ estatus: 'resuelto_por_reporte', search: 'A0-123' });

      expect(NetpayReporte.find).toHaveBeenCalledWith({
        estatus: 'resuelto_por_reporte',
        eliminado: { $ne: true },
        $or: [{ claveRastreo: expect.any(RegExp) }],
      });
    });

    test('número sin decimales: agrega tolerancia ±1 sobre montoDepositoTotal', async () => {
      const sortFn = jest.fn(() => fakeFind([]));
      NetpayReporte.find = jest.fn(() => ({ sort: sortFn }));

      await listar({ search: '69558' });

      const filtroUsado = NetpayReporte.find.mock.calls[0][0];
      expect(filtroUsado.$or).toContainEqual({ montoDepositoTotal: { $gte: 69557, $lte: 69559 } });
    });

    test('número con 2 decimales: tolerancia ±0.005', async () => {
      const sortFn = jest.fn(() => fakeFind([]));
      NetpayReporte.find = jest.fn(() => ({ sort: sortFn }));

      await listar({ search: '69558.26' });

      const filtroUsado = NetpayReporte.find.mock.calls[0][0];
      const clausulaMonto = filtroUsado.$or.find(c => 'montoDepositoTotal' in c);
      expect(clausulaMonto.montoDepositoTotal.$gte).toBeCloseTo(69558.255, 5);
      expect(clausulaMonto.montoDepositoTotal.$lte).toBeCloseTo(69558.265, 5);
    });

    test('sin search: no agrega $or', async () => {
      const sortFn = jest.fn(() => fakeFind([]));
      NetpayReporte.find = jest.fn(() => ({ sort: sortFn }));

      await listar({});

      expect(NetpayReporte.find).toHaveBeenCalledWith({ eliminado: { $ne: true } });
    });
  });
});

describe('obtenerUltimaCarga', () => {
  test('hay reportes: trae cargadoEn/cargadoPor/nombreArchivoOriginal del más reciente', async () => {
    const selectFn = jest.fn(() => fakeFind({
      cargadoEn: new Date('2026-10-08T15:04:52.999Z'),
      cargadoPor: { userId: 'auth0|1', nombre: 'jesuscruz' },
      nombreArchivoOriginal: '60718_91791_07102026_07102026_DetalleDepositos.xlsx',
    }));
    const sortFn = jest.fn(() => ({ select: selectFn }));
    NetpayReporte.findOne = jest.fn(() => ({ sort: sortFn }));

    const { ultimaCarga } = await obtenerUltimaCarga();

    expect(NetpayReporte.findOne).toHaveBeenCalledWith({});
    expect(sortFn).toHaveBeenCalledWith({ cargadoEn: -1 });
    expect(ultimaCarga).toEqual({
      cargadoEn: new Date('2026-10-08T15:04:52.999Z'),
      cargadoPor: { userId: 'auth0|1', nombre: 'jesuscruz' },
      nombreArchivoOriginal: '60718_91791_07102026_07102026_DetalleDepositos.xlsx',
    });
  });

  test('sin ningún reporte cargado todavía: ultimaCarga null', async () => {
    const selectFn = jest.fn(() => fakeFind(null));
    NetpayReporte.findOne = jest.fn(() => ({ sort: jest.fn(() => ({ select: selectFn })) }));

    const { ultimaCarga } = await obtenerUltimaCarga();

    expect(ultimaCarga).toBeNull();
  });
});

describe('obtenerDetalle', () => {
  test('no existe: NotFoundError', async () => {
    NetpayReporte.findById = jest.fn(() => fakeFind(null));
    await expect(obtenerDetalle('x')).rejects.toThrow(NotFoundError);
  });

  test('existe, sin movimiento vinculado: lo devuelve con movimientoVinculado:null', async () => {
    NetpayReporte.findById = jest.fn(() => fakeFind({ _id: 'r1', movementIdConfirmado: null }));
    const { reporte } = await obtenerDetalle('r1');
    expect(reporte).toEqual({ _id: 'r1', movementIdConfirmado: null, movimientoVinculado: null });
  });

  test('existe, con movimiento vinculado: lo devuelve con movimientoVinculado poblado (banco/fecha/monto)', async () => {
    NetpayReporte.findById = jest.fn(() => fakeFind({ _id: 'r1', movementIdConfirmado: 'mov-1' }));
    BankMovement.findById = jest.fn(() => fakeFind({
      _id: 'mov-1', banco: 'BBVA', fecha: new Date('2026-09-29T00:00:00.000Z'), deposito: 503646.17,
    }));
    const { reporte } = await obtenerDetalle('r1');
    expect(BankMovement.findById).toHaveBeenCalledWith('mov-1');
    expect(reporte.movimientoVinculado).toEqual({
      banco: 'BBVA', fecha: new Date('2026-09-29T00:00:00.000Z'), monto: 503646.17,
    });
  });

  test('movimiento vinculado ya no existe: movimientoVinculado:null, nunca lanza', async () => {
    NetpayReporte.findById = jest.fn(() => fakeFind({ _id: 'r1', movementIdConfirmado: 'mov-borrado' }));
    BankMovement.findById = jest.fn(() => fakeFind(null));
    const { reporte } = await obtenerDetalle('r1');
    expect(reporte.movimientoVinculado).toBeNull();
  });
});

describe('_poblarMovimientoVinculado', () => {
  test('reporte null/undefined: lo devuelve tal cual, sin llamar a BankMovement', async () => {
    expect(await _poblarMovimientoVinculado(null)).toBeNull();
    expect(BankMovement.findById).not.toHaveBeenCalled();
  });

  test('sin movementIdConfirmado: movimientoVinculado:null, sin llamar a BankMovement', async () => {
    const resultado = await _poblarMovimientoVinculado({ _id: 'r1', movementIdConfirmado: null });
    expect(resultado).toEqual({ _id: 'r1', movementIdConfirmado: null, movimientoVinculado: null });
    expect(BankMovement.findById).not.toHaveBeenCalled();
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

// consultarFoliosPendientesDeLote — export-lote (pedido explícito del usuario, 2026-10-07):
// versión "N reportes" de consultarFoliosPendientes de arriba, usada por GET
// .../reporte/export-lote. 2 correcciones de revisión de resiliencia (2026-10-07) cubiertas
// acá: (1) el presupuesto de tiempo escala con la cantidad REAL de lotes de folios pendientes
// de cada reporte (no asume 1 lote fijo), (2) un reporte que falla individualmente NUNCA
// aborta el resto del lote.
describe('consultarFoliosPendientesDeLote', () => {
  function fakeQuery(result) {
    return {
      lean: jest.fn().mockResolvedValue(result),
      then: (resolve, reject) => Promise.resolve(result).then(resolve, reject),
    };
  }

  test('caso feliz: reportes sin pendientes se consultan sin omitirse ni fallar', async () => {
    const reporteA = { _id: 'rep-a', folios: [{ referencia: 'A1', koreCache: { cuenta: { Total: 1 } } }] };
    const reporteB = { _id: 'rep-b', folios: [{ referencia: 'B1', koreCache: { cuenta: { Total: 1 } } }] };
    NetpayReporte.find = jest.fn(() => ({ select: () => fakeFind([reporteA, reporteB]) }));
    NetpayReporte.findById = jest.fn(id => fakeQuery(id === 'rep-a' ? reporteA : reporteB));

    const resultado = await consultarFoliosPendientesDeLote(['rep-a', 'rep-b']);

    expect(resultado).toEqual({ fallos: [], omitidosPorTiempo: [], reportesFallidos: [] });
    expect(buscarTransaccionesNetpay).not.toHaveBeenCalled();
  });

  test('ids vacío: no toca Mongo, devuelve todo vacío', async () => {
    NetpayReporte.find = jest.fn(() => ({ select: () => fakeFind([]) }));

    const resultado = await consultarFoliosPendientesDeLote([]);

    expect(resultado).toEqual({ fallos: [], omitidosPorTiempo: [], reportesFallidos: [] });
  });

  // Fix 1 (BLOCKER, revisión de resiliencia 2026-10-07): un reporte con 6 folios pendientes
  // necesita 2 lotes de 5 (CONSULTAR_FOLIOS_CONCURRENCIA) — su peor caso teórico (2 lotes)
  // YA excede por sí solo el presupuesto del lote completo, así que debe omitirse por tiempo
  // ANTES de intentarlo siquiera (nunca llega a NetpayReporte.findById con su id). Esto es
  // determinístico sin fake timers: el check compara contra el peor caso teórico escalado por
  // cantidad de lotes, no contra tiempo real transcurrido. Antes del fix, la fórmula vieja
  // comparaba solo el peor caso de UN lote (que SÍ entra en el presupuesto), así que este
  // reporte habría arrancado igual y corrido sin ningún control de tiempo interno.
  test('un reporte con muchos folios pendientes (2+ lotes) se omite por presupuesto de tiempo, uno con 0 pendientes nunca se omite', async () => {
    const reporteGrande = {
      _id: 'rep-grande',
      folios: Array.from({ length: 6 }, (_, i) => ({ referencia: `G${i}`, koreCache: null })),
    };
    const reporteChico = {
      _id: 'rep-chico',
      folios: [{ referencia: 'C1', koreCache: { cuenta: { Total: 1 } } }],
    };
    NetpayReporte.find = jest.fn(() => ({ select: () => fakeFind([reporteGrande, reporteChico]) }));
    NetpayReporte.findById = jest.fn(() => fakeQuery(reporteChico));

    const resultado = await consultarFoliosPendientesDeLote(['rep-grande', 'rep-chico']);

    expect(resultado.omitidosPorTiempo).toEqual(['rep-grande']);
    expect(resultado.reportesFallidos).toEqual([]);
    expect(NetpayReporte.findById).not.toHaveBeenCalledWith('rep-grande');
  });

  // Fix 2 (CRITICAL, revisión de resiliencia 2026-10-07): antes, un error de
  // consultarFoliosPendientes para UN reporte (acá simulado como "ya no existe" — ej. borrado
  // concurrente por otro usuario a mitad de la corrida) abortaba TODO el lote sin try/catch,
  // perdiendo el trabajo ya hecho de los reportes anteriores.
  test('un reporte que falla individualmente se registra en reportesFallidos, sin abortar el resto del lote', async () => {
    const reporteB = {
      _id: 'rep-b', folios: [{ referencia: 'B1', koreCache: null }], save: jest.fn().mockResolvedValue(undefined),
    };
    NetpayReporte.find = jest.fn(() => ({ select: () => fakeFind([{ _id: 'rep-a', folios: [] }, reporteB]) }));
    NetpayReporte.findById = jest.fn(id => (id === 'rep-a' ? fakeQuery(null) : fakeQuery(reporteB)));
    buscarTransaccionesNetpay.mockResolvedValue({ raw: { Data: { transactions: [{ folio: 'B1', cuentas: [{ Total: 1 }] }] } } });

    const resultado = await consultarFoliosPendientesDeLote(['rep-a', 'rep-b']);

    expect(resultado.reportesFallidos).toEqual([{ reporteId: 'rep-a', error: expect.stringContaining('Reporte Netpay') }]);
    expect(resultado.omitidosPorTiempo).toEqual([]);
    // rep-b SÍ se procesó pese a que rep-a falló antes en el loop.
    expect(buscarTransaccionesNetpay).toHaveBeenCalledWith(expect.objectContaining({ folio: 'B1' }));
  });

  test('techo de folios pendientes del lote completo: UnprocessableError, nunca llega a consultar nada', async () => {
    const { UnprocessableError } = require('../../shared/errors/AppError');
    const reporteEnorme = {
      _id: 'rep-enorme',
      folios: Array.from({ length: 301 }, (_, i) => ({ referencia: `E${i}`, koreCache: null })),
    };
    NetpayReporte.find = jest.fn(() => ({ select: () => fakeFind([reporteEnorme]) }));
    NetpayReporte.findById = jest.fn();

    await expect(consultarFoliosPendientesDeLote(['rep-enorme'])).rejects.toThrow(UnprocessableError);
    expect(NetpayReporte.findById).not.toHaveBeenCalled();
  });
});
