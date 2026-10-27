'use strict';

// collection-request.create.test.js — create(): no tenía NINGÚN test hasta
// ahora (confirmado por investigación previa en esta sesión), pese a ser el
// endpoint que Kore llama (POST /api/collection-requests) para CADA solicitud
// de cobro. Mismo patrón de mocks que collection-request.identificar.test.js/
// collection-request.rechazar.test.js (mismo dominio) — se mockean solo los
// límites de I/O que create() realmente toca: el modelo Mongoose
// (CollectionRequest) y el servicio de Drive (subida de comprobantes) y el
// socket (aviso en tiempo real). create() NO toca Kore/BankMovement/mongo-tx
// en absoluto, así que no hace falta mockearlos acá (a diferencia de
// identificar()). parseCxcs/parseFormasPago/tipoSaldoEspecial corren REALES
// (son funciones puras) — no mockearlas sería acoplar el test a su
// implementación interna sin necesidad.

jest.mock('./CollectionRequest.model');
jest.mock('./drive-comprobantes.service');
jest.mock('../../shared/socket');

const CollectionRequest  = require('./CollectionRequest.model');
const driveComprobantes  = require('./drive-comprobantes.service');
const { emitToAll }      = require('../../shared/socket');

const service = require('./collection-request.service');

// ── Helpers de fixtures ──────────────────────────────────────────────────────

function validData(overrides = {}) {
  return {
    cxcs: [{ erpId: 'CXC-1' }],
    formasPago: [{ formaPagoId: 'fp-1', formaPagoDescripcion: 'Transferencia', importe: 1000 }],
    conceptoId: 'concepto-1',
    solicitudIdErp: 'SOL-1',
    usuarioSolicitanteId: 'cajero-1',
    usuarioSolicitanteNombre: 'Juan Pérez',
    ...overrides,
  };
}

function multerFile(originalname, overrides = {}) {
  return { originalname, mimetype: 'image/jpeg', buffer: Buffer.from('fake'), ...overrides };
}

beforeEach(() => {
  jest.clearAllMocks();
  // Sin solicitud previa por default — cada test de idempotencia/carrera la
  // sobreescribe explícitamente.
  CollectionRequest.findOne.mockResolvedValue(null);
  // Doc "creado" = lo que se le pasó + _id — sin .toObject() (_toSafeJSON cae
  // al branch `doc.toObject ? ... : obj`, igual de válido y más simple acá).
  CollectionRequest.create.mockImplementation(async (doc) => ({ ...doc, _id: 'cr-new' }));
  driveComprobantes.subirComprobante.mockResolvedValue({ driveFileId: 'drive-1', driveWebViewLink: 'https://drive/1' });
});

// ── Validación de campos ─────────────────────────────────────────────────────

describe('create() — validación de campos requeridos', () => {
  test('sin usuarioSolicitanteId -> BadRequestError, nunca llega a Mongo', async () => {
    const data = validData({ usuarioSolicitanteId: undefined });
    await expect(service.create(data)).rejects.toThrow(/usuarioSolicitanteId es requerido/);
    expect(CollectionRequest.create).not.toHaveBeenCalled();
  });

  test('solicitudIdErp ausente -> BadRequestError, nunca llega a Mongo', async () => {
    const data = validData({ solicitudIdErp: undefined });
    await expect(service.create(data)).rejects.toThrow(/solicitudIdErp es requerido/);
    expect(CollectionRequest.create).not.toHaveBeenCalled();
  });

  test('solicitudIdErp solo espacios (tras trim queda vacío) -> BadRequestError', async () => {
    const data = validData({ solicitudIdErp: '   ' });
    await expect(service.create(data)).rejects.toThrow(/solicitudIdErp es requerido/);
    expect(CollectionRequest.create).not.toHaveBeenCalled();
  });

  test('un CxC sin erpId -> BadRequestError', async () => {
    const data = validData({ cxcs: [{ erpId: '' }] });
    await expect(service.create(data)).rejects.toThrow(/Cada CxC requiere erpId/);
    expect(CollectionRequest.create).not.toHaveBeenCalled();
  });

  test('forma de pago sin formaPagoId -> BadRequestError', async () => {
    const data = validData({ formasPago: [{ formaPagoId: '', formaPagoDescripcion: 'Efectivo', importe: 100 }] });
    await expect(service.create(data)).rejects.toThrow(/Cada forma de pago requiere formaPagoId/);
  });

  test('forma de pago sin formaPagoDescripcion -> BadRequestError', async () => {
    const data = validData({ formasPago: [{ formaPagoId: 'fp-1', formaPagoDescripcion: '', importe: 100 }] });
    await expect(service.create(data)).rejects.toThrow(/Cada forma de pago requiere formaPagoId/);
  });

  test('forma de pago con importe <= 0 -> BadRequestError', async () => {
    const data = validData({ formasPago: [{ formaPagoId: 'fp-1', formaPagoDescripcion: 'Efectivo', importe: 0 }] });
    await expect(service.create(data)).rejects.toThrow(/Cada forma de pago requiere formaPagoId/);
  });
});

// ── Idempotencia — el corazón de la función para Kore ───────────────────────

describe('create() — idempotencia por solicitudIdErp', () => {
  test('solicitudIdErp ya existe -> regresa el doc existente, nunca crea, nunca sube comprobantes, nunca avisa por socket', async () => {
    const existente = { _id: 'cr-vieja', solicitudIdErp: 'SOL-1', monto: 500 };
    CollectionRequest.findOne.mockResolvedValue(existente);
    const data = validData();

    const resultado = await service.create(data, [multerFile('comprobante.jpg')]);

    expect(resultado).toEqual(existente);
    expect(CollectionRequest.create).not.toHaveBeenCalled();
    // Comportamiento ACTUAL (no necesariamente el ideal): un reintento de Kore
    // con comprobantes nuevos NO los sube — el chequeo de idempotencia corta
    // antes de llegar a esa parte del código.
    expect(driveComprobantes.subirComprobante).not.toHaveBeenCalled();
    // Comportamiento ACTUAL: tampoco se vuelve a avisar por socket en un
    // reintento — solo se emite en la creación nueva real (ver más abajo).
    expect(emitToAll).not.toHaveBeenCalled();
  });

  test('carrera de índice único (11000) -> recupera y regresa el documento ganador, no propaga el error crudo', async () => {
    CollectionRequest.findOne
      .mockResolvedValueOnce(null)               // chequeo inicial: no existe todavía
      .mockResolvedValueOnce({ _id: 'cr-ganador', solicitudIdErp: 'SOL-1' }); // recuperación post-11000
    const dupErr = new Error('E11000 duplicate key');
    dupErr.code = 11000;
    CollectionRequest.create.mockRejectedValue(dupErr);

    const resultado = await service.create(validData());

    expect(resultado).toEqual({ _id: 'cr-ganador', solicitudIdErp: 'SOL-1' });
    expect(CollectionRequest.findOne).toHaveBeenCalledTimes(2);
  });

  test('carrera de índice único (11000) pero la recuperación no encuentra nada -> propaga el error original', async () => {
    CollectionRequest.findOne
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce(null); // recuperación tampoco encuentra nada (caso raro/teórico)
    const dupErr = new Error('E11000 duplicate key');
    dupErr.code = 11000;
    CollectionRequest.create.mockRejectedValue(dupErr);

    await expect(service.create(validData())).rejects.toBe(dupErr);
  });

  test('error de Mongo que NO es 11000 -> se propaga tal cual, sin intentar recuperar nada', async () => {
    const otroError = new Error('Mongo connection lost');
    CollectionRequest.create.mockRejectedValue(otroError);

    await expect(service.create(validData())).rejects.toBe(otroError);
    // Solo el chequeo inicial de idempotencia — nunca un segundo findOne de
    // recuperación, porque el catch solo reacciona a err.code === 11000.
    expect(CollectionRequest.findOne).toHaveBeenCalledTimes(1);
  });
});

// ── Saldo especial / anticipo (usa tipoSaldoEspecial REAL) ──────────────────

describe('create() — formas de pago de saldo a favor / anticipo', () => {
  test('"Saldo a favor" sin saldosAplicados -> BadRequestError', async () => {
    const data = validData({
      formasPago: [{ formaPagoId: 'fp-1', formaPagoDescripcion: 'Saldo a favor', importe: 500 }],
    });
    await expect(service.create(data)).rejects.toThrow(/requiere saldosAplicados/);
    expect(CollectionRequest.create).not.toHaveBeenCalled();
  });

  test('saldosAplicados con un item sin id -> BadRequestError', async () => {
    const data = validData({
      formasPago: [{
        formaPagoId: 'fp-1', formaPagoDescripcion: 'Saldo a favor', importe: 500,
        saldosAplicados: [{ id: '', monto: 500 }],
      }],
    });
    await expect(service.create(data)).rejects.toThrow(/requiere saldosAplicados/);
  });

  test('saldosAplicados con monto <= 0 -> BadRequestError', async () => {
    const data = validData({
      formasPago: [{
        formaPagoId: 'fp-1', formaPagoDescripcion: 'Anticipo', importe: 500,
        saldosAplicados: [{ id: 'saldo-1', monto: 0 }],
      }],
    });
    await expect(service.create(data)).rejects.toThrow(/requiere saldosAplicados/);
  });

  test('suma de saldosAplicados no coincide con el importe -> BadRequestError', async () => {
    const data = validData({
      formasPago: [{
        formaPagoId: 'fp-1', formaPagoDescripcion: 'Saldo a favor', importe: 500,
        saldosAplicados: [{ id: 'saldo-1', monto: 300 }],
      }],
    });
    await expect(service.create(data)).rejects.toThrow(/requiere saldosAplicados/);
  });

  test('saldosAplicados válido y la suma coincide -> crea normalmente', async () => {
    const data = validData({
      formasPago: [{
        formaPagoId: 'fp-1', formaPagoDescripcion: 'Saldo a favor', importe: 500,
        saldosAplicados: [{ id: 'saldo-1', monto: 500 }],
      }],
    });
    await expect(service.create(data)).resolves.toBeDefined();
    expect(CollectionRequest.create).toHaveBeenCalledTimes(1);
  });
});

// ── Modo 2 (varias CxC, siempre del mismo cliente) ──────────────────────────

describe('create() — Modo 2 (varias CxC)', () => {
  test('2+ CxCs con más de 1 forma de pago -> BadRequestError', async () => {
    const data = validData({
      cxcs: [{ erpId: 'CXC-1', montoAsignado: 600 }, { erpId: 'CXC-2', montoAsignado: 400 }],
      formasPago: [
        { formaPagoId: 'fp-1', formaPagoDescripcion: 'Transferencia', importe: 600 },
        { formaPagoId: 'fp-2', formaPagoDescripcion: 'Efectivo', importe: 400 },
      ],
    });
    await expect(service.create(data)).rejects.toThrow(/Modo 2.*una forma de pago/);
    expect(CollectionRequest.create).not.toHaveBeenCalled();
  });

  test('2+ CxCs con 1 forma de pago pero una CxC sin montoAsignado -> BadRequestError', async () => {
    const data = validData({
      cxcs: [{ erpId: 'CXC-1', montoAsignado: 600 }, { erpId: 'CXC-2' }],
      formasPago: [{ formaPagoId: 'fp-1', formaPagoDescripcion: 'Transferencia', importe: 1000 }],
    });
    await expect(service.create(data)).rejects.toThrow(/Modo 2, cada CxC requiere montoAsignado/);
    expect(CollectionRequest.create).not.toHaveBeenCalled();
  });

  test('2+ CxCs, 1 forma de pago, todas con montoAsignado > 0 -> crea normalmente (caso feliz Modo 2)', async () => {
    const data = validData({
      cxcs: [{ erpId: 'CXC-1', montoAsignado: 600 }, { erpId: 'CXC-2', montoAsignado: 400 }],
      formasPago: [{ formaPagoId: 'fp-1', formaPagoDescripcion: 'Transferencia', importe: 1000 }],
    });
    await expect(service.create(data)).resolves.toBeDefined();
    expect(CollectionRequest.create).toHaveBeenCalledTimes(1);
    const docCreado = CollectionRequest.create.mock.calls[0][0];
    expect(docCreado.cxcs).toHaveLength(2);
  });
});

// ── Cálculo de monto y defaults ──────────────────────────────────────────────

describe('create() — cálculo de monto y descripción default', () => {
  test('monto = suma de formasPago[].importe, redondeado a 2 decimales (sin arrastre de punto flotante)', async () => {
    const data = validData({
      formasPago: [
        { formaPagoId: 'fp-1', formaPagoDescripcion: 'Efectivo', importe: 0.1 },
        { formaPagoId: 'fp-2', formaPagoDescripcion: 'Efectivo', importe: 0.2 },
      ],
    });
    await service.create(data);
    const docCreado = CollectionRequest.create.mock.calls[0][0];
    expect(docCreado.monto).toBe(0.3); // 0.1+0.2 crudo en JS da 0.30000000000000004
  });

  test('sin descripcion -> genera default con usuarioSolicitanteNombre + fecha de hoy (ISO, YYYY-MM-DD)', async () => {
    const data = validData({ descripcion: undefined, usuarioSolicitanteNombre: 'Juan Pérez' });
    await service.create(data);
    const docCreado = CollectionRequest.create.mock.calls[0][0];
    const hoy = new Date().toISOString().slice(0, 10);
    expect(docCreado.descripcion).toBe(`Solicitud de cobro de Juan Pérez con fecha ${hoy}`);
  });

  test('sin descripcion NI usuarioSolicitanteNombre -> usa usuarioSolicitanteId en el default', async () => {
    const data = validData({ descripcion: undefined, usuarioSolicitanteNombre: undefined, usuarioSolicitanteId: 'cajero-99' });
    await service.create(data);
    const docCreado = CollectionRequest.create.mock.calls[0][0];
    const hoy = new Date().toISOString().slice(0, 10);
    expect(docCreado.descripcion).toBe(`Solicitud de cobro de cajero-99 con fecha ${hoy}`);
  });

  test('con descripcion explícita -> se usa tal cual, trimmeada', async () => {
    const data = validData({ descripcion: '  Cobro de mostrador  ' });
    await service.create(data);
    const docCreado = CollectionRequest.create.mock.calls[0][0];
    expect(docCreado.descripcion).toBe('Cobro de mostrador');
  });
});

// ── Lealtad (2026-10-27, opcional): telefono a nivel solicitud, subtotal por CxC ──

describe('create() — campos opcionales de Lealtad (telefono/subtotal)', () => {
  test('con telefono en el body -> se guarda tal cual en el doc creado', async () => {
    const data = validData({ telefono: '  5512345678  ' });
    await service.create(data);
    const docCreado = CollectionRequest.create.mock.calls[0][0];
    expect(docCreado.telefono).toBe('5512345678');
  });

  test('sin telefono -> el doc se crea igual, sin error, campo queda null', async () => {
    const resultado = await service.create(validData());
    expect(resultado._id).toBe('cr-new');
    const docCreado = CollectionRequest.create.mock.calls[0][0];
    expect(docCreado.telefono).toBeNull();
  });

  test('un CxC con subtotal -> se parsea y persiste junto al resto de la CxC', async () => {
    const data = validData({ cxcs: [{ erpId: 'CXC-1', serie: 'A0', folioExterno: '123', subtotal: 431.03 }] });
    await service.create(data);
    const docCreado = CollectionRequest.create.mock.calls[0][0];
    expect(docCreado.cxcs[0]).toMatchObject({ erpId: 'CXC-1', serie: 'A0', folioExterno: '123', subtotal: 431.03 });
  });

  test('CxC sin subtotal -> queda null, no truena', async () => {
    await service.create(validData());
    const docCreado = CollectionRequest.create.mock.calls[0][0];
    expect(docCreado.cxcs[0].subtotal).toBeNull();
  });
});

// ── Comprobantes (suben a Drive ANTES de crear el documento) ────────────────

describe('create() — comprobantes', () => {
  test('sin files -> comprobantes: [], nunca sube nada a Drive', async () => {
    await service.create(validData());
    expect(driveComprobantes.subirComprobante).not.toHaveBeenCalled();
    const docCreado = CollectionRequest.create.mock.calls[0][0];
    expect(docCreado.comprobantes).toEqual([]);
  });

  test('1 archivo, Modo 1 (1 CxC con serie+folioExterno) -> nombre en Drive = serie-folioExterno, sin sufijo', async () => {
    const data = validData({ cxcs: [{ erpId: 'CXC-1', serie: 'A0', folioExterno: '260605320' }] });
    await service.create(data, [multerFile('recibo.jpg')]);

    expect(driveComprobantes.subirComprobante).toHaveBeenCalledTimes(1);
    const [, , driveName] = driveComprobantes.subirComprobante.mock.calls[0];
    expect(driveName).toBe('A0-260605320.jpg');
  });

  test('2 archivos, Modo 1 -> mismo nombre base con sufijo -1/-2', async () => {
    const data = validData({ cxcs: [{ erpId: 'CXC-1', serie: 'A0', folioExterno: '260605320' }] });
    await service.create(data, [multerFile('a.jpg'), multerFile('b.png', { mimetype: 'image/png' })]);

    expect(driveComprobantes.subirComprobante).toHaveBeenCalledTimes(2);
    expect(driveComprobantes.subirComprobante.mock.calls[0][2]).toBe('A0-260605320-1.jpg');
    expect(driveComprobantes.subirComprobante.mock.calls[1][2]).toBe('A0-260605320-2.png');
  });

  test('archivos en Modo 2 (varias CxC, sin un único serie-folio) -> nombre en Drive usa solicitudIdErp', async () => {
    const data = validData({
      solicitudIdErp: 'SOL-MODO2',
      cxcs: [{ erpId: 'CXC-1', montoAsignado: 600 }, { erpId: 'CXC-2', montoAsignado: 400 }],
      formasPago: [{ formaPagoId: 'fp-1', formaPagoDescripcion: 'Transferencia', importe: 1000 }],
    });
    await service.create(data, [multerFile('recibo.jpg')]);

    const [, , driveName] = driveComprobantes.subirComprobante.mock.calls[0];
    expect(driveName).toBe('SOL-MODO2.jpg');
  });

  test('driveComprobantes.subirComprobante falla -> BadRequestError, y CollectionRequest.create NUNCA se llega a llamar', async () => {
    driveComprobantes.subirComprobante.mockRejectedValue(new Error('Drive cuota excedida'));

    await expect(service.create(validData(), [multerFile('recibo.jpg')])).rejects.toThrow(
      /No se pudieron subir los comprobantes: Drive cuota excedida/,
    );
    // El punto central: la subida ocurre ANTES de tocar Mongo, así que un
    // fallo de Drive nunca deja una solicitud a medias creada.
    expect(CollectionRequest.create).not.toHaveBeenCalled();
  });
});

// ── Evento en tiempo real ────────────────────────────────────────────────────

describe('create() — aviso en tiempo real', () => {
  test('camino feliz -> emitToAll("collection-request:created", <doc creado>) exactamente una vez', async () => {
    const resultado = await service.create(validData());

    expect(emitToAll).toHaveBeenCalledTimes(1);
    expect(emitToAll).toHaveBeenCalledWith('collection-request:created', resultado);
  });
});
