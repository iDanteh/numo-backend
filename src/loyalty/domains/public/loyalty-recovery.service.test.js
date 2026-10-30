'use strict';

// loyalty-recovery.service.test.js — recuperación de NIP en 2 pasos (cliente
// final, sin sesión). Mismo threat model que loyalty-public.service.test.js:
// toda falla debe ser indistinguible desde afuera, con igualación de tiempo
// vía bcrypt.compare contra DUMMY_CODIGO_HASH, lockout por tarjeta tras 5
// fallos consecutivos (15 min), código de un solo uso, y el logger NUNCA debe
// recibir el código en texto plano.

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard: { scope: jest.fn(), findOne: jest.fn() },
  LoyaltySecurityEvent: { create: jest.fn() },
}));
jest.mock('../../shared/nip.util', () => ({
  generarNip: jest.fn(),
  hashNip: jest.fn(),
  generarCodigoRecuperacion: jest.fn(),
  hashCodigo: jest.fn(),
  compararCodigo: jest.fn(),
  DUMMY_CODIGO_HASH: '$2a$10$dummyCodigoHashModuleLevelXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
  MAX_INTENTOS_FALLIDOS: 5,
  LOCKOUT_MS: 15 * 60 * 1000,
}));
jest.mock('../../../shared/utils/logger', () => ({
  logger: { warn: jest.fn(), error: jest.fn(), info: jest.fn(), debug: jest.fn() },
}));

const { LoyaltyCard, LoyaltySecurityEvent } = require('../../../shared/models/postgres');
const {
  generarNip, generarCodigoRecuperacion, hashCodigo, compararCodigo, DUMMY_CODIGO_HASH,
} = require('../../shared/nip.util');
const { logger } = require('../../../shared/utils/logger');
const { solicitarCodigo, confirmarCodigoConsulta } = require('./loyalty-recovery.service');

const TELEFONO = '5512345678';
const CODIGO_REAL = '654321';
const CODIGO_MALO = '111111';

function makeCard(overrides = {}) {
  return {
    id: 1,
    telefono: TELEFONO,
    nipHash: 'hash-viejo',
    nombre: 'Ana García López',
    nivel: 'Plata',
    recuperacionCodigoHash: 'hash-del-codigo-real',
    recuperacionExpiraEn: new Date(Date.now() + 5 * 60 * 1000),
    recuperacionIntentosFallidos: 0,
    recuperacionBloqueadaHasta: null,
    update: jest.fn(function (cambios) { Object.assign(this, cambios); return Promise.resolve(this); }),
    ...overrides,
  };
}

const ORIGINAL_NODE_ENV = process.env.NODE_ENV;

beforeEach(() => {
  jest.clearAllMocks();
  LoyaltyCard.scope.mockReturnValue(LoyaltyCard);
  process.env.NODE_ENV = 'test';
});

afterAll(() => {
  process.env.NODE_ENV = ORIGINAL_NODE_ENV;
});

describe('solicitarCodigo — anti-enumeración: misma respuesta exista o no la tarjeta', () => {
  test('formato de teléfono inválido → mensaje genérico, sin tocar la base de datos', async () => {
    const resultado = await solicitarCodigo('abc');

    expect(resultado).toEqual({ mensaje: expect.any(String) });
    expect(resultado.codigoDev).toBeUndefined();
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
    expect(compararCodigo).toHaveBeenCalledWith(expect.any(String), DUMMY_CODIGO_HASH);
  });

  test('teléfono desconocido → mismo mensaje genérico, corre compararCodigo contra DUMMY_CODIGO_HASH (timing parity)', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    const resultado = await solicitarCodigo(TELEFONO);

    expect(resultado).toEqual({ mensaje: expect.any(String) });
    expect(resultado.codigoDev).toBeUndefined();
    expect(compararCodigo).toHaveBeenCalledWith(expect.any(String), DUMMY_CODIGO_HASH);
    expect(logger.info).not.toHaveBeenCalled();
  });

  test('teléfono conocido → mismo mensaje genérico (idéntico al de teléfono desconocido)', async () => {
    LoyaltyCard.findOne.mockResolvedValueOnce(null);
    const { mensaje: mensajeDesconocido } = await solicitarCodigo('5500000000');

    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValueOnce(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');
    const { mensaje: mensajeConocido } = await solicitarCodigo(TELEFONO);

    expect(mensajeConocido).toBe(mensajeDesconocido);
  });
});

describe('solicitarCodigo — teléfono existente genera y persiste un código nuevo', () => {
  test('guarda recuperacionCodigoHash/Expira y resetea intentos/bloqueo previos', async () => {
    const card = makeCard({ recuperacionIntentosFallidos: 3, recuperacionBloqueadaHasta: new Date() });
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');

    const antes = Date.now();
    await solicitarCodigo(TELEFONO);
    const despues = Date.now();

    expect(hashCodigo).toHaveBeenCalledWith(CODIGO_REAL);
    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({
      recuperacionCodigoHash: 'hash-nuevo',
      recuperacionIntentosFallidos: 0,
      recuperacionBloqueadaHasta: null,
    }));
    const [[cambios]] = card.update.mock.calls;
    expect(cambios.recuperacionExpiraEn).toBeInstanceOf(Date);
    const deltaMs = cambios.recuperacionExpiraEn.getTime() - antes;
    expect(deltaMs).toBeGreaterThanOrEqual(10 * 60 * 1000 - 1000);
    expect(deltaMs).toBeLessThanOrEqual((despues - antes) + 10 * 60 * 1000 + 1000);
  });

  test('fuera de producción, incluye codigoDev con el código en texto plano', async () => {
    process.env.NODE_ENV = 'development';
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');

    const resultado = await solicitarCodigo(TELEFONO);

    expect(resultado.codigoDev).toBe(CODIGO_REAL);
  });

  test('en producción, NUNCA incluye codigoDev aunque la tarjeta exista', async () => {
    process.env.NODE_ENV = 'production';
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');

    const resultado = await solicitarCodigo(TELEFONO);

    expect(resultado.codigoDev).toBeUndefined();
  });

  test('loguea que se generó un código, enmascarando el teléfono y sin exponer el código', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');

    await solicitarCodigo(TELEFONO);

    expect(logger.info).toHaveBeenCalled();
    for (const call of logger.info.mock.calls) {
      for (const arg of call) {
        const serialized = typeof arg === 'string' ? arg : JSON.stringify(arg);
        expect(serialized).not.toContain(CODIGO_REAL);
      }
    }
    const loggedFullPhone = logger.info.mock.calls.some((call) =>
      call.some((arg) => typeof arg === 'string' && arg.includes(TELEFONO)));
    expect(loggedFullPhone).toBe(false);
  });
});

describe('confirmarCodigoConsulta — formato inválido rechaza sin tocar la base de datos', () => {
  test.each([
    ['sin código', TELEFONO, undefined],
    ['código de 3 dígitos', TELEFONO, '123'],
    ['código con letras', TELEFONO, 'abcdef'],
    ['teléfono inválido', '551234abcd', CODIGO_REAL],
  ])('%s → 422 genérico, sin findOne', async (_desc, telefono, codigo) => {
    await expect(confirmarCodigoConsulta(telefono, codigo)).rejects.toMatchObject({
      statusCode: 422,
      code: 'LOYALTY_RECOVERY_FAILED',
    });
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
  });
});

describe('confirmarCodigoConsulta — caminos de falla, todos indistinguibles entre sí', () => {
  test('teléfono desconocido → 422 genérico, corre compararCodigo contra DUMMY_CODIGO_HASH', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_REAL)).rejects.toMatchObject({
      statusCode: 422, code: 'LOYALTY_RECOVERY_FAILED',
    });
    expect(compararCodigo).toHaveBeenCalledWith(CODIGO_REAL, DUMMY_CODIGO_HASH);
  });

  test('tarjeta bloqueada por intentos → 422 genérico aunque el código sea correcto', async () => {
    const card = makeCard({ activa: true, recuperacionBloqueadaHasta: new Date(Date.now() + 5 * 60 * 1000) });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_REAL)).rejects.toMatchObject({
      statusCode: 422, code: 'LOYALTY_RECOVERY_FAILED',
    });
    expect(card.update).not.toHaveBeenCalled();
  });

  test('sin código vigente (recuperacionCodigoHash null) → 422 genérico', async () => {
    const card = makeCard({ recuperacionCodigoHash: null, recuperacionExpiraEn: null });
    LoyaltyCard.findOne.mockResolvedValue(card);

    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_REAL)).rejects.toMatchObject({
      statusCode: 422, code: 'LOYALTY_RECOVERY_FAILED',
    });
    expect(compararCodigo).toHaveBeenCalledWith(CODIGO_REAL, DUMMY_CODIGO_HASH);
  });

  test('código expirado → 422 genérico', async () => {
    const card = makeCard({ recuperacionExpiraEn: new Date(Date.now() - 1000) });
    LoyaltyCard.findOne.mockResolvedValue(card);

    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_REAL)).rejects.toMatchObject({
      statusCode: 422, code: 'LOYALTY_RECOVERY_FAILED',
    });
  });

  test('todos los caminos de falla devuelven el mismo code/mensaje', async () => {
    LoyaltyCard.findOne.mockResolvedValueOnce(null);
    let errDesconocido;
    try { await confirmarCodigoConsulta('5500000000', CODIGO_REAL); } catch (e) { errDesconocido = e; }

    const cardExpirada = makeCard({ recuperacionExpiraEn: new Date(Date.now() - 1000) });
    LoyaltyCard.findOne.mockResolvedValueOnce(cardExpirada);
    let errExpirado;
    try { await confirmarCodigoConsulta(TELEFONO, CODIGO_REAL); } catch (e) { errExpirado = e; }

    expect(errDesconocido.statusCode).toBe(errExpirado.statusCode);
    expect(errDesconocido.code).toBe(errExpirado.code);
    expect(errDesconocido.message).toBe(errExpirado.message);
  });
});

describe('confirmarCodigoConsulta — código incorrecto incrementa intentos y bloquea al 5to', () => {
  test('incrementa recuperacionIntentosFallidos en 1', async () => {
    const card = makeCard({ activa: true, recuperacionIntentosFallidos: 2 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(false);

    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_MALO)).rejects.toMatchObject({ statusCode: 422 });

    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({ recuperacionIntentosFallidos: 3 }));
  });

  test('el 5to fallo consecutivo activa recuperacionBloqueadaHasta ~15 min adelante', async () => {
    const card = makeCard({ activa: true, recuperacionIntentosFallidos: 4 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(false);

    const antes = Date.now();
    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_MALO)).rejects.toMatchObject({ statusCode: 422 });

    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({ recuperacionIntentosFallidos: 5 }));
    const [[cambios]] = card.update.mock.calls;
    expect(cambios.recuperacionBloqueadaHasta).toBeInstanceOf(Date);
    expect(cambios.recuperacionBloqueadaHasta.getTime() - antes).toBeGreaterThanOrEqual(15 * 60 * 1000 - 1000);
  });

  test('el 4to fallo consecutivo NO bloquea todavía', async () => {
    const card = makeCard({ activa: true, recuperacionIntentosFallidos: 3 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(false);

    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_MALO)).rejects.toMatchObject({ statusCode: 422 });

    const [[cambios]] = card.update.mock.calls;
    expect(cambios.recuperacionBloqueadaHasta).toBeUndefined();
  });
});

describe('confirmarCodigoConsulta — busca la tarjeta con el scope que incluye recuperacionCodigoHash', () => {
  test('usa el scope withRecuperacionCodigoHash', async () => {
    const card = makeCard({ activa: true, saldoPuntos: 100 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    await confirmarCodigoConsulta(TELEFONO, CODIGO_REAL);

    expect(LoyaltyCard.scope).toHaveBeenCalledWith('withRecuperacionCodigoHash');
  });
});

describe('logging — nunca el código en texto plano en logger.warn, sí el teléfono enmascarado', () => {
  test('ningún argumento de logger.warn contiene el código intentado', async () => {
    const card = makeCard({ activa: true });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(false);

    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_MALO)).rejects.toMatchObject({ statusCode: 422 });

    expect(logger.warn).toHaveBeenCalled();
    for (const call of logger.warn.mock.calls) {
      for (const arg of call) {
        const serialized = typeof arg === 'string' ? arg : JSON.stringify(arg);
        expect(serialized).not.toContain(CODIGO_MALO);
      }
    }
  });

  test('el teléfono logueado está enmascarado (no aparece completo)', async () => {
    const card = makeCard({ activa: true });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(false);

    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_MALO)).rejects.toMatchObject({ statusCode: 422 });

    const loggedFullPhone = logger.warn.mock.calls.some((call) =>
      call.some((arg) => typeof arg === 'string' && arg.includes(TELEFONO)));
    expect(loggedFullPhone).toBe(false);
  });
});

describe('confirmarCodigoConsulta — reemplazo del NIP estático en la ruta pública (2026-10-26)', () => {
  test('código correcto + tarjeta activa → devuelve nombre/nivel/saldoPuntos, nunca un NIP', async () => {
    const card = makeCard({ activa: true, saldoPuntos: 1234.5 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    const resultado = await confirmarCodigoConsulta(TELEFONO, CODIGO_REAL);

    expect(resultado).toEqual({ nombre: 'Ana García López', nivel: 'Plata', saldoPuntos: 1234.5 });
    expect(generarNip).not.toHaveBeenCalled();
    expect(card.update).toHaveBeenCalledWith({
      recuperacionCodigoHash: null,
      recuperacionExpiraEn: null,
      recuperacionIntentosFallidos: 0,
      recuperacionBloqueadaHasta: null,
    });
  });

  test('código incorrecto comparte el mismo camino de error único (_validarCodigo)', async () => {
    const card = makeCard({ activa: true });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(false);

    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_MALO)).rejects.toMatchObject({
      statusCode: 422, code: 'LOYALTY_RECOVERY_FAILED',
    });
    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({ recuperacionIntentosFallidos: expect.any(Number) }));
  });

  test('código correcto pero tarjeta inactiva → 422 genérico, no consume el código ni devuelve saldo', async () => {
    const card = makeCard({ activa: false });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_REAL)).rejects.toMatchObject({
      statusCode: 422, code: 'LOYALTY_RECOVERY_FAILED',
    });
    expect(card.update).not.toHaveBeenCalled();
  });
});

describe('Auditoría — eventos de seguridad persistidos en LoyaltySecurityEvent (2026-10-26)', () => {
  test('solicitarCodigo: fallo (teléfono desconocido) registra evento de auditoría', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await solicitarCodigo(TELEFONO, { ip: '10.0.0.1' });

    expect(LoyaltySecurityEvent.create).toHaveBeenCalledWith({
      tipo: 'solicitud_codigo',
      resultado: 'fallo',
      motivo: 'telefono desconocido',
      telefono: TELEFONO,
      ip: '10.0.0.1',
    });
  });

  test('solicitarCodigo: éxito registra evento sin motivo', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');

    await solicitarCodigo(TELEFONO, { ip: '10.0.0.1' });

    expect(LoyaltySecurityEvent.create).toHaveBeenCalledWith({
      tipo: 'solicitud_codigo',
      resultado: 'exito',
      motivo: null,
      telefono: TELEFONO,
      ip: '10.0.0.1',
    });
  });

  test('confirmarCodigoConsulta: fallo (código incorrecto) registra evento de auditoría', async () => {
    const card = makeCard({ activa: true });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(false);

    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_MALO, { ip: '10.0.0.2' })).rejects.toMatchObject({ statusCode: 422 });

    expect(LoyaltySecurityEvent.create).toHaveBeenCalledWith({
      tipo: 'confirmacion_codigo',
      resultado: 'fallo',
      motivo: 'codigo incorrecto',
      telefono: TELEFONO,
      ip: '10.0.0.2',
    });
  });

  test('confirmarCodigoConsulta: éxito registra evento sin motivo', async () => {
    const card = makeCard({ activa: true, saldoPuntos: 100 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    await confirmarCodigoConsulta(TELEFONO, CODIGO_REAL, { ip: '10.0.0.3' });

    expect(LoyaltySecurityEvent.create).toHaveBeenCalledWith({
      tipo: 'confirmacion_codigo',
      resultado: 'exito',
      motivo: null,
      telefono: TELEFONO,
      ip: '10.0.0.3',
    });
  });

  test('si LoyaltySecurityEvent.create rechaza, la función de negocio igual completa normalmente', async () => {
    LoyaltySecurityEvent.create.mockRejectedValue(new Error('DB caída'));
    const card = makeCard({ activa: true, saldoPuntos: 100 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    await expect(confirmarCodigoConsulta(TELEFONO, CODIGO_REAL)).resolves.toEqual({
      nombre: 'Ana García López', nivel: 'Plata', saldoPuntos: 100,
    });
  });
});
