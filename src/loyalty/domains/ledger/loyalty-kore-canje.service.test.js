'use strict';

// loyalty-kore-canje.service.test.js — código de autorización de un solo uso para que un cliente
// autorice canjear sus puntos en una venta de Kore (2026-11-13). A diferencia de
// loyalty-recovery.service.test.js (tráfico público anónimo, anti-enumeración estricto en AMBAS
// funciones), acá el threat model es asimétrico: `solicitarCodigoCanje` SÍ revela errores
// accionables (tráfico server-a-servidor autenticado con Kore, que ya resolvió al cliente vía
// /saldo antes de llegar acá), mientras que `validarCodigoCanje` SÍ mantiene el único error
// genérico + igualación de tiempo vía bcrypt.compare contra DUMMY_CODIGO_HASH (esa función sí es
// la superficie real de fuerza bruta de un código de 6 dígitos).

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard: { scope: jest.fn(), findOne: jest.fn() },
  LoyaltySecurityEvent: { create: jest.fn() },
}));
jest.mock('../../shared/nip.util', () => ({
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
jest.mock('../config/loyalty-config.service', () => ({
  obtenerCanjeCodigoVigenciaMin: jest.fn(),
}));

const { LoyaltyCard, LoyaltySecurityEvent } = require('../../../shared/models/postgres');
const {
  generarCodigoRecuperacion, hashCodigo, compararCodigo, DUMMY_CODIGO_HASH,
} = require('../../shared/nip.util');
const { logger } = require('../../../shared/utils/logger');
const configService = require('../config/loyalty-config.service');
const {
  solicitarCodigoCanje, validarCodigoCanje, CanjeCodigoInvalidoError,
} = require('./loyalty-kore-canje.service');

const TELEFONO = '5512345678';
const CODIGO_REAL = '654321';
const CODIGO_MALO = '111111';

function makeCard(overrides = {}) {
  return {
    id: 1,
    telefono: TELEFONO,
    activa: true,
    canjeCodigoHash: 'hash-del-codigo-real',
    canjeCodigoExpiraEn: new Date(Date.now() + 5 * 60 * 1000),
    canjeIntentosFallidos: 0,
    canjeBloqueadaHasta: null,
    update: jest.fn(function (cambios) { Object.assign(this, cambios); return Promise.resolve(this); }),
    ...overrides,
  };
}

const ORIGINAL_NODE_ENV = process.env.NODE_ENV;

beforeEach(() => {
  jest.clearAllMocks();
  LoyaltyCard.scope.mockReturnValue(LoyaltyCard);
  process.env.NODE_ENV = 'test';
  configService.obtenerCanjeCodigoVigenciaMin.mockResolvedValue(5);
});

afterAll(() => {
  process.env.NODE_ENV = ORIGINAL_NODE_ENV;
});

describe('solicitarCodigoCanje — a diferencia de la consulta pública, SÍ lanza errores accionables', () => {
  test('teléfono con formato inválido → BadRequestError, sin tocar la base de datos', async () => {
    await expect(solicitarCodigoCanje('abc')).rejects.toMatchObject({ statusCode: 400 });
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
  });

  test('teléfono desconocido → NotFoundError (404), no el genérico de recuperación', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await expect(solicitarCodigoCanje(TELEFONO)).rejects.toMatchObject({ statusCode: 404 });
    expect(hashCodigo).not.toHaveBeenCalled();
  });

  test('tarjeta inactiva → BadRequestError (400), no genera código', async () => {
    const card = makeCard({ activa: false });
    LoyaltyCard.findOne.mockResolvedValue(card);

    await expect(solicitarCodigoCanje(TELEFONO)).rejects.toMatchObject({ statusCode: 400 });
    expect(card.update).not.toHaveBeenCalled();
  });

  test('teléfono activo y válido → genera código, hashea y persiste en las columnas PROPIAS de canje', async () => {
    const card = makeCard({ canjeIntentosFallidos: 3, canjeBloqueadaHasta: new Date() });
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');
    configService.obtenerCanjeCodigoVigenciaMin.mockResolvedValue(5);

    const antes = Date.now();
    await solicitarCodigoCanje(TELEFONO);

    expect(hashCodigo).toHaveBeenCalledWith(CODIGO_REAL);
    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({
      canjeCodigoHash: 'hash-nuevo',
      canjeIntentosFallidos: 0,
      canjeBloqueadaHasta: null,
    }));
    const [[cambios]] = card.update.mock.calls;
    expect(cambios.canjeCodigoExpiraEn).toBeInstanceOf(Date);
    expect(cambios.canjeCodigoExpiraEn.getTime() - antes).toBeGreaterThanOrEqual(5 * 60 * 1000 - 1000);
  });

  test('usa la vigencia configurable de loyalty-config.service (no un valor hardcodeado)', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');
    configService.obtenerCanjeCodigoVigenciaMin.mockResolvedValue(10);

    const antes = Date.now();
    await solicitarCodigoCanje(TELEFONO);

    const [[cambios]] = card.update.mock.calls;
    const deltaMs = cambios.canjeCodigoExpiraEn.getTime() - antes;
    expect(deltaMs).toBeGreaterThanOrEqual(10 * 60 * 1000 - 1000);
    expect(deltaMs).toBeLessThanOrEqual(10 * 60 * 1000 + 1000);
  });

  test('fuera de producción, incluye codigoDev con el código en texto plano', async () => {
    process.env.NODE_ENV = 'development';
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');

    const resultado = await solicitarCodigoCanje(TELEFONO);

    expect(resultado.codigoDev).toBe(CODIGO_REAL);
  });

  test('en producción, NUNCA incluye codigoDev aunque la tarjeta exista', async () => {
    process.env.NODE_ENV = 'production';
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');

    const resultado = await solicitarCodigoCanje(TELEFONO);

    expect(resultado.codigoDev).toBeUndefined();
  });

  test('loguea que se generó un código, enmascarando el teléfono y sin exponer el código', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');

    await solicitarCodigoCanje(TELEFONO);

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

  describe('auditoría (LoyaltySecurityEvent, tipo solicitud_codigo_canje)', () => {
    test('fallo (teléfono desconocido) registra evento de auditoría', async () => {
      LoyaltyCard.findOne.mockResolvedValue(null);

      await expect(solicitarCodigoCanje(TELEFONO)).rejects.toMatchObject({ statusCode: 404 });

      expect(LoyaltySecurityEvent.create).toHaveBeenCalledWith(expect.objectContaining({
        tipo: 'solicitud_codigo_canje', resultado: 'fallo', motivo: 'telefono desconocido', telefono: TELEFONO,
      }));
    });

    test('fallo (tarjeta inactiva) registra evento de auditoría', async () => {
      const card = makeCard({ activa: false });
      LoyaltyCard.findOne.mockResolvedValue(card);

      await expect(solicitarCodigoCanje(TELEFONO)).rejects.toMatchObject({ statusCode: 400 });

      expect(LoyaltySecurityEvent.create).toHaveBeenCalledWith(expect.objectContaining({
        tipo: 'solicitud_codigo_canje', resultado: 'fallo', motivo: 'tarjeta inactiva', telefono: TELEFONO,
      }));
    });

    test('éxito registra evento sin motivo', async () => {
      const card = makeCard();
      LoyaltyCard.findOne.mockResolvedValue(card);
      generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
      hashCodigo.mockResolvedValue('hash-nuevo');

      await solicitarCodigoCanje(TELEFONO);

      expect(LoyaltySecurityEvent.create).toHaveBeenCalledWith(expect.objectContaining({
        tipo: 'solicitud_codigo_canje', resultado: 'exito', motivo: null, telefono: TELEFONO,
      }));
    });

    test('si LoyaltySecurityEvent.create rechaza, la función de negocio igual completa normalmente (fire-and-forget)', async () => {
      LoyaltySecurityEvent.create.mockRejectedValue(new Error('DB caída'));
      const card = makeCard();
      LoyaltyCard.findOne.mockResolvedValue(card);
      generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
      hashCodigo.mockResolvedValue('hash-nuevo');

      await expect(solicitarCodigoCanje(TELEFONO)).resolves.toEqual(expect.objectContaining({ mensaje: expect.any(String) }));
    });
  });
});

describe('validarCodigoCanje — único error genérico (CanjeCodigoInvalidoError) para cualquier causa de falla', () => {
  test('formato inválido (teléfono o código) → CanjeCodigoInvalidoError, corre compararCodigo contra DUMMY_CODIGO_HASH (timing parity)', async () => {
    await expect(validarCodigoCanje(TELEFONO, '123')).rejects.toBeInstanceOf(CanjeCodigoInvalidoError);
    expect(compararCodigo).toHaveBeenCalledWith(expect.any(String), DUMMY_CODIGO_HASH);
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
  });

  test('teléfono desconocido → CanjeCodigoInvalidoError, corre compararCodigo contra el dummy', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await expect(validarCodigoCanje(TELEFONO, CODIGO_REAL)).rejects.toBeInstanceOf(CanjeCodigoInvalidoError);
    expect(compararCodigo).toHaveBeenCalledWith(CODIGO_REAL, DUMMY_CODIGO_HASH);
  });

  test('canje bloqueado por intentos → CanjeCodigoInvalidoError aunque el código sea correcto', async () => {
    const card = makeCard({ canjeBloqueadaHasta: new Date(Date.now() + 5 * 60 * 1000) });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    await expect(validarCodigoCanje(TELEFONO, CODIGO_REAL)).rejects.toBeInstanceOf(CanjeCodigoInvalidoError);
    expect(card.update).not.toHaveBeenCalled();
  });

  test('sin código vigente (canjeCodigoHash null) → CanjeCodigoInvalidoError', async () => {
    const card = makeCard({ canjeCodigoHash: null, canjeCodigoExpiraEn: null });
    LoyaltyCard.findOne.mockResolvedValue(card);

    await expect(validarCodigoCanje(TELEFONO, CODIGO_REAL)).rejects.toBeInstanceOf(CanjeCodigoInvalidoError);
    expect(compararCodigo).toHaveBeenCalledWith(CODIGO_REAL, DUMMY_CODIGO_HASH);
  });

  test('código expirado → CanjeCodigoInvalidoError', async () => {
    const card = makeCard({ canjeCodigoExpiraEn: new Date(Date.now() - 1000) });
    LoyaltyCard.findOne.mockResolvedValue(card);

    await expect(validarCodigoCanje(TELEFONO, CODIGO_REAL)).rejects.toBeInstanceOf(CanjeCodigoInvalidoError);
  });

  test('busca la tarjeta con el scope withCanjeCodigoHash (columna excluida del defaultScope)', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    await validarCodigoCanje(TELEFONO, CODIGO_REAL);

    expect(LoyaltyCard.scope).toHaveBeenCalledWith('withCanjeCodigoHash');
  });

  test('todos los caminos de falla devuelven la misma instancia de error (indistinguibles)', async () => {
    LoyaltyCard.findOne.mockResolvedValueOnce(null);
    let errDesconocido;
    try { await validarCodigoCanje(TELEFONO, CODIGO_REAL); } catch (e) { errDesconocido = e; }

    const cardExpirada = makeCard({ canjeCodigoExpiraEn: new Date(Date.now() - 1000) });
    LoyaltyCard.findOne.mockResolvedValueOnce(cardExpirada);
    let errExpirado;
    try { await validarCodigoCanje(TELEFONO, CODIGO_REAL); } catch (e) { errExpirado = e; }

    expect(errDesconocido.statusCode).toBe(errExpirado.statusCode);
    expect(errDesconocido.code).toBe(errExpirado.code);
    expect(errDesconocido.message).toBe(errExpirado.message);
  });
});

describe('validarCodigoCanje — código incorrecto incrementa intentos y bloquea al 5to (mismo lockout que recuperación)', () => {
  test('incrementa canjeIntentosFallidos en 1', async () => {
    const card = makeCard({ canjeIntentosFallidos: 2 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(false);

    await expect(validarCodigoCanje(TELEFONO, CODIGO_MALO)).rejects.toBeInstanceOf(CanjeCodigoInvalidoError);

    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({ canjeIntentosFallidos: 3 }));
  });

  test('el 5to fallo consecutivo activa canjeBloqueadaHasta ~15 min adelante', async () => {
    const card = makeCard({ canjeIntentosFallidos: 4 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(false);

    const antes = Date.now();
    await expect(validarCodigoCanje(TELEFONO, CODIGO_MALO)).rejects.toBeInstanceOf(CanjeCodigoInvalidoError);

    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({ canjeIntentosFallidos: 5 }));
    const [[cambios]] = card.update.mock.calls;
    expect(cambios.canjeBloqueadaHasta).toBeInstanceOf(Date);
    expect(cambios.canjeBloqueadaHasta.getTime() - antes).toBeGreaterThanOrEqual(15 * 60 * 1000 - 1000);
  });

  test('el 4to fallo consecutivo NO bloquea todavía', async () => {
    const card = makeCard({ canjeIntentosFallidos: 3 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(false);

    await expect(validarCodigoCanje(TELEFONO, CODIGO_MALO)).rejects.toBeInstanceOf(CanjeCodigoInvalidoError);

    const [[cambios]] = card.update.mock.calls;
    expect(cambios.canjeBloqueadaHasta).toBeUndefined();
  });
});

describe('validarCodigoCanje — código correcto: un solo uso, se consume de inmediato', () => {
  test('devuelve {valido: true} y limpia TODAS las columnas de canje (sin dejar sesión de varios usos)', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    const resultado = await validarCodigoCanje(TELEFONO, CODIGO_REAL);

    expect(resultado).toEqual({ valido: true });
    expect(card.update).toHaveBeenCalledWith({
      canjeCodigoHash: null,
      canjeCodigoExpiraEn: null,
      canjeIntentosFallidos: 0,
      canjeBloqueadaHasta: null,
    });
  });

  test('un segundo intento con el mismo código, tras consumirse, falla (no hay "ver de nuevo")', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValueOnce(true);
    await validarCodigoCanje(TELEFONO, CODIGO_REAL);

    // Tras el update del mock, la propia instancia ya refleja canjeCodigoHash:null — simula el
    // segundo findOne devolviendo esa misma tarjeta ya consumida.
    compararCodigo.mockResolvedValueOnce(false);
    await expect(validarCodigoCanje(TELEFONO, CODIGO_REAL)).rejects.toBeInstanceOf(CanjeCodigoInvalidoError);
  });

  test('registra auditoría de éxito (tipo confirmacion_codigo_canje, sin motivo)', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    await validarCodigoCanje(TELEFONO, CODIGO_REAL);

    expect(LoyaltySecurityEvent.create).toHaveBeenCalledWith(expect.objectContaining({
      tipo: 'confirmacion_codigo_canje', resultado: 'exito', motivo: null, telefono: TELEFONO,
    }));
  });
});

describe('logging — nunca el código en texto plano, teléfono siempre enmascarado (validarCodigoCanje)', () => {
  test('ningún log contiene el código intentado', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(false);

    await expect(validarCodigoCanje(TELEFONO, CODIGO_MALO)).rejects.toBeInstanceOf(CanjeCodigoInvalidoError);

    const allCalls = [...logger.info.mock.calls, ...logger.warn.mock.calls, ...logger.error.mock.calls];
    for (const call of allCalls) {
      for (const arg of call) {
        const serialized = typeof arg === 'string' ? arg : JSON.stringify(arg);
        expect(serialized).not.toContain(CODIGO_MALO);
      }
    }
  });
});
