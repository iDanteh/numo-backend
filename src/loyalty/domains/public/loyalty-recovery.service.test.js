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
  // Sesión de consulta (2026-11-09) — generarSesionToken/hashSesionToken son deterministas acá
  // (no bcrypt): el test controla directamente qué hash "guardado" matchea con compararSesionToken.
  generarSesionToken: jest.fn(),
  hashSesionToken: jest.fn((token) => `hash-de-${token}`),
  compararSesionToken: jest.fn(),
  DUMMY_SESION_TOKEN_HASH: 'dummy-sesion-token-hash',
}));
jest.mock('../../../shared/utils/logger', () => ({
  logger: { warn: jest.fn(), error: jest.fn(), info: jest.fn(), debug: jest.fn() },
}));
// Historial de movimientos (2026-11-03) — se mockea completo, cubierto aparte en
// loyalty-ledger.service.test.js; acá solo se verifica que se agrega al payload de éxito.
jest.mock('../ledger/loyalty-ledger.service', () => ({
  listarMovimientosPublicos: jest.fn(),
}));
// Historial de cambios de catálogo (2026-11-09) — se mockea completo, cubierto aparte en
// loyalty-card-audit.service.test.js; acá solo se verifica QUÉ se le pasa (antes/despues/accion/
// origen), no su comportamiento interno de comparación.
jest.mock('../cards/loyalty-card-audit.service', () => ({
  registrarCambioTarjeta: jest.fn(),
}));
// Límite mensual de consultas con código (2026-11-09) — se mockea completo, cubierto aparte en
// loyalty-config.service.test.js; acá solo se verifica que solicitarCodigo lo consulte y respete.
jest.mock('../config/loyalty-config.service', () => ({
  obtenerConsultaCodigoMaxMes: jest.fn(),
}));

const { LoyaltyCard, LoyaltySecurityEvent } = require('../../../shared/models/postgres');
const {
  generarNip, generarCodigoRecuperacion, hashCodigo, compararCodigo, DUMMY_CODIGO_HASH,
  generarSesionToken, compararSesionToken, DUMMY_SESION_TOKEN_HASH,
} = require('../../shared/nip.util');
const { logger } = require('../../../shared/utils/logger');
const ledgerService = require('../ledger/loyalty-ledger.service');
const { registrarCambioTarjeta } = require('../cards/loyalty-card-audit.service');
const configService = require('../config/loyalty-config.service');
const {
  solicitarCodigo, confirmarCodigoConsulta, verConsultaSesion, guardarEmailConsulta,
} = require('./loyalty-recovery.service');

const TELEFONO = '5512345678';
const CODIGO_REAL = '654321';
const CODIGO_MALO = '111111';
const SESION_TOKEN = 'token-de-sesion-real';

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
    consultaSesionTokenHash: null,
    consultaSesionUsosRestantes: null,
    consultaSesionExpiraEn: null,
    totalConsultas: 0,
    emailContacto: null,
    consultaCodigoMesPeriodo: null,
    consultaCodigoMesContador: 0,
    update: jest.fn(function (cambios) { Object.assign(this, cambios); return Promise.resolve(this); }),
    ...overrides,
  };
}

/** Tarjeta con una sesión de consulta vigente (post-confirmarCodigoConsulta) — usada por los
 *  describe de verConsultaSesion/guardarEmailConsulta. `compararSesionToken` se mockea aparte en
 *  cada test (no es determinista por defecto, a diferencia de hashSesionToken). */
function makeCardConSesion(overrides = {}) {
  return makeCard({
    activa: true,
    consultaSesionTokenHash: 'hash-del-token-real',
    consultaSesionUsosRestantes: 3,
    consultaSesionExpiraEn: new Date(Date.now() + 10 * 60 * 1000),
    ...overrides,
  });
}

const ORIGINAL_NODE_ENV = process.env.NODE_ENV;

beforeEach(() => {
  jest.clearAllMocks();
  LoyaltyCard.scope.mockReturnValue(LoyaltyCard);
  process.env.NODE_ENV = 'test';
  ledgerService.listarMovimientosPublicos.mockResolvedValue([]);
  generarSesionToken.mockReturnValue(SESION_TOKEN);
  // Default: sin límite configurado (preserva el comportamiento de todos los tests que no
  // ejercitan el límite mensual explícitamente) — ver describe dedicado más abajo.
  configService.obtenerConsultaCodigoMaxMes.mockResolvedValue(null);
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
  test('código correcto + tarjeta activa → devuelve nombre/nivel/saldoPuntos + token/usosRestantes de sesión, nunca un NIP', async () => {
    const card = makeCard({ activa: true, saldoPuntos: 1234.5 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    const resultado = await confirmarCodigoConsulta(TELEFONO, CODIGO_REAL);

    expect(resultado).toEqual({
      nombre: 'Ana García López', nivel: 'Plata', saldoPuntos: 1234.5, movimientos: [],
      token: SESION_TOKEN, usosRestantes: 3, emailContacto: null,
    });
    expect(generarNip).not.toHaveBeenCalled();
    expect(card.update).toHaveBeenCalledWith({
      recuperacionCodigoHash: null,
      recuperacionExpiraEn: null,
      recuperacionIntentosFallidos: 0,
      recuperacionBloqueadaHasta: null,
      consultaSesionTokenHash: `hash-de-${SESION_TOKEN}`,
      consultaSesionUsosRestantes: 3,
      consultaSesionExpiraEn: expect.any(Date),
      ultimaConsultaAt: expect.any(Date),
      totalConsultas: 1,
      consultaCodigoMesPeriodo: expect.any(String),
      consultaCodigoMesContador: 1,
    });
  });

  test('token de sesión emitido tiene ventana de ~15 minutos', async () => {
    const card = makeCard({ activa: true, saldoPuntos: 100 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    const antes = Date.now();
    await confirmarCodigoConsulta(TELEFONO, CODIGO_REAL);

    const [[cambios]] = card.update.mock.calls;
    const deltaMs = cambios.consultaSesionExpiraEn.getTime() - antes;
    expect(deltaMs).toBeGreaterThanOrEqual(15 * 60 * 1000 - 1000);
    expect(deltaMs).toBeLessThanOrEqual(15 * 60 * 1000 + 1000);
  });

  test('totalConsultas acumula sobre el valor previo de la tarjeta (nunca reinicia a 1)', async () => {
    const card = makeCard({ activa: true, totalConsultas: 7 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    await confirmarCodigoConsulta(TELEFONO, CODIGO_REAL);

    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({ totalConsultas: 8 }));
  });

  // Correo ya registrado en una sesión anterior (2026-11-09) — el frontend lo usa para mostrar el
  // campo ya bloqueado desde el primer vistazo, sin depender de guardarEmail() en esta sesión.
  test('tarjeta con emailContacto ya guardado → lo incluye en la respuesta', async () => {
    const card = makeCard({ activa: true, emailContacto: 'previo@correo.com' });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    const resultado = await confirmarCodigoConsulta(TELEFONO, CODIGO_REAL);

    expect(resultado.emailContacto).toBe('previo@correo.com');
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

// Mismo cálculo que _periodoActualMexico() (privada, no exportada) — duplicado acá a propósito
// para comparar en negro contra el comportamiento real, sin mockear Date/Intl.
const periodoActualMexico = () => new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/Mexico_City', month: '2-digit', year: 'numeric',
}).format(new Date());

describe('Límite mensual de consultas CON CÓDIGO (2026-11-09, blindaje adicional)', () => {
  test('sin límite configurado (null) → genera código normalmente aunque el contador sea alto', async () => {
    configService.obtenerConsultaCodigoMaxMes.mockResolvedValue(null);
    const card = makeCard({ consultaCodigoMesPeriodo: periodoActualMexico(), consultaCodigoMesContador: 999 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');

    const resultado = await solicitarCodigo(TELEFONO);

    expect(hashCodigo).toHaveBeenCalledWith(CODIGO_REAL);
    expect(resultado.codigoDev).toBe(CODIGO_REAL);
  });

  test('límite alcanzado en el período vigente → NO genera código real, mismo mensaje genérico, timing igualado', async () => {
    configService.obtenerConsultaCodigoMaxMes.mockResolvedValue(2);
    const card = makeCard({ consultaCodigoMesPeriodo: periodoActualMexico(), consultaCodigoMesContador: 2 });
    LoyaltyCard.findOne.mockResolvedValue(card);

    const resultado = await solicitarCodigo(TELEFONO);

    expect(resultado).toEqual({ mensaje: expect.any(String) });
    expect(resultado.codigoDev).toBeUndefined();
    expect(hashCodigo).not.toHaveBeenCalled();
    expect(card.update).not.toHaveBeenCalled();
    // Igualación de tiempo: corre el mismo dummy compare que los otros 2 caminos de rechazo.
    expect(compararCodigo).toHaveBeenCalledWith(expect.any(String), DUMMY_CODIGO_HASH);
  });

  test('contador por encima del límite (no solo igual) también bloquea', async () => {
    configService.obtenerConsultaCodigoMaxMes.mockResolvedValue(1);
    const card = makeCard({ consultaCodigoMesPeriodo: periodoActualMexico(), consultaCodigoMesContador: 5 });
    LoyaltyCard.findOne.mockResolvedValue(card);

    await solicitarCodigo(TELEFONO);

    expect(hashCodigo).not.toHaveBeenCalled();
  });

  test('período guardado distinto al actual (mes nuevo) → reset perezoso, genera código aunque el contador viejo ya estuviera en el límite', async () => {
    configService.obtenerConsultaCodigoMaxMes.mockResolvedValue(1);
    const card = makeCard({ consultaCodigoMesPeriodo: '2020-01', consultaCodigoMesContador: 99 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');

    const resultado = await solicitarCodigo(TELEFONO);

    expect(hashCodigo).toHaveBeenCalledWith(CODIGO_REAL);
    expect(resultado.codigoDev).toBe(CODIGO_REAL);
  });

  test('tarjeta que nunca usó el mecanismo (consultaCodigoMesPeriodo null) → no bloquea bajo el límite', async () => {
    configService.obtenerConsultaCodigoMaxMes.mockResolvedValue(3);
    const card = makeCard({ consultaCodigoMesPeriodo: null, consultaCodigoMesContador: 0 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    generarCodigoRecuperacion.mockReturnValue(CODIGO_REAL);
    hashCodigo.mockResolvedValue('hash-nuevo');

    await solicitarCodigo(TELEFONO);

    expect(hashCodigo).toHaveBeenCalledWith(CODIGO_REAL);
  });

  test('confirmarCodigoConsulta exitoso incrementa el contador cuando el período coincide', async () => {
    const card = makeCard({ activa: true, consultaCodigoMesPeriodo: periodoActualMexico(), consultaCodigoMesContador: 1 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    await confirmarCodigoConsulta(TELEFONO, CODIGO_REAL);

    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({
      consultaCodigoMesPeriodo: periodoActualMexico(),
      consultaCodigoMesContador: 2,
    }));
  });

  test('confirmarCodigoConsulta exitoso reinicia el contador a 1 cuando el período guardado es distinto (o nunca se usó)', async () => {
    const card = makeCard({ activa: true, consultaCodigoMesPeriodo: '2020-01', consultaCodigoMesContador: 50 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);

    await confirmarCodigoConsulta(TELEFONO, CODIGO_REAL);

    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({
      consultaCodigoMesPeriodo: periodoActualMexico(),
      consultaCodigoMesContador: 1,
    }));
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
      nombre: 'Ana García López', nivel: 'Plata', saldoPuntos: 100, movimientos: [],
      token: SESION_TOKEN, usosRestantes: 3, emailContacto: null,
    });
  });

  // Historial de movimientos (2026-11-03, pedido explícito del usuario) — reusa
  // listarMovimientosPublicos del ledger, nunca reasonId/usuarioId/justificacion (D6-D8).
  test('incluye movimientos (historial público recortado) resueltos por cardId', async () => {
    const card = makeCard({ activa: true, saldoPuntos: 50 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararCodigo.mockResolvedValue(true);
    const movimientos = [{ fecha: '2026-11-01', tipoLabel: 'Bonificación', puntos: '100.00', saldoAnterior: '0.00', saldoActual: '100.00' }];
    ledgerService.listarMovimientosPublicos.mockResolvedValue(movimientos);

    const resultado = await confirmarCodigoConsulta(TELEFONO, CODIGO_REAL);

    expect(ledgerService.listarMovimientosPublicos).toHaveBeenCalledWith(card.id);
    expect(resultado.movimientos).toEqual(movimientos);
  });
});

describe('verConsultaSesion — "ver de nuevo" sin pedir un código nuevo (2026-11-09)', () => {
  test('formato inválido (sin token) → 422 genérico, sin findOne', async () => {
    await expect(verConsultaSesion(TELEFONO, '')).rejects.toMatchObject({
      statusCode: 422, code: 'LOYALTY_RECOVERY_FAILED',
    });
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
    expect(compararSesionToken).toHaveBeenCalledWith(expect.any(String), DUMMY_SESION_TOKEN_HASH);
  });

  test('teléfono desconocido → 422 genérico, corre compararSesionToken contra el dummy (timing parity)', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);

    await expect(verConsultaSesion(TELEFONO, SESION_TOKEN)).rejects.toMatchObject({ statusCode: 422 });
    expect(compararSesionToken).toHaveBeenCalledWith(SESION_TOKEN, DUMMY_SESION_TOKEN_HASH);
  });

  test('sin sesión vigente (consultaSesionTokenHash null) → 422 genérico', async () => {
    const card = makeCard({ activa: true });
    LoyaltyCard.findOne.mockResolvedValue(card);

    await expect(verConsultaSesion(TELEFONO, SESION_TOKEN)).rejects.toMatchObject({ statusCode: 422 });
    expect(card.update).not.toHaveBeenCalled();
  });

  test('sesión expirada → 422 genérico', async () => {
    const card = makeCardConSesion({ consultaSesionExpiraEn: new Date(Date.now() - 1000) });
    LoyaltyCard.findOne.mockResolvedValue(card);

    await expect(verConsultaSesion(TELEFONO, SESION_TOKEN)).rejects.toMatchObject({ statusCode: 422 });
  });

  test('usos agotados (consultaSesionUsosRestantes = 0) → 422 genérico', async () => {
    const card = makeCardConSesion({ consultaSesionUsosRestantes: 0 });
    LoyaltyCard.findOne.mockResolvedValue(card);

    await expect(verConsultaSesion(TELEFONO, SESION_TOKEN)).rejects.toMatchObject({ statusCode: 422 });
  });

  test('token incorrecto → 422 genérico', async () => {
    const card = makeCardConSesion();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararSesionToken.mockReturnValue(false);

    await expect(verConsultaSesion(TELEFONO, 'token-malo')).rejects.toMatchObject({ statusCode: 422 });
  });

  test('tarjeta inactiva → 422 genérico aunque el token sea correcto', async () => {
    const card = makeCardConSesion({ activa: false });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararSesionToken.mockReturnValue(true);

    await expect(verConsultaSesion(TELEFONO, SESION_TOKEN)).rejects.toMatchObject({ statusCode: 422 });
    expect(card.update).not.toHaveBeenCalled();
  });

  test('token correcto + usos restantes > 1 → decrementa usos, NO limpia la sesión', async () => {
    const card = makeCardConSesion({ consultaSesionUsosRestantes: 3, saldoPuntos: 500 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararSesionToken.mockReturnValue(true);

    const resultado = await verConsultaSesion(TELEFONO, SESION_TOKEN);

    expect(resultado).toEqual({
      nombre: 'Ana García López', nivel: 'Plata', saldoPuntos: 500, movimientos: [],
      usosRestantes: 2, emailContacto: null,
    });
    expect(card.update).toHaveBeenCalledWith({
      consultaSesionUsosRestantes: 2,
      ultimaConsultaAt: expect.any(Date),
      totalConsultas: 1,
    });
  });

  test('última vista permitida (usos restantes = 1) → limpia la sesión entera, usosRestantes: 0', async () => {
    const card = makeCardConSesion({ consultaSesionUsosRestantes: 1 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararSesionToken.mockReturnValue(true);

    const resultado = await verConsultaSesion(TELEFONO, SESION_TOKEN);

    expect(resultado.usosRestantes).toBe(0);
    expect(card.update).toHaveBeenCalledWith({
      consultaSesionTokenHash: null,
      consultaSesionUsosRestantes: null,
      consultaSesionExpiraEn: null,
      ultimaConsultaAt: expect.any(Date),
      totalConsultas: 1,
    });
  });

  test('tarjeta con emailContacto ya guardado → lo incluye en la respuesta', async () => {
    const card = makeCardConSesion({ emailContacto: 'previo@correo.com' });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararSesionToken.mockReturnValue(true);

    const resultado = await verConsultaSesion(TELEFONO, SESION_TOKEN);

    expect(resultado.emailContacto).toBe('previo@correo.com');
  });

  test('registra auditoría de éxito (tipo ver_consulta)', async () => {
    const card = makeCardConSesion();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararSesionToken.mockReturnValue(true);

    await verConsultaSesion(TELEFONO, SESION_TOKEN, { ip: '10.0.0.4' });

    expect(LoyaltySecurityEvent.create).toHaveBeenCalledWith({
      tipo: 'ver_consulta', resultado: 'exito', motivo: null, telefono: TELEFONO, ip: '10.0.0.4',
    });
  });
});

describe('guardarEmailConsulta — correo de contacto futuro, no consume un uso de sesión (2026-11-09)', () => {
  test('sin sesión vigente → 422 genérico (mismo error que verConsultaSesion)', async () => {
    const card = makeCard({ activa: true });
    LoyaltyCard.findOne.mockResolvedValue(card);

    await expect(guardarEmailConsulta(TELEFONO, SESION_TOKEN, 'cliente@correo.com')).rejects.toMatchObject({
      statusCode: 422, code: 'LOYALTY_RECOVERY_FAILED',
    });
  });

  test('email con formato inválido → error DISTINTO (LOYALTY_EMAIL_INVALIDO, no RecoveryFailedError)', async () => {
    const card = makeCardConSesion();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararSesionToken.mockReturnValue(true);

    await expect(guardarEmailConsulta(TELEFONO, SESION_TOKEN, 'no-es-un-correo')).rejects.toMatchObject({
      statusCode: 422, code: 'LOYALTY_EMAIL_INVALIDO',
    });
    expect(card.update).not.toHaveBeenCalled();
  });

  test('email válido → persiste emailContacto, NO toca usosRestantes/ultimaConsultaAt/totalConsultas, devuelve emailContacto', async () => {
    const card = makeCardConSesion({ consultaSesionUsosRestantes: 3, totalConsultas: 5 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararSesionToken.mockReturnValue(true);

    const resultado = await guardarEmailConsulta(TELEFONO, SESION_TOKEN, '  Cliente@Correo.com  '.trim());

    expect(resultado).toEqual({ mensaje: expect.any(String), emailContacto: 'Cliente@Correo.com' });
    expect(card.update).toHaveBeenCalledWith({ emailContacto: 'Cliente@Correo.com' });
  });

  // Historial de cambios (2026-11-09, solo interno/staff — ver tarjeta-detail.component.ts) —
  // reusa registrarCambioTarjeta, mismo mecanismo que teléfono/nombre/activa.
  describe('historial de cambios (registrarCambioTarjeta)', () => {
    test('primera vez (sin email previo) → antes:null, despues:el nuevo, origen cliente', async () => {
      const card = makeCardConSesion({ emailContacto: null });
      LoyaltyCard.findOne.mockResolvedValue(card);
      compararSesionToken.mockReturnValue(true);

      await guardarEmailConsulta(TELEFONO, SESION_TOKEN, 'nuevo@correo.com');

      expect(registrarCambioTarjeta).toHaveBeenCalledWith({
        cardId: card.id,
        antes: { emailContacto: null },
        despues: { emailContacto: 'nuevo@correo.com' },
        accion: 'actualizacion_email_publico',
        origen: 'cliente',
      });
    });

    // CRÍTICO (mismo criterio que loyalty-card.service.js#editarTarjeta): `antes` se captura ANTES
    // del `.update()` — si se leyera card.emailContacto DESPUÉS, ya tendría el valor nuevo de los
    // dos lados (Sequelize muta la instancia en memoria apenas resuelve el update).
    test('actualización (con email previo distinto) → antes:el viejo, despues:el nuevo', async () => {
      const card = makeCardConSesion({ emailContacto: 'viejo@correo.com' });
      LoyaltyCard.findOne.mockResolvedValue(card);
      compararSesionToken.mockReturnValue(true);

      await guardarEmailConsulta(TELEFONO, SESION_TOKEN, 'nuevo@correo.com');

      expect(registrarCambioTarjeta).toHaveBeenCalledWith(expect.objectContaining({
        antes: { emailContacto: 'viejo@correo.com' },
        despues: { emailContacto: 'nuevo@correo.com' },
      }));
    });

    test('si registrarCambioTarjeta rechaza, el error se propaga (no es fire-and-forget)', async () => {
      const card = makeCardConSesion();
      LoyaltyCard.findOne.mockResolvedValue(card);
      compararSesionToken.mockReturnValue(true);
      registrarCambioTarjeta.mockRejectedValueOnce(new Error('DB caída'));

      await expect(guardarEmailConsulta(TELEFONO, SESION_TOKEN, 'nuevo@correo.com')).rejects.toThrow('DB caída');
    });
  });

  test('registra auditoría de éxito (tipo guardar_email)', async () => {
    const card = makeCardConSesion();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararSesionToken.mockReturnValue(true);

    await guardarEmailConsulta(TELEFONO, SESION_TOKEN, 'cliente@correo.com', { ip: '10.0.0.5' });

    expect(LoyaltySecurityEvent.create).toHaveBeenCalledWith({
      tipo: 'guardar_email', resultado: 'exito', motivo: null, telefono: TELEFONO, ip: '10.0.0.5',
    });
  });

  test('tarjeta inactiva → 422 genérico, nunca persiste el email', async () => {
    const card = makeCardConSesion({ activa: false });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararSesionToken.mockReturnValue(true);

    await expect(guardarEmailConsulta(TELEFONO, SESION_TOKEN, 'cliente@correo.com')).rejects.toMatchObject({
      statusCode: 422, code: 'LOYALTY_RECOVERY_FAILED',
    });
    expect(card.update).not.toHaveBeenCalled();
  });
});
