'use strict';

// loyalty-public.service.test.js — PR4/8 sistema-lealtad. Cubre el "threat
// matrix" de design.md: toda falla (teléfono desconocido, NIP incorrecto,
// tarjeta inactiva, bloqueada, input malformado) debe ser INDISTINGUIBLE desde
// afuera (mismo status/código/mensaje), con igualación de tiempo vía
// bcrypt.compare contra DUMMY_HASH cuando el teléfono no existe, lockout por
// tarjeta tras 5 fallos consecutivos (15 min), y el logger NUNCA debe recibir
// el NIP en texto plano.

jest.mock('../../../shared/models/postgres', () => ({
  LoyaltyCard: { scope: jest.fn(), findOne: jest.fn() },
}));
jest.mock('../../shared/nip.util', () => ({
  compararNip: jest.fn(),
  DUMMY_HASH: '$2a$10$dummyHashModuleLevelXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
  MAX_INTENTOS_FALLIDOS: 5,
  LOCKOUT_MS: 15 * 60 * 1000,
}));
jest.mock('../../../shared/utils/logger', () => ({
  logger: { warn: jest.fn(), error: jest.fn(), info: jest.fn(), debug: jest.fn() },
}));

const { LoyaltyCard } = require('../../../shared/models/postgres');
const { compararNip, DUMMY_HASH } = require('../../shared/nip.util');
const { logger } = require('../../../shared/utils/logger');
const { consultaPublica } = require('./loyalty-public.service');

const TELEFONO = '5512345678';
const NIP_REAL = '9876';
const NIP_MALO = '1111'; // NIP débil, pero sirve igual como "candidato incorrecto" en este test

function makeCard(overrides = {}) {
  return {
    id: 1,
    telefono: TELEFONO,
    nipHash: 'hash-real-de-la-tarjeta',
    nombre: 'Ana García López',
    nivel: 'Plata',
    saldoPuntos: '150.00',
    activa: true,
    nipIntentosFallidos: 0,
    nipBloqueadoHasta: null,
    update: jest.fn(function (cambios) { Object.assign(this, cambios); return Promise.resolve(this); }),
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  LoyaltyCard.scope.mockReturnValue(LoyaltyCard);
});

describe('formato inválido — rechaza SIN tocar la base de datos', () => {
  test.each([
    ['sin nip', TELEFONO, undefined],
    ['sin telefono', undefined, NIP_REAL],
    ['telefono con letras', '551234abcd', NIP_REAL],
    ['telefono de 9 dígitos', '551234567', NIP_REAL],
    ['nip de 3 dígitos', TELEFONO, '123'],
    ['nip con letras', TELEFONO, 'ab12'],
  ])('%s → 422 genérico, sin findOne', async (_desc, telefono, nip) => {
    await expect(consultaPublica(telefono, nip)).rejects.toMatchObject({
      statusCode: 422,
      code: 'LOYALTY_LOOKUP_FAILED',
    });
    expect(LoyaltyCard.findOne).not.toHaveBeenCalled();
  });
});

describe('teléfono desconocido — indistinguible de NIP incorrecto', () => {
  test('422 genérico y corre bcrypt.compare contra DUMMY_HASH (igualación de tiempo)', async () => {
    LoyaltyCard.findOne.mockResolvedValue(null);
    compararNip.mockResolvedValue(false);

    await expect(consultaPublica(TELEFONO, NIP_REAL)).rejects.toMatchObject({
      statusCode: 422,
      code: 'LOYALTY_LOOKUP_FAILED',
    });

    expect(compararNip).toHaveBeenCalledWith(NIP_REAL, DUMMY_HASH);
  });
});

describe('NIP incorrecto para un teléfono real — mismo cuerpo que teléfono desconocido', () => {
  test('422 genérico, mismo code/mensaje', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararNip.mockResolvedValue(false);

    let errDesconocido;
    let errNipMalo;
    LoyaltyCard.findOne.mockResolvedValueOnce(null);
    try { await consultaPublica('5500000000', NIP_REAL); } catch (e) { errDesconocido = e; }

    LoyaltyCard.findOne.mockResolvedValueOnce(card);
    try { await consultaPublica(TELEFONO, NIP_MALO); } catch (e) { errNipMalo = e; }

    expect(errDesconocido.statusCode).toBe(errNipMalo.statusCode);
    expect(errDesconocido.code).toBe(errNipMalo.code);
    expect(errDesconocido.message).toBe(errNipMalo.message);
  });

  test('incrementa nipIntentosFallidos en 1', async () => {
    const card = makeCard({ nipIntentosFallidos: 2 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararNip.mockResolvedValue(false);

    await expect(consultaPublica(TELEFONO, NIP_MALO)).rejects.toMatchObject({ statusCode: 422 });

    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({ nipIntentosFallidos: 3 }));
  });
});

describe('tarjeta inactiva — mismo error genérico que un teléfono desconocido', () => {
  test('422 genérico aunque el NIP sea correcto', async () => {
    const card = makeCard({ activa: false });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararNip.mockResolvedValue(true); // NIP correcto, pero inactiva

    await expect(consultaPublica(TELEFONO, NIP_REAL)).rejects.toMatchObject({
      statusCode: 422,
      code: 'LOYALTY_LOOKUP_FAILED',
    });
  });
});

describe('lockout por tarjeta (D7): 5 fallos consecutivos → 15 min bloqueada', () => {
  test('el 5to fallo consecutivo activa nipBloqueadoHasta ~15 min adelante', async () => {
    const card = makeCard({ nipIntentosFallidos: 4 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararNip.mockResolvedValue(false);

    const antes = Date.now();
    await expect(consultaPublica(TELEFONO, NIP_MALO)).rejects.toMatchObject({ statusCode: 422 });
    const despues = Date.now();

    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({ nipIntentosFallidos: 5 }));
    const [[cambios]] = card.update.mock.calls;
    expect(cambios.nipBloqueadoHasta).toBeInstanceOf(Date);
    const deltaMs = cambios.nipBloqueadoHasta.getTime() - antes;
    expect(deltaMs).toBeGreaterThanOrEqual(15 * 60 * 1000 - 1000);
    expect(deltaMs).toBeLessThanOrEqual((despues - antes) + 15 * 60 * 1000 + 1000);
  });

  test('el 4to fallo consecutivo NO bloquea todavía', async () => {
    const card = makeCard({ nipIntentosFallidos: 3 });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararNip.mockResolvedValue(false);

    await expect(consultaPublica(TELEFONO, NIP_MALO)).rejects.toMatchObject({ statusCode: 422 });

    expect(card.update).toHaveBeenCalledWith(expect.objectContaining({ nipIntentosFallidos: 4 }));
    const [[cambios]] = card.update.mock.calls;
    expect(cambios.nipBloqueadoHasta).toBeUndefined();
  });

  test('con la tarjeta YA bloqueada, el NIP CORRECTO sigue fallando', async () => {
    const card = makeCard({ nipBloqueadoHasta: new Date(Date.now() + 5 * 60 * 1000) });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararNip.mockResolvedValue(true); // NIP correcto

    await expect(consultaPublica(TELEFONO, NIP_REAL)).rejects.toMatchObject({
      statusCode: 422,
      code: 'LOYALTY_LOOKUP_FAILED',
    });
  });

  test('con el bloqueo YA expirado, un NIP correcto SÍ pasa', async () => {
    const card = makeCard({ nipIntentosFallidos: 5, nipBloqueadoHasta: new Date(Date.now() - 1000) });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararNip.mockResolvedValue(true);

    const resultado = await consultaPublica(TELEFONO, NIP_REAL);
    expect(resultado.nombre).toBe('Ana García López');
  });
});

describe('éxito — resetea el contador y responde el payload mínimo', () => {
  test('resetea nipIntentosFallidos/nipBloqueadoHasta tras un login correcto', async () => {
    const card = makeCard({ nipIntentosFallidos: 3, nipBloqueadoHasta: null });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararNip.mockResolvedValue(true);

    await consultaPublica(TELEFONO, NIP_REAL);

    expect(card.update).toHaveBeenCalledWith({ nipIntentosFallidos: 0, nipBloqueadoHasta: null });
  });

  test('no vuelve a escribir si ya estaba en 0 intentos (evita un UPDATE innecesario)', async () => {
    const card = makeCard({ nipIntentosFallidos: 0, nipBloqueadoHasta: null });
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararNip.mockResolvedValue(true);

    await consultaPublica(TELEFONO, NIP_REAL);

    expect(card.update).not.toHaveBeenCalled();
  });

  test('responde solo nombre completo, nivel y saldoPuntos — nunca teléfono/nipHash/createdBy', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararNip.mockResolvedValue(true);

    const resultado = await consultaPublica(TELEFONO, NIP_REAL);

    expect(resultado).toEqual({ nombre: 'Ana García López', nivel: 'Plata', saldoPuntos: '150.00' });
    expect(resultado.telefono).toBeUndefined();
    expect(resultado.nipHash).toBeUndefined();
    expect(resultado.createdById).toBeUndefined();
  });
});

describe('logging — nunca el NIP en texto plano, sí el teléfono enmascarado', () => {
  test('ningún argumento de logger.warn contiene el NIP intentado', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararNip.mockResolvedValue(false);

    await expect(consultaPublica(TELEFONO, NIP_MALO)).rejects.toMatchObject({ statusCode: 422 });

    expect(logger.warn).toHaveBeenCalled();
    for (const call of logger.warn.mock.calls) {
      for (const arg of call) {
        const serialized = typeof arg === 'string' ? arg : JSON.stringify(arg);
        expect(serialized).not.toContain(NIP_MALO);
      }
    }
  });

  test('el teléfono logueado está enmascarado (no aparece completo)', async () => {
    const card = makeCard();
    LoyaltyCard.findOne.mockResolvedValue(card);
    compararNip.mockResolvedValue(false);

    await expect(consultaPublica(TELEFONO, NIP_MALO)).rejects.toMatchObject({ statusCode: 422 });

    const loggedFullPhone = logger.warn.mock.calls.some((call) =>
      call.some((arg) => typeof arg === 'string' && arg.includes(TELEFONO)));
    expect(loggedFullPhone).toBe(false);
  });
});
