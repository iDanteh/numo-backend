'use strict';

/**
 * loyalty/shared/nip.util.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad (PR2) — generación/hash del NIP de 4 dígitos (D6).
 * El NIP en texto plano nunca se persiste ni se loguea; solo se devuelve una
 * vez en la respuesta de creación/reset (eso lo verifica el servicio, PR2
 * también, no este archivo).
 */

const { generarNip, hashNip, compararNip, DUMMY_HASH } = require('./nip.util');

describe('generarNip', () => {
  test('siempre devuelve 4 dígitos (con ceros a la izquierda si aplica)', () => {
    for (let i = 0; i < 200; i++) {
      const nip = generarNip();
      expect(nip).toMatch(/^\d{4}$/);
    }
  });

  test('nunca genera un NIP de la lista débil', () => {
    const listaDebil = new Set([
      '0000', '1111', '2222', '3333', '4444', '5555', '6666', '7777', '8888', '9999',
      '1234', '4321',
    ]);
    for (let i = 0; i < 500; i++) {
      expect(listaDebil.has(generarNip())).toBe(false);
    }
  });
});

describe('hashNip / compararNip', () => {
  test('el hash nunca es igual al NIP en texto plano', async () => {
    const nip = '7392';
    const hash = await hashNip(nip);
    expect(hash).not.toBe(nip);
    expect(hash.length).toBeGreaterThan(10);
  });

  test('compararNip devuelve true para el NIP correcto', async () => {
    const nip = '7392';
    const hash = await hashNip(nip);
    await expect(compararNip(nip, hash)).resolves.toBe(true);
  });

  test('compararNip devuelve false para un NIP incorrecto', async () => {
    const hash = await hashNip('7392');
    await expect(compararNip('0001', hash)).resolves.toBe(false);
  });

  test('dos hashes del mismo NIP son distintos (salt aleatorio de bcrypt)', async () => {
    const [h1, h2] = await Promise.all([hashNip('7392'), hashNip('7392')]);
    expect(h1).not.toBe(h2);
  });
});

describe('DUMMY_HASH', () => {
  test('es un hash bcrypt válido utilizable para comparar en tiempo constante', async () => {
    expect(typeof DUMMY_HASH).toBe('string');
    await expect(compararNip('0000', DUMMY_HASH)).resolves.toBe(false);
  });
});
