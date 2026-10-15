'use strict';

/**
 * loyalty/shared/puntos.util.test.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad (PR2) — aritmética en centavos enteros (D3: Sequelize
 * devuelve DECIMAL como string; la matemática en floats arrastraría centavos).
 */

const { toCents, fromCents, calcularPuntos, normalizarTelefono } = require('./puntos.util');

describe('toCents / fromCents', () => {
  test('convierte un string DECIMAL con 2 decimales a centavos enteros', () => {
    expect(toCents('1234.56')).toBe(123456);
  });

  test('round-trip: fromCents(toCents(x)) reconstruye el string original', () => {
    expect(fromCents(toCents('1234.56'))).toBe('1234.56');
    expect(fromCents(toCents('0.00'))).toBe('0.00');
    expect(fromCents(toCents('100.00'))).toBe('100.00');
  });

  test('acepta strings sin parte decimal', () => {
    expect(toCents('100')).toBe(10000);
  });

  test('rellena con ceros cuando falta un decimal', () => {
    expect(toCents('100.5')).toBe(10050);
  });

  test('maneja negativos', () => {
    expect(toCents('-60.00')).toBe(-6000);
    expect(fromCents(-6000)).toBe('-60.00');
  });

  test('acepta números (no solo strings)', () => {
    expect(toCents(43.21)).toBe(4321);
  });
});

describe('calcularPuntos', () => {
  test('1234.56 x 3.5% = 43.21 (caso de design.md)', () => {
    const montoCents = toCents('1234.56');
    const puntosCents = calcularPuntos(montoCents, 3.5);
    expect(fromCents(puntosCents)).toBe('43.21');
  });

  test('$1,000.00 a 5% (Plata) = 50.00', () => {
    const montoCents = toCents('1000.00');
    const puntosCents = calcularPuntos(montoCents, 5);
    expect(fromCents(puntosCents)).toBe('50.00');
  });

  test('redondea al centavo más cercano', () => {
    const montoCents = toCents('10.00');
    const puntosCents = calcularPuntos(montoCents, 3.333);
    expect(fromCents(puntosCents)).toBe('0.33');
  });
});

describe('normalizarTelefono', () => {
  test('10 dígitos ya limpios se devuelven igual', () => {
    expect(normalizarTelefono('5512345678')).toBe('5512345678');
  });

  test('quita espacios y guiones', () => {
    expect(normalizarTelefono('55 1234 5678')).toBe('5512345678');
    expect(normalizarTelefono('55-1234-5678')).toBe('5512345678');
  });

  test('quita el prefijo +52', () => {
    expect(normalizarTelefono('+525512345678')).toBe('5512345678');
  });

  test('quita el prefijo 521 (celular con 13 dígitos)', () => {
    expect(normalizarTelefono('5215512345678')).toBe('5512345678');
  });

  test('rechaza un número con menos de 10 dígitos', () => {
    expect(() => normalizarTelefono('12345')).toThrow();
  });

  test('rechaza un número con letras', () => {
    expect(() => normalizarTelefono('55ABCD5678')).toThrow();
  });

  test('rechaza vacío o nulo', () => {
    expect(() => normalizarTelefono('')).toThrow();
    expect(() => normalizarTelefono(null)).toThrow();
  });
});
