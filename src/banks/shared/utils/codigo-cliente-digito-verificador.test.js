'use strict';

const {
  calcularDigitoVerificador,
  construirCodigoConDigito,
  validarCodigoConDigito,
} = require('./codigo-cliente-digito-verificador');

describe('calcularDigitoVerificador', () => {
  test('es determinista: mismo codigo base siempre da el mismo digito', () => {
    const d1 = calcularDigitoVerificador(24788);
    const d2 = calcularDigitoVerificador(24788);
    expect(d1).toBe(d2);
    expect(d1).toBeGreaterThanOrEqual(0);
    expect(d1).toBeLessThanOrEqual(9);
  });

  test('siempre devuelve un solo digito decimal (0-9), incluso cuando el resto mod 11 da 10', () => {
    for (let n = 1; n <= 2000; n++) {
      const d = calcularDigitoVerificador(n);
      expect(Number.isInteger(d)).toBe(true);
      expect(d).toBeGreaterThanOrEqual(0);
      expect(d).toBeLessThanOrEqual(9);
    }
  });

  test('rechaza codigos base invalidos (no enteros, negativos)', () => {
    expect(() => calcularDigitoVerificador('abc')).toThrow();
    expect(() => calcularDigitoVerificador(-5)).toThrow();
    expect(() => calcularDigitoVerificador(3.5)).toThrow();
  });
});

describe('construirCodigoConDigito', () => {
  test('pega el digito verificador al final del codigo base', () => {
    const codigo = construirCodigoConDigito(40);
    expect(codigo).toMatch(/^40\d$/);
    expect(codigo).toHaveLength(3);
  });
});

describe('validarCodigoConDigito', () => {
  test('un codigo construido por construirCodigoConDigito siempre valida como valido', () => {
    for (const base of [1, 7, 40, 999, 12398, 24788, 33333]) {
      const conDigito = construirCodigoConDigito(base);
      const resultado = validarCodigoConDigito(conDigito);
      expect(resultado.valido).toBe(true);
      expect(resultado.codigoBase).toBe(base);
    }
  });

  test('invalida si el digito verificador fue alterado', () => {
    const conDigito = construirCodigoConDigito(40);
    const digitoReal = Number(conDigito.slice(-1));
    const digitoAlterado = (digitoReal + 1) % 10;
    const alterado = `${conDigito.slice(0, -1)}${digitoAlterado}`;
    expect(validarCodigoConDigito(alterado).valido).toBe(false);
  });

  test('formatos no validos (texto, un solo digito, vacio) devuelven valido:false sin lanzar', () => {
    expect(validarCodigoConDigito('abc').valido).toBe(false);
    expect(validarCodigoConDigito('4').valido).toBe(false);
    expect(validarCodigoConDigito('').valido).toBe(false);
    expect(validarCodigoConDigito('12 34').valido).toBe(false);
  });

  // Esta es la propiedad que justifica agregar el digito verificador: un
  // numero que aparece por casualidad en texto libre (cantidad, folio, numero
  // de producto) NO deberia validar como "codigo real" salvo coincidencia
  // rara. Se mide contra errores de transcripcion de UN digito (el típo mas
  // comun al escribir un numero a mano) sobre un universo grande de codigos,
  // no contra un solo ejemplo feliz.
  test('atrapa la enorme mayoria de errores de transcripcion de un solo digito', () => {
    let total = 0;
    let detectados = 0;
    for (let base = 1; base <= 5000; base++) {
      const conDigito = construirCodigoConDigito(base);
      for (let pos = 0; pos < conDigito.length - 1; pos++) { // no altera el digito verificador mismo
        const original = conDigito[pos];
        for (let reemplazo = 0; reemplazo <= 9; reemplazo++) {
          if (String(reemplazo) === original) continue;
          const alterado = conDigito.slice(0, pos) + reemplazo + conDigito.slice(pos + 1);
          total++;
          if (!validarCodigoConDigito(alterado).valido) detectados++;
        }
      }
    }
    const tasaDeteccion = detectados / total;
    expect(tasaDeteccion).toBeGreaterThan(0.85);
  });
});
