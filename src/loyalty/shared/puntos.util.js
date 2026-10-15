'use strict';

/**
 * loyalty/shared/puntos.util.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — aritmética de puntos/dinero en centavos enteros (D3).
 * Sequelize devuelve las columnas DECIMAL como STRING; hacer la matemática en
 * floats de JS arrastraría centavos (1 punto = $1.00 MXN, el error importa).
 * Todo el servicio de ledger opera sobre enteros vía `toCents`/`fromCents` y
 * solo serializa a string DECIMAL al leer/escribir el modelo.
 */

const TELEFONO_RE = /^\d{10}$/;

/**
 * Convierte un valor DECIMAL (string u number, como vienen de Sequelize o del
 * body de un request) a centavos enteros. Nunca usa parseFloat sobre el total
 * — separa parte entera/decimal como texto para no heredar el redondeo
 * binario de IEEE-754 en el límite de 2 decimales.
 */
function toCents(valor) {
  if (valor === null || valor === undefined) {
    throw new Error('toCents: valor requerido.');
  }
  const str = String(valor).trim();
  if (str === '' || Number.isNaN(Number(str))) {
    throw new Error(`toCents: valor numérico inválido: "${valor}"`);
  }

  const negativo = str.startsWith('-');
  const abs = negativo ? str.slice(1) : str;
  const [enteroRaw, decimalRaw = ''] = abs.split('.');
  const entero = enteroRaw === '' ? '0' : enteroRaw;
  const decimal = (decimalRaw + '00').slice(0, 2);

  const centavos = Number(entero) * 100 + Number(decimal || '0');
  return negativo ? -centavos : centavos;
}

/** Convierte centavos enteros de vuelta a un string DECIMAL de 2 decimales. */
function fromCents(centavos) {
  const redondeado = Math.round(centavos);
  const negativo = redondeado < 0;
  const abs = Math.abs(redondeado);
  const entero = Math.floor(abs / 100);
  const decimal = String(abs % 100).padStart(2, '0');
  return `${negativo ? '-' : ''}${entero}.${decimal}`;
}

/**
 * Puntos ganados (en centavos) por una venta, dado el % del nivel (D3):
 * `Math.round(montoCents * pctBasisPoints / 10000)`, donde `pctBasisPoints`
 * es el porcentaje (ej. 3.5) expresado en centésimas de punto porcentual
 * (350), de modo que dividir entre 10000 da la fracción real (0.035).
 */
function calcularPuntos(montoCents, porcentaje) {
  const pctBasisPoints = Math.round(Number(porcentaje) * 100);
  return Math.round((montoCents * pctBasisPoints) / 10000);
}

/**
 * Normaliza un teléfono mexicano a 10 dígitos (D1: el teléfono es la única
 * llave de identidad de la tarjeta). Acepta espacios, guiones, y los
 * prefijos +52 / 521 (celular con el "1" histórico); rechaza cualquier otra
 * cosa de forma explícita — nunca trunca en silencio.
 */
function normalizarTelefono(valor) {
  const soloDigitos = String(valor ?? '').replace(/\D/g, '');

  let digitos = soloDigitos;
  if (digitos.length === 13 && digitos.startsWith('521')) {
    digitos = digitos.slice(3);
  } else if (digitos.length === 12 && digitos.startsWith('52')) {
    digitos = digitos.slice(2);
  }

  if (!TELEFONO_RE.test(digitos)) {
    throw new Error(`Teléfono inválido: "${valor}". Debe tener 10 dígitos (con o sin +52/521).`);
  }
  return digitos;
}

module.exports = { toCents, fromCents, calcularPuntos, normalizarTelefono };
