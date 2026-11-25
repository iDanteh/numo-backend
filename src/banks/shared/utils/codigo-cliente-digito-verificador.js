'use strict';

// codigo-cliente-digito-verificador.js — calcula y valida un digito
// verificador modulo 11 para el `codigo` numerico de cliente (catalogo
// externo del ERP, ver Clientes-ids.xlsx, NO sincronizado todavia a NUMO).
//
// Motivo: el catalogo real ocupa el 99.73% del rango 1-33333 (33.242 de
// 33.333 numeros posibles ya son el codigo de ALGUN cliente). Buscar el
// numero pelado dentro del texto libre de un deposito (concepto bancario)
// no distingue un codigo real de una coincidencia — cantidades, folios,
// numeros de producto, nombres de marca ("WD-40") caen todos en ese mismo
// rango. Agregarle un digito verificador reduce drasticamente el choque al
// azar: un numero incidental tendria ademas que cumplir la formula exacta
// del checksum, algo que una cantidad o un folio no hace por casualidad.
//
// Convencion elegida (mod-11 "clasico" de un solo digito, variante propia de
// NUMO, no atada a un estandar externo puntual): pesos ciclicos 2..7
// aplicados de derecha a izquierda sobre los digitos del codigo base; el
// resultado 10 colapsa a 0 para mantener un solo digito decimal (simplificacion
// documentada: ese caso puntual pierde algo de poder de deteccion, pero sigue
// siendo muchisimo mejor que no tener digito verificador).

const PESOS = [2, 3, 4, 5, 6, 7];

function _sumaPonderada(digitos) {
  let suma = 0;
  for (let i = 0; i < digitos.length; i++) {
    const peso = PESOS[i % PESOS.length];
    suma += digitos[digitos.length - 1 - i] * peso;
  }
  return suma;
}

function calcularDigitoVerificador(codigoBase) {
  const n = Number(codigoBase);
  if (!Number.isInteger(n) || n < 0) {
    throw new Error(`codigoBase invalido: ${codigoBase}`);
  }
  const digitos = String(n).split('').map(Number);
  const resto = _sumaPonderada(digitos) % 11;
  const digito = (11 - resto) % 11;
  return digito === 10 ? 0 : digito;
}

// Texto que NUMO/el ERP le comunica al cliente para que lo escriba en el
// concepto de su deposito (ej. codigo 40 -> "406").
function construirCodigoConDigito(codigoBase) {
  return `${codigoBase}${calcularDigitoVerificador(codigoBase)}`;
}

// Valida un numero encontrado en texto libre: separa el ultimo digito como
// supuesto digito verificador y confirma que cuadre contra el resto. Devuelve
// { valido:false } ante cualquier formato que no sea "2 o mas digitos, todos
// numericos" (nunca lanza, pensado para usarse sobre candidatos ya extraidos
// de un regex, no sobre input validado de antemano).
function validarCodigoConDigito(codigoConDigito) {
  const texto = String(codigoConDigito);
  if (!/^\d{2,}$/.test(texto)) {
    return { valido: false };
  }
  const base = texto.slice(0, -1);
  const digito = Number(texto.slice(-1));
  const esperado = calcularDigitoVerificador(base);
  return { valido: digito === esperado, codigoBase: Number(base) };
}

module.exports = {
  calcularDigitoVerificador,
  construirCodigoConDigito,
  validarCodigoConDigito,
};
