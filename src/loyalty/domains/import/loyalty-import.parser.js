'use strict';

/**
 * domains/import/loyalty-import.parser.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — migración de clientes legados ("Club Tuberos"). Este
 * parser SOLO extrae y normaliza datos de los dos reportes fuente; no hace
 * ninguna escritura en base de datos (eso queda fuera de alcance de este
 * cambio — ver `loyalty-import.service.js#construirConcentrado`, que es la
 * capa que cruza ambos arrays).
 *
 * Fuentes:
 *  - clientes.csv  — Windows-1252/ISO-8859-1, columnas con teléfono/nombre
 *                     pero SIN saldo de puntos.
 *  - puntos.pdf    — texto plano (no escaneado), 39 páginas, con el saldo
 *                     por Tarjeta pero SIN teléfono.
 *
 * Ambos se cruzan por `Tarjeta` (normalizada, ver `normalizarTarjeta`) porque
 * el sistema nuevo identifica la tarjeta por teléfono, no por el folio legado.
 */

const { Readable } = require('stream');
const ExcelJS = require('exceljs');

// ── Normalización de Tarjeta ───────────────────────────────────────────────
//
// Quirks reales confirmados a mano contra los reportes (ver business context
// del cambio):
//   1. Ceros a la izquierda inconsistentes entre clientes.csv y puntos.pdf
//      ("44" vs "044") → se normalizan quitando los ceros a la izquierda.
//   2. Códigos "CF-" con separador inconsistente ("CF-3615" vs "CF 2221")
//      → se normalizan a un único formato: "CF-" + dígitos, sin espacios.
//
// Tarjetas que no matchean ninguno de los dos patrones (vacías o con un
// formato no previsto) se devuelven tal cual, en mayúsculas y recortadas —
// nunca se descartan ni se lanza un error, para no perder filas reales.
function normalizarTarjeta(raw) {
  const s = String(raw ?? '').trim();
  if (!s) return '';

  const upper = s.toUpperCase();

  const cfMatch = upper.match(/^CF[\s-]*(\d+)$/);
  if (cfMatch) {
    return `CF-${cfMatch[1]}`;
  }

  if (/^\d+$/.test(upper)) {
    // parseInt + toString quita los ceros a la izquierda sin arriesgar
    // overflow: los folios reales tienen pocos dígitos.
    return String(parseInt(upper, 10));
  }

  return upper;
}

// ── clientes.csv ────────────────────────────────────────────────────────────

const CLIENTES_COLUMNAS_REQUERIDAS = ['Tarjeta', 'Nombre', 'Apellidos', 'Categoria', 'Celular', 'Telefono'];

/**
 * Lee el encabezado (fila 1) de la hoja generada por exceljs a partir del CSV
 * y arma un mapa { nombreColumna → índice 1-based }. Match exacto (trim +
 * case-insensitive) — las columnas de clientes.csv están documentadas y no
 * requieren el matching por subcadena/alias que usa bank.parser.js.
 */
function _mapearEncabezado(headerRow) {
  const colMap = {};
  headerRow.eachCell((cell, colIdx) => {
    const text = String(cell.value ?? '').trim();
    if (!text) return;
    colMap[text.toLowerCase()] = colIdx;
  });
  return colMap;
}

function _celda(row, colMap, nombreColumna) {
  const idx = colMap[nombreColumna.toLowerCase()];
  if (idx === undefined) return '';
  const val = row.getCell(idx).value;
  return val === null || val === undefined ? '' : String(val).trim();
}

/**
 * Parsea el buffer de clientes.csv (Windows-1252/ISO-8859-1) usando
 * `exceljs` (`workbook.csv.read`) — ya es dependencia del proyecto (usada en
 * bank.parser.js) y resuelve RFC4180 correctamente, incluyendo campos entre
 * comillas con saltos de línea embebidos (direcciones multi-línea reales en
 * este reporte). Nunca se parte el archivo por `\n` a mano.
 *
 * Decodificación: igual criterio que `visor/sat/download.js` (comentario
 * "Reintentar decodificando como latin1 (Windows-1252) — común en archivos
 * SAT") — Node decodifica 'latin1' byte-a-byte, que coincide con
 * Windows-1252/ISO-8859-1 para todo el rango de acentos/ñ que usa este
 * archivo. No se agregó `iconv-lite`: no es dependencia del proyecto y este
 * archivo no usa el rango C1 (0x80-0x9F) donde Windows-1252 difiere de
 * ISO-8859-1.
 *
 * @param {Buffer} buffer
 * @returns {Promise<Array<{ tarjetaRaw: string, tarjetaNormalizada: string, nombre: string, celular: string, telefono: string, categoriaOriginal: string }>>}
 */
async function parseClientesCsv(buffer) {
  const texto = buffer.toString('latin1');
  // Re-codificar a utf8: fast-csv (usado por exceljs internamente) decodifica
  // el stream como utf8; al re-encodear el string ya corregido obtenemos los
  // bytes utf8 correctos para los mismos caracteres.
  const utf8Buffer = Buffer.from(texto, 'utf8');
  const stream = Readable.from(utf8Buffer);

  const workbook = new ExcelJS.Workbook();
  // `map: (v) => v` desactiva la coerción automática de fast-csv/exceljs a
  // Number/Date — necesitamos las columnas como string crudo (ej. "Tarjeta"
  // con ceros a la izquierda, "CP" con ceros a la izquierda, fechas con
  // formato variable que no nos interesa parsear aquí).
  const worksheet = await workbook.csv.read(stream, { map: (v) => v });

  if (worksheet.rowCount < 1) return [];

  const headerRow = worksheet.getRow(1);
  const colMap = _mapearEncabezado(headerRow);

  const faltantes = CLIENTES_COLUMNAS_REQUERIDAS.filter((c) => colMap[c.toLowerCase()] === undefined);
  if (faltantes.length) {
    const err = new Error(`clientes.csv no tiene las columnas esperadas: ${faltantes.join(', ')}`);
    err.statusCode = 400;
    throw err;
  }

  const filas = [];
  for (let rowIdx = 2; rowIdx <= worksheet.rowCount; rowIdx++) {
    const row = worksheet.getRow(rowIdx);

    const tarjetaRaw = _celda(row, colMap, 'Tarjeta');
    const nombreCol = _celda(row, colMap, 'Nombre');
    const apellidos = _celda(row, colMap, 'Apellidos');
    const categoriaOriginal = _celda(row, colMap, 'Categoria');
    const celular = _celda(row, colMap, 'Celular');
    const telefono = _celda(row, colMap, 'Telefono');
    // Correo: columna documentada en clientes.csv, puramente informativa (no es
    // llave de identidad) — se usa para contactar a clientes que quedan en
    // espera en loyalty_pending_enrollments (ver loyalty-import-apply.service.js).
    const correo = _celda(row, colMap, 'Correo');

    // Enriquecimiento (2026-10-23) — registro histórico simple, nunca llave de identidad ni
    // usado para lógica de negocio. A propósito NO están en CLIENTES_COLUMNAS_REQUERIDAS: deben
    // ser opcionales, un CSV sin alguna de ellas no debe romper el import (ej. un CSV viejo
    // generado antes de que el reporte las incluyera). "Vendedor" se deja afuera a propósito
    // (a futuro guardará el id de Auth0, fuera de alcance de este cambio).
    const apodo = _celda(row, colMap, 'Apodo');
    const sexo = _celda(row, colMap, 'Sexo');
    const fechaNacimiento = _celda(row, colMap, 'Fecha nacimiento');
    const especialidades = _celda(row, colMap, 'Especialidades');
    const sucursal = _celda(row, colMap, 'Sucursal');
    const calle = _celda(row, colMap, 'Calle');
    const numExterior = _celda(row, colMap, 'Num exterior');
    const numInterior = _celda(row, colMap, 'Num Interior');
    const colonia = _celda(row, colMap, 'Colonia');
    const municipio = _celda(row, colMap, 'Municipio');
    const estado = _celda(row, colMap, 'Estado');
    const cp = _celda(row, colMap, 'CP');
    const comentarios = _celda(row, colMap, 'Comentarios');

    // Fila completamente vacía (línea en blanco al final del archivo) → se
    // descarta; distinto de una fila con Tarjeta vacía pero nombre real,
    // que sí se conserva (es uno de los casos reales de duplicado, ver D4).
    if (!tarjetaRaw && !nombreCol && !apellidos) continue;

    const nombre = `${nombreCol} ${apellidos}`.replace(/\s+/g, ' ').trim();

    filas.push({
      tarjetaRaw,
      tarjetaNormalizada: normalizarTarjeta(tarjetaRaw),
      nombre,
      // Apellidos (2026-10-23) — enfoque aditivo: `nombre` sigue siendo el combinado de siempre,
      // esto es solo granularidad extra para quien la necesite (ver LoyaltyCard.js#apellidos).
      apellidos,
      celular,
      telefono,
      correo,
      categoriaOriginal,
      apodo,
      sexo,
      fechaNacimiento,
      especialidades,
      sucursal,
      calle,
      numExterior,
      numInterior,
      colonia,
      municipio,
      estado,
      cp,
      comentarios,
    });
  }

  return filas;
}

// ── puntos.pdf ───────────────────────────────────────────────────────────────

// CORRECCIÓN 2026-10-19: verificado a mano contra el puntos.pdf real con
// pdf-parse — a diferencia de pdfplumber (Python), pdf-parse/pdfjs-dist NO
// preserva los espacios que separaban visualmente las columnas de la tabla en
// el PDF. El texto real que entrega es, por ejemplo:
//   "12044RAYMUNDO JAVIER RICARDEZ MORALES7,703"
//   "199999900CLIENTE PRUEBA2 no borrar238,620"
//   "19CF-3615CRISTINA DIAZ GUTIERREZ5,134"
// es decir, <rowIndex><tarjeta><nombre con sus propios espacios><saldo>, TODO
// concatenado sin separador entre rowIndex/tarjeta y entre nombre/saldo. Una
// versión anterior de este archivo asumía espacios (y sus tests lo
// confirmaban con fixtures escritos a mano) pero nunca se verificó contra el
// PDF real — contra el archivo real parseaba 0 de 2,161 filas.
//
// Para separar rowIndex de tarjeta (ambos corridas de dígitos pegadas, sin
// delimitador) se aprovecha que el índice de fila es GLOBAL y estrictamente
// secuencial a lo largo de las 39 páginas (1..2161, verificado contra el
// archivo real, sin reinicios por página) — se lleva un contador del índice
// esperado y se exige que la línea empiece EXACTAMENTE con ese número seguido
// inmediatamente (sin espacio) por dígitos (tarjeta numérica) o por "CF"
// (tarjeta con prefijo). Cualquier línea que no calce así (encabezados, pies
// de página, líneas en blanco) se descarta sin avanzar el contador, de forma
// que el parser se resincroniza solo en la siguiente fila de datos real.
const PUNTOS_TARJETA_CF_RE = /^CF[\s-]*(\d+)/i;
const PUNTOS_TARJETA_NUM_RE = /^\d+/;
// Saldo: token final compuesto solo por dígitos y comas (incluye "0" plano,
// sin coma de miles, para saldos cero — confirmado real al final del reporte).
const PUNTOS_NOMBRE_SALDO_RE = /^(.+?)([\d,]+)$/;

/**
 * Parsea las líneas de texto plano ya extraídas de puntos.pdf. Separado de
 * `parsePuntosPdf` a propósito (ver tests) para poder probar la lógica de
 * parseo de líneas sin tener que generar un PDF real en memoria.
 *
 * @param {string} texto
 * @returns {Array<{ tarjetaRaw: string, tarjetaNormalizada: string, nombre: string, saldo: number }>}
 */
function parsePuntosLines(texto) {
  const lineas = String(texto ?? '').split(/\r?\n/);
  const filas = [];
  let indiceEsperado = 1;

  for (const lineaCruda of lineas) {
    const linea = lineaCruda.trim();
    if (!linea) continue;

    const prefijo = String(indiceEsperado);
    if (!linea.startsWith(prefijo)) continue; // encabezado/pie/línea ajena — no avanza el contador

    const resto = linea.slice(prefijo.length);

    let tarjetaRaw = null;
    let restoSinTarjeta = null;
    const cfMatch = resto.match(PUNTOS_TARJETA_CF_RE);
    if (cfMatch) {
      tarjetaRaw = cfMatch[0];
      restoSinTarjeta = resto.slice(cfMatch[0].length);
    } else {
      const numMatch = resto.match(PUNTOS_TARJETA_NUM_RE);
      if (numMatch) {
        tarjetaRaw = numMatch[0];
        restoSinTarjeta = resto.slice(numMatch[0].length);
      }
    }
    // Ni dígitos ni "CF" justo después del índice esperado → esta línea no es
    // la fila de datos de ese índice (ej. pie de página "1 de 3919/10/2026...");
    // se descarta sin avanzar el contador, para resincronizar en la próxima.
    if (tarjetaRaw === null) continue;

    const nombreSaldoMatch = restoSinTarjeta.match(PUNTOS_NOMBRE_SALDO_RE);
    if (!nombreSaldoMatch) continue;

    const [, nombreCrudo, saldoToken] = nombreSaldoMatch;
    const saldo = parseInt(saldoToken.replace(/,/g, ''), 10);
    if (Number.isNaN(saldo)) continue;

    filas.push({
      tarjetaRaw,
      tarjetaNormalizada: normalizarTarjeta(tarjetaRaw),
      nombre: nombreCrudo.replace(/\s+/g, ' ').trim(),
      saldo,
    });
    indiceEsperado += 1;
  }

  return filas;
}

/**
 * Parsea el buffer de puntos.pdf (PDF con texto embebido, NO escaneado) con
 * `pdf-parse` — ya es dependencia del proyecto (usada en
 * `banks/domains/collection-requests/ocr-engine.js#extractTextFromPdf`).
 *
 * @param {Buffer} buffer
 * @returns {Promise<Array<{ tarjetaRaw: string, tarjetaNormalizada: string, nombre: string, saldo: number }>>}
 */
async function parsePuntosPdf(buffer) {
  const pdfParse = require('pdf-parse');
  const data = await pdfParse(buffer);
  return parsePuntosLines(data.text || '');
}

// ── correcciones.xlsx (re-subida de filas bloqueantes corregidas a mano) ─────

// Motivo: columna informativa agregada por el export del frontend (ver
// descargarBloqueantes() en importar-clientes.component.ts) — nunca se exige
// ni se lee acá, solo ayuda al admin a decidir qué corregir en Excel.
const CORRECCIONES_COLUMNAS = ['Tarjeta', 'Nombre', 'Celular', 'Telefono', 'Categoria', 'Saldo'];

/**
 * Parsea el Excel de correcciones que el admin vuelve a subir tras editar a mano el export de
 * filas bloqueantes (`loyalty-import-apply.service.js#construirFilasDesdeCorrecciones` las
 * convierte al shape de fila del concentrado). Hoja plana, primera fila = encabezados
 * (match case-insensitive, mismo criterio que `_mapearEncabezado` de clientes.csv).
 *
 * Saldo: validación suave — un humano ya revisó la fila a mano, así que un valor que no parsea
 * a un número ≥0 se asigna 0 en vez de tronar el archivo completo (mismo espíritu que
 * `migrate-lealtad-csv.js#_validarSaldoRaw`, pero sin abortar).
 *
 * @param {Buffer} buffer
 * @returns {Promise<Array<{ tarjetaRaw: string, nombre: string, celular: string, telefono: string, categoriaOriginal: string, saldo: number }>>}
 */
async function parseCorreccionesXlsx(buffer) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(buffer);
  const worksheet = workbook.worksheets[0];
  if (!worksheet || worksheet.rowCount < 1) return [];

  const headerRow = worksheet.getRow(1);
  const colMap = _mapearEncabezado(headerRow);

  const faltantes = CORRECCIONES_COLUMNAS.filter((c) => colMap[c.toLowerCase()] === undefined);
  if (faltantes.length) {
    const err = new Error(`El Excel de correcciones no tiene las columnas esperadas: ${faltantes.join(', ')}`);
    err.statusCode = 400;
    throw err;
  }

  const filas = [];
  for (let rowIdx = 2; rowIdx <= worksheet.rowCount; rowIdx++) {
    const row = worksheet.getRow(rowIdx);

    const tarjetaRaw = _celda(row, colMap, 'Tarjeta');
    const nombre = _celda(row, colMap, 'Nombre');
    const celular = _celda(row, colMap, 'Celular');
    const telefono = _celda(row, colMap, 'Telefono');
    const categoriaOriginal = _celda(row, colMap, 'Categoria');
    const saldoRaw = _celda(row, colMap, 'Saldo');

    // Fila completamente vacía (línea en blanco al final del archivo) → se descarta.
    if (!tarjetaRaw && !nombre) continue;

    const saldoNum = Number(String(saldoRaw).replace(/,/g, ''));
    const saldo = Number.isFinite(saldoNum) && saldoNum >= 0 ? saldoNum : 0;

    filas.push({ tarjetaRaw, nombre, celular, telefono, categoriaOriginal, saldo });
  }

  return filas;
}

module.exports = {
  normalizarTarjeta,
  parseClientesCsv,
  parsePuntosLines,
  parsePuntosPdf,
  parseCorreccionesXlsx,
};
