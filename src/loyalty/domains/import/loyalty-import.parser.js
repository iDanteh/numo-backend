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
 *  - clientes.csv                  — Windows-1252/ISO-8859-1, columnas con
 *                                     teléfono/nombre pero SIN saldo de puntos.
 *  - reporteClientesPorVendedor.xlsx — reemplaza a puntos.pdf (2026-10-29, pedido
 *                                     explícito del usuario): saldo CON decimales
 *                                     reales por Tarjeta, SIN teléfono. La sola
 *                                     presencia de una Tarjeta acá es además la
 *                                     señal de "cliente activo" (ver
 *                                     loyalty-import.service.js#activaSugerida).
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

// ── reporteClientesPorVendedor.xlsx (reemplaza a puntos.pdf, 2026-10-29) ─────

// Fila 1 = título del reporte generado ("Clientes por vendedor registrados del...
// al..."), fila 2 = vacía, fila 3 = encabezados reales (Tarjeta, Cliente,
// Vendedor, Fecha de registro, Saldo) — verificado contra el archivo real.
const VENDEDOR_HEADER_ROW = 3;
const VENDEDOR_COLUMNAS_REQUERIDAS = ['Tarjeta', 'Saldo'];

/**
 * Parsea el Excel "reporteClientesPorVendedor" (reemplaza a puntos.pdf, pedido
 * explícito del usuario 2026-10-29) — hoja "Clientes por Vendedor", encabezados
 * en la FILA 3 (no la 1), datos desde la fila 4. Solo se usan `Tarjeta` y
 * `Saldo` — `Cliente`/`Vendedor`/`Fecha de registro` no aportan nada que
 * clientes.csv no traiga ya igual o mejor.
 *
 * `Tarjeta` viene con tipo mixto en el archivo real: a veces string con ceros a
 * la izquierda ("016"), a veces número puro (9277) — `_celda()` ya la lee como
 * `String(val).trim()`, y `normalizarTarjeta()` tolera ambos formatos igual.
 *
 * `Saldo` SÍ trae decimales reales (14.31, 294.75) — a diferencia del viejo
 * `parsePuntosLines` (que hacía `parseInt` sobre saldos sin decimales del PDF),
 * acá se usa `Number()` para no truncarlos (el schema ya soporta decimales,
 * `LoyaltyPointsLedger.puntos` es `DECIMAL(10,2)`).
 *
 * La sola PRESENCIA de una Tarjeta en este reporte (con cualquier Saldo,
 * incluido 0) es también la señal de "cliente activo" que usa
 * `construirConcentrado` (ver loyalty-import.service.js#activaSugerida) — por
 * eso nunca se descarta una fila por tener Saldo 0, igual que hacía el parser
 * de puntos.pdf que reemplaza.
 *
 * @param {Buffer} buffer
 * @returns {Promise<Array<{ tarjetaRaw: string, tarjetaNormalizada: string, nombre: string, saldo: number }>>}
 */
async function parseVendedorXlsx(buffer) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(buffer);
  const worksheet = workbook.worksheets[0];
  if (!worksheet || worksheet.rowCount < VENDEDOR_HEADER_ROW) return [];

  const headerRow = worksheet.getRow(VENDEDOR_HEADER_ROW);
  const colMap = _mapearEncabezado(headerRow);

  const faltantes = VENDEDOR_COLUMNAS_REQUERIDAS.filter((c) => colMap[c.toLowerCase()] === undefined);
  if (faltantes.length) {
    const err = new Error(`reporteClientesPorVendedor.xlsx no tiene las columnas esperadas: ${faltantes.join(', ')}`);
    err.statusCode = 400;
    throw err;
  }

  const filas = [];
  for (let rowIdx = VENDEDOR_HEADER_ROW + 1; rowIdx <= worksheet.rowCount; rowIdx++) {
    const row = worksheet.getRow(rowIdx);

    const tarjetaRaw = _celda(row, colMap, 'Tarjeta');
    const nombre = _celda(row, colMap, 'Cliente');
    const saldoRaw = _celda(row, colMap, 'Saldo');

    // Fila completamente vacía (línea en blanco al final del reporte) → se descarta.
    if (!tarjetaRaw && !nombre) continue;

    const saldoNum = Number(String(saldoRaw).replace(/,/g, ''));
    const saldo = Number.isFinite(saldoNum) ? saldoNum : 0;

    filas.push({
      tarjetaRaw,
      tarjetaNormalizada: normalizarTarjeta(tarjetaRaw),
      nombre,
      saldo,
    });
  }

  return filas;
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
  parseVendedorXlsx,
  parseCorreccionesXlsx,
};
