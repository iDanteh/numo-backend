'use strict';

// clientes-codigo.parser.js — parsea el Excel del ERP (Clientes-ids.xlsx) que trae el
// catálogo completo de clientes. La mayoría de las filas NO tienen `codigo` (36.046 de 69.288
// en el export real) — es esperado, no un error: esas filas se descartan en silencio, solo
// importa el subconjunto de clientes a los que el ERP sí les asignó un código.

const ExcelJS = require('exceljs');

const HEADER_ROW = 1;

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
  if (idx === undefined) return null;
  const valor = row.getCell(idx).value;
  if (valor === null || valor === undefined) return null;
  if (typeof valor === 'object' && 'text' in valor) return String(valor.text).trim(); // rich text
  return String(valor).trim();
}

// Primer campo no vacío entre alias / Nombrecte / razon_social — mismo orden de fallback que
// se validó manualmente sobre el Excel real antes de decidir este matching.
function _nombreConFallback(row, colMap) {
  for (const col of ['alias', 'Nombrecte', 'razon_social']) {
    const v = _celda(row, colMap, col);
    if (v) return v;
  }
  return null;
}

async function parseClientesIdsXlsx(buffer) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(buffer);
  const worksheet = workbook.worksheets[0];
  if (!worksheet || worksheet.rowCount < HEADER_ROW) return [];

  const headerRow = worksheet.getRow(HEADER_ROW);
  const colMap = _mapearEncabezado(headerRow);

  if (colMap['codigo'] === undefined) {
    const err = new Error('Clientes-ids.xlsx no tiene la columna "codigo"');
    err.statusCode = 400;
    throw err;
  }

  const filas = [];
  for (let rowIdx = HEADER_ROW + 1; rowIdx <= worksheet.rowCount; rowIdx++) {
    const row = worksheet.getRow(rowIdx);

    const codigoRaw = _celda(row, colMap, 'codigo');
    if (!codigoRaw) continue; // sin código asignado — mayoría del catálogo, se descarta en silencio

    const codigo = Number(codigoRaw);
    if (!Number.isInteger(codigo) || codigo <= 0) continue; // codigo corrupto/no numérico, se descarta

    filas.push({
      codigo,
      nombre: _nombreConFallback(row, colMap),
      rfc: _celda(row, colMap, 'rfc'),
    });
  }

  return filas;
}

module.exports = { parseClientesIdsXlsx };
