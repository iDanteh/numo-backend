'use strict';

/**
 * bank-auxiliary.parser.js
 *
 * Importa la tabla auxiliar de cuentas desde un Excel de dos columnas:
 *   Columna A (índice 1) → referencia  (número de cuenta, clave de depósito, etc.)
 *   Columna B (índice 2) → nombre      (persona o empresa)
 *
 * Si la referencia ya existe en la base de datos, actualiza el nombre (upsert).
 */

const ExcelJS        = require('exceljs');
const BankAuxiliary  = require('./BankAuxiliary.model');
const BankMovement   = require('./BankMovement.model');

function toStr(val) {
  if (val === null || val === undefined) return '';
  if (typeof val === 'object' && val.richText) {
    return val.richText.map(r => r.text).join('').trim();
  }
  return String(val).trim();
}

/**
 * @param {Buffer} buffer  Buffer del archivo .xlsx / .xls
 * @returns {{ importados, actualizados, omitidos, errores, total }}
 */
async function parseAuxiliaryFile(buffer) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(buffer);

  const sheet = workbook.worksheets[0];
  if (!sheet) throw new Error('No se encontró la hoja de trabajo en el archivo');

  const rows = [];
  sheet.eachRow((row) => {
    const referencia = toStr(row.values[1]);
    const nombre     = toStr(row.values[2]);
    if (referencia && nombre) rows.push({ referencia, nombre });
  });

  if (rows.length === 0) throw new Error('El archivo no contiene filas válidas');

  let importados   = 0;
  let actualizados = 0;
  let omitidos     = 0;
  const errores    = [];

  for (const row of rows) {
    try {
      const existing = await BankAuxiliary.findOne({ referencia: row.referencia }).select('_id').lean();

      await BankAuxiliary.findOneAndUpdate(
        { referencia: row.referencia },
        { $set: { nombre: row.nombre } },
        { upsert: true, new: true, runValidators: true, setDefaultsOnInsert: true },
      );

      if (existing) actualizados++;
      else importados++;
    } catch (err) {
      errores.push(`"${row.referencia}": ${err.message}`);
      omitidos++;
    }
  }

  return { importados, actualizados, omitidos, errores, total: rows.length };
}

/**
 * Aplica el catálogo auxiliar a los movimientos bancarios.
 *
 * Para cada registro de BankAuxiliary, busca en BankMovement.concepto
 * los movimientos cuyo concepto contenga la referencia (case-insensitive)
 * y les asigna el auxNombre correspondiente.
 *
 * Antes de aplicar, limpia el auxNombre de todos los movimientos activos
 * para que el resultado refleje únicamente el catálogo actual.
 *
 * Si la referencia de un registro es demasiado corta (< 3 caracteres)
 * se omite para evitar falsos positivos.
 *
 * @returns {{ limpiados, actualizados, noEncontrados, total }}
 */
async function applyAuxiliaryMatching() {
  // Paso 1: limpiar auxNombre previo
  const { modifiedCount: limpiados } = await BankMovement.updateMany(
    { isActive: true, auxNombre: { $ne: null } },
    { $set: { auxNombre: null } },
  );

  // Paso 2: cargar todos los registros auxiliares
  const auxiliaries = await BankAuxiliary.find({}, 'referencia nombre').lean();

  let actualizados  = 0;
  let noEncontrados = 0;

  for (const aux of auxiliaries) {
    const ref = aux.referencia.trim();
    if (ref.length < 3) { noEncontrados++; continue; }

    // Escapar caracteres especiales de regex
    const escaped = ref.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re       = new RegExp(escaped, 'i');

    const result = await BankMovement.updateMany(
      { isActive: true, concepto: re },
      { $set: { auxNombre: aux.nombre } },
    );

    if (result.modifiedCount > 0) actualizados += result.modifiedCount;
    else noEncontrados++;
  }

  return { limpiados, actualizados, noEncontrados, total: auxiliaries.length };
}

/**
 * Devuelve un resumen agrupado por auxNombre (clientes identificados).
 * @param {object} filters - { banco?, fechaInicio?, fechaFin? }
 */
async function resumenAuxiliarClientes(filters = {}) {
  const match = { isActive: true, auxNombre: { $ne: null } };
  if (filters.banco) match.banco = filters.banco;
  if (filters.fechaInicio || filters.fechaFin) {
    match.fecha = {};
    if (filters.fechaInicio) match.fecha.$gte = new Date(filters.fechaInicio);
    if (filters.fechaFin)    match.fecha.$lte = new Date(`${filters.fechaFin}T23:59:59.999Z`);
  }

  return BankMovement.aggregate([
    { $match: match },
    {
      $group: {
        _id:            '$auxNombre',
        movimientos:    { $sum: 1 },
        totalDepositos: { $sum: { $ifNull: ['$deposito', 0] } },
        totalRetiros:   { $sum: { $ifNull: ['$retiro',   0] } },
        bancos:         { $addToSet: '$banco' },
        ultimaFecha:    { $max: '$fecha' },
      },
    },
    { $sort: { totalDepositos: -1 } },
  ]);
}

/**
 * Lista movimientos identificados por auxiliar, con paginación.
 * @param {object} filters - { auxNombre?, banco?, fechaInicio?, fechaFin?, tipo?, page?, limit? }
 */
async function listMovimientosAuxiliar(filters = {}) {
  const {
    auxNombre, banco, fechaInicio, fechaFin,
    tipo, page = 1, limit = 50,
  } = filters;

  const match = { isActive: true, auxNombre: { $ne: null } };
  if (auxNombre) match.auxNombre = auxNombre;
  if (banco)     match.banco     = banco;
  if (tipo === 'deposito') match.deposito = { $gt: 0 };
  if (tipo === 'retiro')   match.retiro   = { $gt: 0 };
  if (fechaInicio || fechaFin) {
    match.fecha = {};
    if (fechaInicio) match.fecha.$gte = new Date(fechaInicio);
    if (fechaFin)    match.fecha.$lte = new Date(`${fechaFin}T23:59:59.999Z`);
  }

  const skip = (parseInt(page) - 1) * parseInt(limit);
  const [data, total] = await Promise.all([
    BankMovement.find(match)
      .sort({ fecha: -1, _id: 1 })
      .skip(skip)
      .limit(parseInt(limit))
      .lean(),
    BankMovement.countDocuments(match),
  ]);

  return {
    data,
    pagination: {
      total,
      page:  parseInt(page),
      limit: parseInt(limit),
      pages: Math.ceil(total / parseInt(limit)),
    },
  };
}

module.exports = { parseAuxiliaryFile, applyAuxiliaryMatching, resumenAuxiliarClientes, listMovimientosAuxiliar };
