'use strict';

// netpay-reporte-parser.service.js — parsea el Excel REAL que exporta Netpay (2 hojas:
// "Resumen" y "Ventas Tarjeta Presente") a la forma que persiste NetpayReporte.model.js.
// Mismo patrón que bank.service.js#_parseConciliacionExcel (ExcelJS, headerMap desde una
// fila de encabezados, manejo de celdas fórmula/richText) — extendido acá porque el reporte
// de Netpay NO tiene sus encabezados en la fila 1: trae varias filas de branding/título
// antes de cada tabla real, y la posición exacta puede variar de un reporte a otro. Por eso
// se BUSCA la fila de encabezados (por las claves normalizadas que debe contener) en vez de
// asumir un número de fila fijo — verificado contra los 2 archivos reales del repo
// (F0-Netpay.xlsx, 20260925_DetalleDepósitos.xlsx): la tabla de detalle arranca en la fila
// 17/18 en ambos, pero nada garantiza que Netpay no agregue una fila de branding más el día
// de mañana.
//
// Nunca deja pasar un 500/stack trace de Excel corrupto o con hojas/columnas inesperadas —
// siempre BadRequestError con mensaje legible (ver netpay-reporte.service.js#cargarReporte,
// que además envuelve cualquier error no tipado de ExcelJS).

const ExcelJS = require('exceljs');
const { BadRequestError } = require('../../shared/errors/AppError');

const MESES_ES = {
  ene: 0, feb: 1, mar: 2, abr: 3, may: 4, jun: 5,
  jul: 6, ago: 7, sep: 8, oct: 9, nov: 10, dic: 11,
};

// Normaliza un encabezado de columna a una clave estable: sin acentos, minúsculas, sin
// espacios/paréntesis/signos — "Comisión Base (%)" -> "comision_base_pct", "IVA Comisiones
// (16%)" -> "iva_comisiones_16pct", "Comisiones + IVA" -> "comisiones_iva". % y $ se
// convierten a palabra ANTES de eliminar el resto de la puntuación, para no colisionar
// "Comisión Base (%)" y "Comisión Base ($)" en la misma clave.
function _normalizarHeader(v) {
  let s = String(v ?? '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase().trim();
  s = s.replace(/%/g, 'pct').replace(/\$/g, 'monto');
  s = s.replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
  return s;
}

function _valorCelda(cell) {
  const v = cell.value;
  if (v === null || v === undefined) return null;
  if (typeof v === 'object' && !(v instanceof Date)) {
    if ('result' in v) return v.result;
    if ('richText' in v) return v.richText.map(t => t.text ?? '').join('');
  }
  return v;
}

// Parsea 'DD-MM-YYYY' (fechas de movimiento/trx del reporte, ej. '25-09-2026').
function _parseFechaDDMMYYYY(raw) {
  if (raw instanceof Date && !isNaN(raw.getTime())) return raw;
  const s = String(raw ?? '').trim();
  const m = s.match(/^(\d{1,2})-(\d{1,2})-(\d{4})$/);
  if (!m) return null;
  return new Date(Date.UTC(+m[3], +m[2] - 1, +m[1]));
}

// Parsea 'DD-mmm-YYYY' (rango de "Periodo:", mes en español abreviado, ej. '25-sep-2026').
function _parseFechaMesEs(raw) {
  const s = String(raw ?? '').trim().toLowerCase();
  const m = s.match(/^(\d{1,2})-([a-z]{3})-(\d{4})$/);
  if (!m) return null;
  const mes = MESES_ES[m[2]];
  if (mes === undefined) return null;
  return new Date(Date.UTC(+m[3], mes, +m[1]));
}

function _numero(raw) {
  if (typeof raw === 'number') return raw;
  const n = parseFloat(String(raw ?? '0').replace(/,/g, ''));
  return Number.isFinite(n) ? n : 0;
}

function _texto(raw) {
  const s = String(raw ?? '').trim();
  return s || null;
}

// Busca, dentro de las primeras `maxFilas` filas de la hoja, la primera fila que contenga
// TODAS las claves normalizadas de `clavesEsperadas` — esa es la fila de encabezados real de
// una tabla (la posición exacta varía según branding/título previo del reporte).
function _buscarFilaHeader(sheet, clavesEsperadas, maxFilas = 60) {
  const tope = Math.min(maxFilas, sheet.rowCount || maxFilas);
  for (let r = 1; r <= tope; r++) {
    const headerMap = {};
    sheet.getRow(r).eachCell({ includeEmpty: false }, (cell, col) => {
      headerMap[col] = _normalizarHeader(_valorCelda(cell));
    });
    const claves = new Set(Object.values(headerMap));
    if (clavesEsperadas.every(k => claves.has(k))) {
      return { filaHeader: r, headerMap };
    }
  }
  return null;
}

// Busca una etiqueta (ej. "Monto transaccionado") y devuelve el primer valor NUMÉRICO en la
// misma fila, después de la celda de la etiqueta — mismo layout que usa Netpay para todo
// bloque "etiqueta ... valor" (Resumen de ventas, Depósitos y cargos del periodo → Periodo).
function _buscarValorPorEtiqueta(sheet, etiquetaNormalizada, maxFilas = 60) {
  const tope = Math.min(maxFilas, sheet.rowCount || maxFilas);
  for (let r = 1; r <= tope; r++) {
    const row = sheet.getRow(r);
    let encontrada = false;
    let valor = null;
    row.eachCell({ includeEmpty: false }, (cell) => {
      if (valor !== null) return;
      const val = _valorCelda(cell);
      if (!encontrada) {
        if (_normalizarHeader(val) === etiquetaNormalizada) encontrada = true;
      } else if (typeof val === 'number') {
        valor = val;
      }
    });
    if (encontrada) return valor;
  }
  return null;
}

// Mismo criterio que _buscarValorPorEtiqueta pero para el primer valor de TEXTO (ej. el
// rango de "Periodo:").
function _buscarTextoPorEtiqueta(sheet, etiquetaNormalizada, maxFilas = 60) {
  const tope = Math.min(maxFilas, sheet.rowCount || maxFilas);
  for (let r = 1; r <= tope; r++) {
    const row = sheet.getRow(r);
    let encontrada = false;
    let valor = null;
    row.eachCell({ includeEmpty: false }, (cell) => {
      if (valor !== null) return;
      const val = _valorCelda(cell);
      if (!encontrada) {
        if (_normalizarHeader(val) === etiquetaNormalizada) encontrada = true;
      } else if (typeof val === 'string' && val.trim()) {
        valor = val.trim();
      }
    });
    if (encontrada) return valor;
  }
  return null;
}

// Extrae el terminalID informativo del "Order ID" (ej.
// '260924181626-2840746396783601' -> '2840746396783601') — se toma la porción posterior al
// ÚLTIMO guión y se recorta a 10 caracteres (verificado contra los 2 reportes reales del
// repo: los 10 primeros dígitos de esa porción son estables entre TODAS las filas de un
// mismo reporte — el mismo terminal/tienda — mientras el resto varía por transacción; mismo
// largo que terminalID ya confirmado contra Kore real en netpay-panel, ej. '2841258490').
function _extraerTerminalID(orderId) {
  const s = String(orderId ?? '');
  const partes = s.split('-');
  if (partes.length < 2) return null;
  const sufijo = partes[partes.length - 1].trim();
  return sufijo ? sufijo.slice(0, 10) : null;
}

// ── Hoja "Resumen" — tabla "Depósitos y cargos del periodo" + rango "Periodo:" ─────────────
function _parseResumen(sheet) {
  const encontrado = _buscarFilaHeader(sheet, ['fecha_de_movimiento', 'clave_rastreo', 'cuenta_deposito', 'monto_deposito']);
  if (!encontrado) {
    throw new BadRequestError(
      'No se encontró la tabla "Depósitos y cargos del periodo" en la hoja "Resumen" (faltan columnas Fecha de Movimiento/Clave Rastreo/Cuenta Depósito/Monto Depósito).',
    );
  }
  const { filaHeader, headerMap } = encontrado;

  const depositos = [];
  for (let r = filaHeader + 1; r <= sheet.rowCount; r++) {
    const row = sheet.getRow(r);
    const obj = {};
    let huboCelda = false;
    row.eachCell({ includeEmpty: false }, (cell, col) => {
      const key = headerMap[col];
      if (!key) return;
      huboCelda = true;
      obj[key] = _valorCelda(cell);
    });
    if (!huboCelda) break;

    const fecha = _parseFechaDDMMYYYY(obj['fecha_de_movimiento']);
    if (!fecha) break; // fila "Total" o nota al pie — deja de ser una fila de depósito

    depositos.push({
      fechaMovimiento: fecha,
      claveRastreo:    _texto(obj['clave_rastreo']),
      cuentaDeposito:  _texto(obj['cuenta_deposito']),
      montoDeposito:   _numero(obj['monto_deposito']),
    });
  }

  if (depositos.length === 0) {
    throw new BadRequestError('La hoja "Resumen" no contiene ninguna fila de depósito bajo "Depósitos y cargos del periodo".');
  }
  if (depositos.length > 1) {
    throw new BadRequestError(
      'El reporte contiene más de un depósito en la hoja "Resumen" — cada carga debe representar un solo depósito (claveRastreo único).',
    );
  }
  const [deposito] = depositos;
  if (!deposito.claveRastreo) {
    throw new BadRequestError('La fila de depósito no trae Clave Rastreo — no se puede identificar el reporte.');
  }

  const periodoTexto = _buscarTextoPorEtiqueta(sheet, 'periodo');
  let periodoDesde = null;
  let periodoHasta = null;
  if (periodoTexto) {
    const partes = periodoTexto.split(/\s+-\s+/);
    if (partes.length === 2) {
      periodoDesde = _parseFechaMesEs(partes[0]);
      periodoHasta = _parseFechaMesEs(partes[1]);
    }
  }

  return { ...deposito, periodoDesde, periodoHasta };
}

// ── Hoja "Ventas Tarjeta Presente" — "Resumen de ventas" + tabla "Ventas pagadas durante
// periodo" ───────────────────────────────────────────────────────────────────────────────
function _parseResumenVentas(sheet) {
  const montoTransaccionado = _buscarValorPorEtiqueta(sheet, 'monto_transaccionado');
  const comisiones          = _buscarValorPorEtiqueta(sheet, 'comisiones');
  const iva                 = _buscarValorPorEtiqueta(sheet, 'iva');
  const montoDepositado     = _buscarValorPorEtiqueta(sheet, 'monto_depositado');

  if ([montoTransaccionado, comisiones, iva, montoDepositado].some(v => v === null)) {
    throw new BadRequestError(
      'No se encontró el "Resumen de ventas" en la hoja "Ventas Tarjeta Presente" (Monto transaccionado/Comisiones/Iva/Monto depositado).',
    );
  }
  return { montoTransaccionado, comisiones, iva, montoDepositado };
}

const COLUMNAS_FOLIO_REQUERIDAS = [
  'fecha_de_deposito', 'clave_rastreo', 'cuenta_deposito', 'nombre_empresa', 'sucursal',
  'store_id', 'monto_deposito', 'fecha_trx', 'hora_de_trx', 'monto_de_trx',
  'comision_base_pct', 'comision_base_monto', 'comisiones_iva', 'banco', 'tipo_de_tarjeta',
  'codigo_de_autorizacion', 'order_id', 'referencia',
];

function _parseFolios(sheet) {
  const encontrado = _buscarFilaHeader(sheet, COLUMNAS_FOLIO_REQUERIDAS);
  if (!encontrado) {
    throw new BadRequestError(
      'No se encontró la tabla "Ventas pagadas durante periodo" en la hoja "Ventas Tarjeta Presente" (faltan columnas esperadas — verifica que el archivo sea el reporte real de Netpay).',
    );
  }
  const { filaHeader, headerMap } = encontrado;

  // La clave EXACTA de "IVA Comisiones (16%)" depende del porcentaje vigente (16% hoy) —
  // se busca por prefijo en vez de hardcodear "16pct" para no romper si Netpay cambia la
  // tasa de IVA en el futuro.
  const keyIvaComision = Object.values(headerMap).find(k => k && k.startsWith('iva_comisiones_')) ?? null;

  const folios = [];
  for (let r = filaHeader + 1; r <= sheet.rowCount; r++) {
    const row = sheet.getRow(r);
    const obj = {};
    let huboCelda = false;
    row.eachCell({ includeEmpty: false }, (cell, col) => {
      const key = headerMap[col];
      if (!key) return;
      huboCelda = true;
      obj[key] = _valorCelda(cell);
    });
    if (!huboCelda) break;

    const fechaTrx = _parseFechaDDMMYYYY(obj['fecha_trx']);
    if (!fechaTrx) break; // nota al pie ("** La información de este reporte...") u otra fila no-dato

    const orderId = _texto(obj['order_id']);
    folios.push({
      referencia:         _texto(obj['referencia']),
      terminalID:         _extraerTerminalID(orderId),
      storeId:            _texto(obj['store_id']),
      sucursal:           _texto(obj['sucursal']),
      nombreEmpresa:      _texto(obj['nombre_empresa']),
      fechaTrx,
      horaTrx:            _texto(obj['hora_de_trx']),
      montoTrx:           _numero(obj['monto_de_trx']),
      comisionBasePct:    _numero(obj['comision_base_pct']),
      comisionBaseMonto:  _numero(obj['comision_base_monto']),
      ivaComision:        keyIvaComision ? _numero(obj[keyIvaComision]) : null,
      comisionMasIva:     _numero(obj['comisiones_iva']),
      montoDeposito:      _numero(obj['monto_deposito']),
      banco:              _texto(obj['banco']),
      tipoTarjeta:         _texto(obj['tipo_de_tarjeta']),
      codigoAutorizacion: _texto(obj['codigo_de_autorizacion']),
      orderId,
    });
  }

  if (folios.length === 0) {
    throw new BadRequestError('La hoja "Ventas Tarjeta Presente" no contiene ninguna fila de transacción bajo "Ventas pagadas durante periodo".');
  }
  return folios;
}

async function parseNetpayReporte(buffer) {
  const workbook = new ExcelJS.Workbook();
  try {
    await workbook.xlsx.load(buffer);
  } catch (err) {
    throw new BadRequestError(`El archivo no es un Excel válido: ${err.message}`);
  }

  const sheetResumen = workbook.getWorksheet('Resumen');
  const sheetVentas  = workbook.getWorksheet('Ventas Tarjeta Presente');
  if (!sheetResumen) throw new BadRequestError('El archivo no contiene la hoja "Resumen" — verifica que sea el reporte real de Netpay.');
  if (!sheetVentas)  throw new BadRequestError('El archivo no contiene la hoja "Ventas Tarjeta Presente" — verifica que sea el reporte real de Netpay.');

  const resumen       = _parseResumen(sheetResumen);
  const resumenVentas = _parseResumenVentas(sheetVentas);
  const folios        = _parseFolios(sheetVentas);

  return {
    claveRastreo:       resumen.claveRastreo,
    cuentaDeposito:     resumen.cuentaDeposito,
    fechaMovimiento:    resumen.fechaMovimiento,
    periodoDesde:       resumen.periodoDesde,
    periodoHasta:       resumen.periodoHasta,
    montoDepositoTotal: resumen.montoDeposito,
    resumenVentas,
    folios,
  };
}

module.exports = {
  parseNetpayReporte,
  // Exportados para tests unitarios de regresión (normalización de headers/fechas y
  // extracción de terminalID) sin tener que armar un workbook completo para cada caso.
  _normalizarHeader,
  _parseFechaDDMMYYYY,
  _parseFechaMesEs,
  _extraerTerminalID,
};
