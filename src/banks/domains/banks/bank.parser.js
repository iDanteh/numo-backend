/**
 * bankParser.js
 * Normaliza estados de cuenta bancarios desde un archivo Excel multi-hoja.
 *
 * Estrategia de detección de columnas (por parser):
 *  1. Si la primera fila contiene texto en col1 se intenta detectar columnas
 *     por encabezado (aliases definidos por banco). Si todos los campos
 *     requeridos se encuentran → modo encabezado (colMap).
 *  2. Si la primera fila ya contiene datos (tipo Date/número de fecha) o si
 *     el encabezado no pudo mapearse completamente → modo posicional (índices
 *     fijos, comportamiento original).
 *  3. Fecha ausente en un movimiento: se asigna la fecha de carga del archivo
 *     (uploadDate), siempre que la fila tenga al menos concepto o saldo.
 *     Las filas completamente vacías se descartan.
 *
 * Lógica de discriminación:
 *  - BBVA:    ignora movimientos cuyo concepto contenga 'SQ', 'Traspaso entre cuentas propias' u 'openmx'
 *  - Banamex: ignora movimientos cuyo concepto contenga 'Evopaymx'
 *  - Santander/Azteca: sin discriminación
 *
 * No. de Autorización:
 *  - BBVA:      primer token después del '/' en el concepto
 *  - Banamex:   sub-fila "No. de Autorización: XXXXX"
 *  - Santander: columna ID inmediatamente después de Saldo (vals[8] en posicional)
 *  - Azteca:    columna ID inmediatamente después de Saldo (vals[7] en posicional)
 */

const ExcelJS = require('exceljs');
const crypto  = require('crypto');

// ── Tipos de celda ExcelJS (ValueType) ────────────────────────────────────────
const CELL_TYPE = {
  NULL:   0,
  MERGE:  1,
  NUMBER: 2,
  STRING: 3,
  DATE:   4,
};

// ── Hash de movimiento ────────────────────────────────────────────────────────
function makeHash(m) {
  const key = [
    m.banco,
    m.fecha instanceof Date ? m.fecha.toISOString() : String(m.fecha),
    String(m.saldo   ?? ''),
    String(m.deposito ?? ''),
    String(m.retiro   ?? ''),
    (m.concepto || '').substring(0, 120),
  ].join('|');
  // SHA-256 completo (64 hex). No se trunca: el truncado anterior a 40 chars
  // reducía la resistencia a colisiones de 2^256 a 2^160 innecesariamente.
  // NOTA: movimientos ya importados conservan el hash de 40 chars; re-importar
  // un archivo histórico generará hashes de 64 chars que no colisionarán con
  // los existentes — esto es intencional y preferible a mantener la debilidad.
  return crypto.createHash('sha256').update(key).digest('hex');
}

// ── Patrones que llevan status = 'otros' ─────────────────────────────────────
const BBVA_OTROS = [
  /\bSQ\b/i,
  /traspaso\s+entre\s+cuentas\s+propias/i,
  /openmx/i,
];
const BANAMEX_OTROS = [/evopaymx/i];

function isOtros(text, patterns) {
  if (!text) return false;
  return patterns.some((p) => p.test(text));
}

// ── Clasificador de categorías ────────────────────────────────────────────────
//
// Orden importa: la primera regla que coincide gana.
// Reglas más específicas van antes que las más generales.
//
const CATEGORIAS = [
  // ── Recursos humanos ──────────────────────────────────────────────────────
  { label: 'Nómina',
    re: /\b(n[oó]mina|salario|sueldo)\b/i },

  // ── Movimientos entre cuentas propias ─────────────────────────────────────
  { label: 'Traspaso',
    re: /\btraspaso\b/i },

  // ── Efectivo / depósitos en ventanilla ────────────────────────────────────
  { label: 'Depósitos',
    re: /\b(dep[oó]s(ito)?s?|ventas?)(\s+(de\s+)?(en\s+)?efectivo)?\b/i },

  // ── Instrumentos de pago físicos ──────────────────────────────────────────
  { label: 'Cheque',
    re: /\bcheque\b/i },

  // ── Retiros / disposiciones ───────────────────────────────────────────────
  { label: 'Retiro ATM',
    re: /\b(cajero|atm|retiro|disposici[oó]n)\b/i },

  // ── Cargos del banco (comisiones, notas de cargo) ─────────────────────────
  { label: 'Cargo bancario',
    re: /\b(comisi[oó]n|mantenimiento|anualidad|cargo\s+(mensual|fijo|por)|nota\s+de\s+cargo)\b/i },

  // ── Servicios públicos y telecomunicaciones ───────────────────────────────
  { label: 'Pago de servicio',
    re: /\b(cfe|telmex|telcel|izzi|totalplay|dish|megacable)\b/i },

  // ── Gastos de administración / cuotas de gestión ─────────────────────────
  { label: 'Gasto administrativo',
    re: /\b(administraci[oó]n|iva\s+administraci[oó]n)\b/i },

  // ── Compra o venta de productos (material, equipos, refacciones) ──────────
  // Cubre terminología del ramo hidráulico: material, bombas, tapas,
  // conexiones, biogestores, válvulas (Valvex), etc.
  { label: 'Compra',
    re: /\b(compra|material(es)?(\s+hidr[aá]ulico)?|bomba(s)?|tapa(s)?|valvex|biogestor|conexi[oó]n(es)?|nota\s+de\s+(venta|remisi[oó]n))\b/i },

  // ── Cobros con terminal punto de venta ────────────────────────────────────
  { label: 'Cobro tarjeta',
    re: /\b(tpv|terminal\s+punto|punto\s+de\s+venta|cobro)\b/i },

  // ── Transferencias electrónicas y pagos de facturas ───────────────────────
  // "pago factura(s)" cubre tanto con "de" (ya atrapado por pago\s+de)
  // como sin "de": "PAGO FACTURA 123".
  { label: 'Transferencia',
    re: /\b(spei|transferencia|transf|trfr|pago\s+(a|de|int|factura(s)?)|nota\s+de\s+pago|env[ií]o|abono|bnam|bbvamex|hdnx)\b/i },

  // ── Pagos a terceros / cotizaciones ───────────────────────────────────────
  { label: 'Pago cuenta de tercero',
    re: /\b(pago\s+cuenta\s+de\s+tercero|pago\s+(a|de)\s+(?!int\b)|cotizaci[oó]n)\b/i },
];


function clasificar(concepto) {
  if (!concepto) return null;
  for (const { label, re } of CATEGORIAS) {
    if (re.test(concepto)) return label;
  }
  return null;
}

// ── Normalización de número de autorización ───────────────────────────────────
// Elimina ceros a la izquierda para que "00199480" y "199480" sean equivalentes.
// Se aplica en todos los parsers al asignar numeroAutorizacion.
function normalizeAuthNum(val) {
  if (!val) return null;
  // Strip apóstrofes iniciales (prefijo de texto de Excel: "'000000337041" → "000000337041")
  const s = String(val).trim().replace(/^'+/, '');
  if (!s) return null;
  // Strip ceros iniciales: "00199480" → "199480"
  return s.replace(/^0+(?=\d)/, '') || s;
}

// ── Helpers de conversión ─────────────────────────────────────────────────────

/**
 * Normaliza fechas que ExcelJS entrega como UTC midnight.
 * ExcelJS convierte los seriales de Excel a Date en UTC, por lo que
 * "2026-03-02" llega como 2026-03-02T00:00:00.000Z.  En una zona UTC-N
 * eso se interpreta como el día anterior.  Re-creamos la fecha a las
 * 12:00 UTC del mismo día UTC para que cualquier zona horaria occidental
 * la muestre con la fecha correcta.
 */
function normalizeExcelDate(d) {
  // Celda fórmula (tipo 6): ExcelJS entrega { formula: '=A1', result: Date|number }
  if (d !== null && typeof d === 'object' && !(d instanceof Date) && d.result !== undefined) {
    d = d.result;
  }
  // ExcelJS a veces entrega seriales numéricos en lugar de objetos Date
  if (typeof d === 'number' && d > 25000) {
    d = new Date((d - 25569) * 86400000);
  }
  if (!(d instanceof Date) || isNaN(d.getTime())) return null;
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), 12, 0, 0));
}

/**
 * Intenta extraer un valor numérico de una celda.
 * Retorna null si el valor es nulo, un guión '-', cadena vacía o no parseable.
 */
function toNumber(val) {
  if (val === null || val === undefined || val === '' || val === '-') return null;
  if (typeof val === 'number') return isNaN(val) ? null : val;
  const cleaned = String(val).replace(/,/g, '').trim();
  const n = parseFloat(cleaned);
  return isNaN(n) ? null : n;
}

/**
 * Convierte un valor de celda a Date.
 * Maneja objetos Date, seriales numéricos de Excel, strings tipo "'02032026'" (Santander) y otros formatos.
 */
function toDate(val) {
  if (!val) return null;
  // Celda fórmula (tipo 6): ExcelJS entrega { formula: '=A1', result: Date|string|number }
  if (typeof val === 'object' && !(val instanceof Date) && val.result !== undefined) {
    return toDate(val.result);
  }
  if (val instanceof Date) return isNaN(val.getTime()) ? null : val;

  // ExcelJS puede devolver el serial numérico de Excel en lugar de un objeto Date
  if (typeof val === 'number' && val > 25000) {
    const d = new Date((val - 25569) * 86400000);
    return isNaN(d.getTime()) ? null : d;
  }

  if (typeof val === 'string') {
    // Santander usa strings con comillas como "'02032026'"
    const clean = val.replace(/'/g, '').trim();
    if (/^\d{8}$/.test(clean)) {
      const d = clean.substring(0, 2);
      const m = clean.substring(2, 4);
      const y = clean.substring(4, 8);
      const date = new Date(`${y}-${m}-${d}T00:00:00`);
      return isNaN(date.getTime()) ? null : date;
    }
    const date = new Date(val);
    return isNaN(date.getTime()) ? null : date;
  }

  return null;
}

/**
 * Extrae el texto plano de un valor de celda de ExcelJS.
 * Maneja RichText, strings ordinarios y números.
 */
function cellText(val) {
  if (val === null || val === undefined) return '';
  if (typeof val === 'string') return val.replace(/\u00A0/g, ' ').trim();
  if (typeof val === 'number') return String(val);
  if (typeof val === 'object' && val.richText) {
    return val.richText.map((r) => r.text || '').join('').replace(/\u00A0/g, ' ').trim();
  }
  return String(val).trim();
}

// ── Detección de encabezados ──────────────────────────────────────────────────

/**
 * Determina si la primera fila de un sheet es un encabezado (texto) o
 * directamente un movimiento (fecha / serial numérico de Excel en col1).
 *
 * Retorna true  → es encabezado: intentar detectar columnas por nombre.
 * Retorna false → es dato: usar índices posicionales (no hay encabezado).
 */
function isLikelyHeaderRow(row) {
  const cell1 = row.getCell(1);
  // Tipo Date en col1 → fila de movimiento, no encabezado
  if (cell1.type === CELL_TYPE.DATE) return false;
  // Tipo Number con valor de serial de fecha Excel → fila de movimiento
  if (cell1.type === CELL_TYPE.NUMBER) {
    const v1 = row.values[1];
    if (typeof v1 === 'number' && v1 > 25000) return false;
  }
  // Tipo String: puede ser una fecha textual (ej. Santander "'02032026'").
  // Si el valor se parsea como fecha válida → fila de movimiento, no encabezado.
  if (cell1.type === CELL_TYPE.STRING) {
    if (toDate(row.values[1]) !== null) return false;
  }
  // Texto de encabezado, null, merge → probable encabezado
  return true;
}

/**
 * Detecta el mapeo de columnas semánticas a índices (1-based) desde una fila
 * de encabezado buscando aliases por coincidencia de subcadena (case-insensitive).
 *
 * @param {ExcelJS.Row}              headerRow      - Fila con los encabezados
 * @param {Object<string,string[]>}  aliases        - { campo: ['alias1', 'alias2', ...] }
 * @param {string[]}                 requiredFields - Campos que deben encontrarse para
 *                                                    considerar la detección exitosa
 * @returns {{ colMap: Object<string,number>, allFound: boolean }}
 *   colMap:   { campo → índice 1-based }
 *   allFound: true si todos los campos requeridos fueron mapeados
 */
function detectHeaderColumns(headerRow, aliases, requiredFields) {
  const colMap = {};

  headerRow.eachCell((cell, colIdx) => {
    const text = cellText(cell.value).toLowerCase().trim();
    if (!text) return;
    for (const [field, names] of Object.entries(aliases)) {
      if (colMap[field] !== undefined) continue; // ya mapeado
      if (names.some((name) => text.includes(name))) {
        colMap[field] = colIdx;
      }
    }
  });

  const allFound = requiredFields.every((f) => colMap[f] !== undefined);
  return { colMap, allFound };
}

// ── Aliases de columnas por banco ─────────────────────────────────────────────
//
// Cada banco define los nombres de columna que puede usar en su encabezado.
// Los aliases son subcadenas (match parcial, case-insensitive), de más
// específico a más genérico. El primer alias que coincida en una celda gana.
// Los campos en REQUIRED deben estar todos presentes para activar el modo
// encabezado; si falta alguno se usa el modo posicional como fallback.

const BANAMEX_ALIASES = {
  fecha:    ['fecha', 'date', 'f.operaci', 'f.valor'],
  concepto: ['concepto', 'descripci', 'movimiento', 'detalle'],
  deposito: ['depósito', 'deposito', 'abono', 'crédito', 'credito', 'haber'],
  retiro:   ['retiro', 'cargo', 'débito', 'debito', 'debe', 'egreso'],
  saldo:    ['saldo', 'balance'],
};
const BANAMEX_REQUIRED = ['fecha', 'concepto', 'saldo'];

const BBVA_ALIASES = {
  fecha:    ['fecha', 'f.valor', 'f. valor', 'date', 'f.operaci'],
  concepto: ['concepto', 'descripci', 'referencia', 'movimiento', 'detalle'],
  cargo:    ['cargo', 'retiro', 'débito', 'debito', 'egreso', 'disposici'],
  abono:    ['abono', 'depósito', 'deposito', 'crédito', 'credito', 'ingreso', 'haber'],
  saldo:    ['saldo', 'balance'],
};
const BBVA_REQUIRED = ['fecha', 'concepto', 'saldo'];

const SANTANDER_ALIASES = {
  fecha:        ['fecha', 'date', 'f.operaci', 'f.valor'],
  // 'descripci' captura "Descripcion" (col4); 'concepto' captura "Concepto" (col9) solo si
  // no hay "Descripcion" primero — en ese caso col9 cae en `extra` (ver abajo).
  concepto:     ['concepto', 'descripci', 'movimiento', 'operaci', 'detalle'],
  // El portal de Santander usa "Cargo/Abono" como cabecera del signo +/-
  signo:        ['cargo/abono', 'signo', '+/-', 'd/c', 'd/h', 'tipo mov', 'tipo de mov'],
  monto:        ['monto', 'importe', 'cantidad', 'valor'],
  saldo:        ['saldo', 'balance'],
  // "Referencia" en el estado de cuenta de Santander equivale al número de autorización
  autorizacion: ['referencia', 'autoriza', 'folio', 'id aut', 'num. aut', 'id mov', 'clave'],
  // 'concepto' aquí captura la col "Concepto" (col9) cuando "Descripcion" ya ocupó el
  // campo `concepto` arriba.  'nombre ben' evita capturar "Clabe Beneficiario" (CLABE).
  extra:        ['concepto', 'banco origen', 'banco destino', 'referencia adic', 'nombre ben', 'nombre ord'],
};
const SANTANDER_REQUIRED = ['fecha', 'monto', 'saldo'];

const AZTECA_ALIASES = {
  fecha:        ['fecha', 'date', 'f.operaci', 'f.valor'],
  // 'movimiento' se elimina de concepto para evitar conflicto con la columna
  // "MOVIMIENTO" que el nuevo formato de Azteca usa como número de autorización.
  concepto:     ['concepto', 'descripci', 'detalle'],
  deposito:     ['depósito', 'deposito', 'abono', 'crédito', 'credito', 'ingreso', 'haber'],
  retiro:       ['retiro', 'cargo', 'débito', 'debito', 'egreso', 'debe'],
  // El nuevo formato de Azteca (2026) consolida depósito y retiro en una sola
  // columna "IMPORTE" con signo (+depósito / -retiro).
  importe:      ['importe'],
  saldo:        ['saldo', 'balance'],
  // "MOVIMIENTO" es el ID de operación en el nuevo formato de Azteca.
  autorizacion: ['autoriza', 'folio', 'id aut', 'num. aut', 'referencia', 'clave', 'movimiento'],
};
const AZTECA_REQUIRED = ['fecha', 'concepto', 'saldo'];

// ── Parsers por banco ─────────────────────────────────────────────────────────

/**
 * Banamex
 *
 * Estructura posicional de columnas (ExcelJS 1-based) — hasta 7 columnas:
 *   vals[1] = Fecha (Date para filas principales, null para sub-filas)
 *   vals[2] = Concepto / texto de sub-fila
 *   vals[3] = Depósitos (número o '-')
 *   vals[4] = Retiros   (número o '-')
 *   vals[5] = Saldo
 *   vals[6] = Extra / No. Autorización (solo en estados fin de semana)
 *   vals[7] = Extra (solo en estados fin de semana)
 *
 * Cada movimiento puede tener sub-filas con:
 *   "Referencia numérica: DEPOS XXXXXXX"
 *   "No. de Autorización: XXXXXXX"
 *   "Concepto del Pago: XXXXXXX"
 *
 * Las columnas 6-7 siempre se leen de forma posicional ya que son extras
 * opcionales que no tienen encabezado propio.
 *
 * @param {ExcelJS.Worksheet} sheet
 * @param {Date}              uploadDate  Fecha de carga; fallback para movimientos sin fecha.
 */
function parseBanamex(sheet, uploadDate) {
  const movements = [];
  let current  = null;
  let firstRow = true;
  let colMap   = null; // null = modo posicional

  sheet.eachRow((row) => {
    const cell1Type = row.getCell(1).type;
    const v = row.values;

    // ── Primera fila: determinar modo ──────────────────────────────────────
    if (firstRow) {
      firstRow = false;
      if (isLikelyHeaderRow(row)) {
        const { colMap: detected, allFound } = detectHeaderColumns(
          row, BANAMEX_ALIASES, BANAMEX_REQUIRED,
        );
        colMap = allFound ? detected : null;
        return; // saltar fila de encabezado independientemente del resultado
      }
      // Primera fila es un dato (sin encabezado) → colMap queda null (posicional)
      // No hacer return: continuar procesando esta fila como movimiento principal
    }

    // ── Clasificar tipo de fila ────────────────────────────────────────────
    // Fila principal: col1 tiene tipo Date o serial numérico de fecha
    const isMainRow = cell1Type === CELL_TYPE.DATE ||
      (cell1Type === CELL_TYPE.NUMBER && typeof v[1] === 'number' && v[1] > 25000);
    // Sub-fila: col1 tiene tipo Merge o Null y existe un movimiento en curso.
    // Ambos tipos ocurren dependiendo de cómo Banamex exportó las celdas combinadas.
    const isSubRow = (cell1Type === CELL_TYPE.MERGE || cell1Type === CELL_TYPE.NULL)
      && current !== null;

    if (isMainRow) {
      // Guardar movimiento anterior
      if (current) {
        movements.push(buildBanamex(current, isOtros(current.conceptoBase, BANAMEX_OTROS), uploadDate));
      }

      // Resolver índices según modo
      const iConcepto = colMap ? colMap.concepto : 2;
      const iDeposito = colMap ? colMap.deposito : 3;
      const iRetiro   = colMap ? colMap.retiro   : 4;
      const iSaldo    = colMap ? colMap.saldo    : 5;
      // La fecha siempre se lee del índice detectado o de v[1];
      // normalizeExcelDate maneja Date, serial numérico y fórmulas.
      // Se guarda como null si no se puede determinar: buildBanamex aplicará
      // el fallback uploadDate DESPUÉS de calcular el hash, para que el hash
      // sea idéntico en cualquier reimportación del mismo archivo.
      const fechaRaw  = colMap ? v[colMap.fecha] : v[1];

      current = {
        fecha:           normalizeExcelDate(fechaRaw),   // puede ser null
        conceptoBase:    cellText(v[iConcepto]),
        lineasExtra:     [],
        deposito:        toNumber(v[iDeposito]),
        retiro:          toNumber(v[iRetiro]),
        saldo:           toNumber(v[iSaldo]),
        numAutorizacion: null,
        refNumerica:     null,
      };

      // Columnas 6-7: presentes solo en estados de fin de semana.
      // Se leen siempre de forma posicional porque son columnas adicionales
      // que no aparecen en el encabezado estándar de Banamex.
      for (let colIdx = 6; colIdx <= 7; colIdx++) {
        const text = cellText(v[colIdx]);
        if (!text) continue;

        const authMatch = text.match(/no\.\s*de\s*autorizaci[oó]n[\s:]+(.+)/i);
        if (authMatch) {
          current.numAutorizacion = authMatch[1].trim();
          continue;
        }
        const refMatch = text.match(/referencia\s+num[eé]rica[\s:]+(.+)/i);
        if (refMatch) {
          current.refNumerica = refMatch[1].trim();
        }
        current.lineasExtra.push(text);
      }

    } else if (isSubRow) {
      // Sub-filas de Banamex: el texto siempre cae en la columna de concepto.
      // En modo posicional es col2; en modo encabezado usamos colMap.concepto
      // si fue detectado, con fallback a 2 por si acaso.
      const iConcepto = colMap ? (colMap.concepto ?? 2) : 2;
      const text = cellText(v[iConcepto]);
      if (!text) return;

      const authMatch = text.match(/no\.\s*de\s*autorizaci[oó]n[\s:]+(.+)/i);
      if (authMatch) {
        current.numAutorizacion = authMatch[1].trim();
        return;
      }

      const refMatch = text.match(/referencia\s+num[eé]rica[\s:]+(.+)/i);
      if (refMatch) {
        current.refNumerica = refMatch[1].trim();
        // no hacemos return para que también se agregue a lineasExtra
      }

      current.lineasExtra.push(text);
    }
  });

  // Último movimiento pendiente
  if (current) {
    movements.push(buildBanamex(current, isOtros(current.conceptoBase, BANAMEX_OTROS), uploadDate));
  }

  return movements;
}

// Extrae el primer monto con formato monetario de un texto (ej. "DEP EN EFECTIVO 5,000.00")
const MONTO_RE = /(\d{1,3}(?:,\d{3})*\.\d{2})/;

function buildBanamex(c, otros = false, uploadDate) {
  const extras = c.lineasExtra.filter(Boolean);
  const conceptoCompleto = extras.length
    ? `${c.conceptoBase} | ${extras.join(' | ')}`
    : c.conceptoBase;

  let deposito = c.deposito && c.deposito > 0 ? c.deposito : null;
  let retiro   = c.retiro   && c.retiro   > 0 ? c.retiro   : null;

  // Banamex exporta ciertos depósitos en efectivo con 0 en la columna de monto
  // y el importe real dentro del texto del concepto (ej. "DEP EN EFECTIVO 5,000.00").
  if (deposito === null && retiro === null) {
    const match = c.conceptoBase.match(MONTO_RE);
    if (match) {
      const importe = parseFloat(match[1].replace(/,/g, ''));
      if (/\b(dep|abono|deposito|cheque\s+bnm)\b/i.test(c.conceptoBase)) {
        deposito = importe;
      } else if (/\b(pago|retiro|cargo|cobro|comis)\b/i.test(c.conceptoBase)) {
        retiro = importe;
      }
    }
  }

  // c.fecha es null cuando el archivo no trae fecha en ese movimiento.
  // El hash se calcula con fecha = null para que sea idéntico en cualquier
  // reimportación. Después se asigna la fecha real con el fallback de uploadDate.
  const m = {
    banco:              'Banamex',
    fecha:              c.fecha,           // null si no vino en el archivo
    concepto:           conceptoCompleto,
    deposito,
    retiro,
    saldo:              c.saldo,
    numeroAutorizacion: normalizeAuthNum(c.numAutorizacion),
    referenciaNumerica: normalizeAuthNum(c.refNumerica),
    status:             otros ? 'otros' : 'no_identificado',
    categoria:          clasificar(conceptoCompleto),
  };
  m.hash  = makeHash(m);
  m.fecha = c.fecha ?? uploadDate;
  return m;
}

/**
 * BBVA
 *
 * Estructura posicional de columnas (ExcelJS 1-based):
 *   vals[1] = Fecha (Date)
 *   vals[2] = Concepto / Referencia
 *   vals[3] = Cargo  (retiro, exportado como negativo)
 *   vals[4] = Abono  (depósito)
 *   vals[5] = Saldo
 *
 * Autorización: primer token numérico después del '/' en el concepto.
 *
 * @param {ExcelJS.Worksheet} sheet
 * @param {Date}              uploadDate  Fecha de carga; fallback para movimientos sin fecha.
 */
function parseBBVA(sheet, uploadDate) {
  const movements = [];
  let firstRow = true;
  let colMap   = null; // null = modo posicional

  sheet.eachRow((row) => {
    const v = row.values;

    // ── Primera fila: determinar modo ──────────────────────────────────────
    if (firstRow) {
      firstRow = false;
      if (isLikelyHeaderRow(row)) {
        const { colMap: detected, allFound } = detectHeaderColumns(
          row, BBVA_ALIASES, BBVA_REQUIRED,
        );
        colMap = allFound ? detected : null;
        return; // saltar fila de encabezado independientemente del resultado
      }
      // Primera fila es un dato (sin encabezado) → colMap queda null (posicional)
      // No hacer return: continuar procesando esta fila como movimiento
    }

    // Resolver columnas
    const col1 = v[colMap ? colMap.fecha    : 1]; // Fecha
    const col2 = v[colMap ? colMap.concepto : 2]; // Concepto
    const col3 = v[colMap ? colMap.cargo    : 3]; // Cargo (retiro)
    const col4 = v[colMap ? colMap.abono    : 4]; // Abono (depósito)
    const col5 = v[colMap ? colMap.saldo    : 5]; // Saldo

    const concepto  = cellText(col2);
    const fechaDate = normalizeExcelDate(col1 instanceof Date ? col1 : toDate(col1));

    // Descartar filas completamente vacías (sin fecha ni concepto ni saldo)
    if (!fechaDate && !concepto && toNumber(col5) === null) return;

    // Extraer autorización: primer bloque numérico después del '/'
    // Se toma solo la parte numérica inicial del token para evitar que
    // valores como "04711358/7607235" (sin espacio) se almacenen como
    // un único token compuesto que nunca coincide al normalizar.
    let numeroAutorizacion = null;
    const slashIdx = concepto.indexOf('/');
    if (slashIdx !== -1) {
      const afterSlash = concepto.substring(slashIdx + 1).trim();
      const firstToken = afterSlash.split(/\s+/)[0];
      if (firstToken) {
        const numMatch = firstToken.match(/^(\d+)/);
        numeroAutorizacion = numMatch ? numMatch[1] : firstToken;
      }
    }

    const cargoRaw = toNumber(col3);
    // El hash se calcula con fechaDate (null si el archivo no trae fecha).
    // Esto garantiza que el hash sea idéntico en cualquier reimportación del
    // mismo archivo, independientemente de cuándo se ejecute.
    // Después del hash se asigna la fecha real con el fallback de uploadDate.
    const mBBVA = {
      banco:              'BBVA',
      fecha:              fechaDate,
      concepto,
      deposito:           toNumber(col4),
      // BBVA exporta cargos como negativos; se guarda el valor absoluto
      retiro:             cargoRaw !== null ? Math.abs(cargoRaw) : null,
      saldo:              toNumber(col5),
      numeroAutorizacion: normalizeAuthNum(numeroAutorizacion),
      referenciaNumerica: null,
      status:             isOtros(concepto, BBVA_OTROS) ? 'otros' : 'no_identificado',
      categoria:          clasificar(concepto),
    };
    mBBVA.hash  = makeHash(mBBVA);
    mBBVA.fecha = fechaDate ?? uploadDate;
    movements.push(mBBVA);
  });

  return movements;
}

/**
 * Santander
 *
 * Estructura posicional de columnas (ExcelJS 1-based):
 *   vals[1]  = Fecha (string "'DDMMYYYY'" con apóstrofe de Excel)
 *   vals[2]  = Hora
 *   vals[3]  = Sucursal
 *   vals[4]  = Concepto principal
 *   vals[5]  = Signo ('+' o '-')
 *   vals[6]  = Monto
 *   vals[7]  = Saldo
 *   vals[8]  = ID Autorización
 *   vals[9]  = Referencia adicional / Banco origen
 *
 * Nota: en modo encabezado, hora y sucursal se ignoran (no se mapean a ningún
 * campo del movimiento). El concepto final se construye concatenando concepto
 * y referencia extra si esta existe.
 *
 * Sin discriminación.
 *
 * @param {ExcelJS.Worksheet} sheet
 * @param {Date}              uploadDate  Fecha de carga; fallback para movimientos sin fecha.
 */
function parseSantander(sheet, uploadDate) {
  const movements = [];
  let firstRow = true;
  let colMap   = null; // null = modo posicional

  sheet.eachRow((row) => {
    const v = row.values;

    // ── Primera fila: determinar modo ──────────────────────────────────────
    if (firstRow) {
      firstRow = false;
      if (isLikelyHeaderRow(row)) {
        const { colMap: detected, allFound } = detectHeaderColumns(
          row, SANTANDER_ALIASES, SANTANDER_REQUIRED,
        );
        colMap = allFound ? detected : null;
        return; // saltar fila de encabezado independientemente del resultado
      }
      // Primera fila es un dato (sin encabezado) → colMap queda null (posicional)
      // No hacer return: continuar procesando esta fila como movimiento
    }

    // Resolver columnas
    // Hora (col2) y Sucursal (col3) no se usan en el movimiento; se omiten.
    const col1 = v[colMap ? colMap.fecha        : 1]; // Fecha
    const col4 = v[colMap ? colMap.concepto     : 4]; // Concepto principal
    const col5 = v[colMap ? colMap.signo        : 5]; // Signo +/-
    const col6 = v[colMap ? colMap.monto        : 6]; // Monto
    const col7 = v[colMap ? colMap.saldo        : 7]; // Saldo
    const col8 = v[colMap ? colMap.autorizacion : 8]; // ID Autorización
    const col9 = v[colMap ? colMap.extra        : 9]; // Referencia adicional

    const concepto1 = cellText(col4);
    const fechaDate = toDate(col1);

    // Descartar filas completamente vacías (sin fecha ni concepto ni saldo)
    if (!fechaDate && !concepto1 && toNumber(col7) === null) return;

    const signo    = cellText(col5);
    const monto    = toNumber(col6);
    const refExtra = cellText(col9);
    const concepto = refExtra ? `${concepto1} | ${refExtra}` : concepto1;

    // Hash con fechaDate (null si el archivo no trae fecha) → estable entre reimportaciones.
    // La fecha almacenada se asigna después del hash con el fallback de uploadDate.
    const mSant = {
      banco:              'Santander',
      fecha:              fechaDate,
      concepto,
      deposito:           signo === '+' ? monto : null,
      retiro:             signo === '-' ? monto : null,
      saldo:              toNumber(col7),
      numeroAutorizacion: normalizeAuthNum(col8 !== null && col8 !== undefined ? String(col8).trim() : null),
      referenciaNumerica: null,
      status:             'no_identificado',
      categoria:          clasificar(concepto),
    };
    mSant.hash  = makeHash(mSant);
    mSant.fecha = fechaDate ?? uploadDate;
    movements.push(mSant);
  });

  return movements;
}

/**
 * Azteca
 *
 * Estructura posicional de columnas (ExcelJS 1-based):
 *   vals[1]  = Fecha (Date)
 *   vals[2]  = Fecha duplicada (ignorada en modo encabezado)
 *   vals[3]  = Concepto
 *   vals[4]  = Depósito (número o null)
 *   vals[5]  = Retiro   (número negativo o null)
 *   vals[6]  = Saldo
 *   vals[7]  = ID Autorización
 *
 * Los archivos descargados del portal de Azteca pueden comenzar directamente
 * con movimientos (sin fila de encabezado). La lógica de isLikelyHeaderRow
 * cubre ambos casos.
 *
 * Sin discriminación.
 *
 * @param {ExcelJS.Worksheet} sheet
 * @param {Date}              uploadDate  Fecha de carga; fallback para movimientos sin fecha.
 */
function parseAzteca(sheet, uploadDate) {
  const movements = [];
  let firstRow = true;
  let colMap   = null; // null = modo posicional

  sheet.eachRow((row) => {
    const v = row.values;

    // ── Primera fila: determinar modo ──────────────────────────────────────
    if (firstRow) {
      firstRow = false;
      if (isLikelyHeaderRow(row)) {
        const { colMap: detected, allFound } = detectHeaderColumns(
          row, AZTECA_ALIASES, AZTECA_REQUIRED,
        );
        colMap = allFound ? detected : null;
        return; // saltar fila de encabezado independientemente del resultado
      }
      // Primera fila es un dato (sin encabezado) → colMap queda null (posicional)
      // No hacer return: continuar procesando esta fila como movimiento
    }

    // Resolver columnas
    // col2 (fecha duplicada) solo existe en modo posicional; en modo encabezado
    // si el banco la incluye se ignorará porque no hay alias mapeado para ella.
    const col1 = v[colMap ? colMap.fecha        : 1]; // Fecha
    const col3 = v[colMap ? colMap.concepto     : 3]; // Concepto
    const col6 = v[colMap ? colMap.saldo        : 6]; // Saldo
    const col7 = v[colMap ? colMap.autorizacion : 7]; // ID Autorización

    const conceptoText = cellText(col3);
    const fechaDate    = normalizeExcelDate(col1 instanceof Date ? col1 : toDate(col1));

    // Descartar filas completamente vacías (sin fecha ni concepto ni saldo)
    if (!fechaDate && !conceptoText && toNumber(col6) === null) return;

    // El nuevo formato de Azteca (2026) usa una sola columna "IMPORTE" con signo
    // (positivo = depósito, negativo = retiro) en lugar de dos columnas separadas.
    // Si colMap.importe está disponible se usa ese campo; de lo contrario se
    // mantiene la lógica con columnas deposito/retiro (formato anterior o modo
    // posicional como fallback).
    let depositoRaw, retiroRaw;
    if (colMap && colMap.importe !== undefined) {
      const importeVal = toNumber(v[colMap.importe]);
      depositoRaw = importeVal !== null && importeVal > 0 ? importeVal : null;
      retiroRaw   = importeVal !== null && importeVal < 0 ? importeVal : null;
    } else {
      depositoRaw = toNumber(v[colMap ? colMap.deposito : 4]);
      retiroRaw   = toNumber(v[colMap ? colMap.retiro   : 5]);
    }

    const deposito = depositoRaw !== null && depositoRaw > 0 ? depositoRaw        : null;
    const retiro   = retiroRaw   !== null && retiroRaw   < 0 ? Math.abs(retiroRaw) : null;

    // Hash con fechaDate (null si el archivo no trae fecha) → estable entre reimportaciones.
    // La fecha almacenada se asigna después del hash con el fallback de uploadDate.
    const mAzt = {
      banco:              'Azteca',
      fecha:              fechaDate,
      concepto:           conceptoText,
      deposito,
      retiro,
      saldo:              toNumber(col6),
      numeroAutorizacion: normalizeAuthNum(col7 !== null && col7 !== undefined ? String(col7).trim() : null),
      referenciaNumerica: null,
      status:             'no_identificado',
      categoria:          clasificar(conceptoText),
    };
    mAzt.hash  = makeHash(mAzt);
    mAzt.fecha = fechaDate ?? uploadDate;
    movements.push(mAzt);
  });

  return movements;
}

// ── Función principal ─────────────────────────────────────────────────────────

/**
 * Carga el buffer del archivo Excel y retorna los movimientos normalizados.
 *
 * @param {Buffer} buffer  - Contenido del archivo Excel
 * @param {string} [banco] - Banco explícito ('BBVA','Banamex','Santander','Azteca').
 *                           Cuando se indica, se aplica ese parser a la PRIMERA hoja
 *                           del archivo sin importar su nombre (útil para archivos
 *                           individuales descargados del portal de cada banco).
 *                           Sin este parámetro, se detecta por nombre de hoja
 *                           (modo archivo consolidado multi-hoja).
 */
async function parseBankFile(buffer, banco) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(buffer);

  // Fecha de carga del archivo. Se usa como valor de fecha cuando un movimiento
  // no trae fecha en el Excel (campo vacío o no reconocible).
  // Se fija una sola vez para que todos los movimientos del mismo archivo
  // compartan el mismo valor de fallback.
  const uploadDate = new Date();

  const sheetParsers = {
    Banamex:   (sheet) => parseBanamex(sheet, uploadDate),
    BBVA:      (sheet) => parseBBVA(sheet, uploadDate),
    Santander: (sheet) => parseSantander(sheet, uploadDate),
    Azteca:    (sheet) => parseAzteca(sheet, uploadDate),
  };

  const allMovements = [];
  const summary      = {};
  const errors       = [];

  if (banco) {
    // ── Modo individual: banco especificado explícitamente ──────────────────
    const parser = sheetParsers[banco];
    if (!parser) {
      errors.push({ hoja: '-', error: `Banco no reconocido: ${banco}` });
      return { movements: allMovements, summary, errors };
    }

    const sheet = workbook.worksheets[0];
    if (!sheet) {
      errors.push({ hoja: '-', error: 'El archivo no contiene hojas' });
      return { movements: allMovements, summary, errors };
    }

    try {
      const movements = parser(sheet);
      allMovements.push(...movements);
      summary[banco] = movements.length;

      if (movements.length === 0) {
        errors.push({
          hoja:  sheet.name,
          error: `El parser de ${banco} no encontró movimientos en la hoja "${sheet.name}". ` +
                 `Verifica que el archivo proviene del portal de ${banco} y no ha sido modificado. ` +
                 `Filas en la hoja: ${sheet.rowCount}.`,
        });
      }
    } catch (err) {
      errors.push({ hoja: sheet.name, error: err.message });
    }

  } else {
    // ── Modo auto-detect: primero busca por nombre de hoja ──────────────────
    const keyByName = {
      banamex:   'Banamex',
      bbva:      'BBVA',
      santander: 'Santander',
      azteca:    'Azteca',
    };

    const sheetsDetectadas = [];

    workbook.eachSheet((sheet) => {
      const nameLower = sheet.name.toLowerCase().trim();
      const key = Object.keys(keyByName).find((k) => nameLower.includes(k));
      if (!key) return;

      sheetsDetectadas.push(sheet.name);
      const bancoLabel = keyByName[key];
      try {
        const movements = sheetParsers[bancoLabel](sheet);
        allMovements.push(...movements);
        summary[bancoLabel] = (summary[bancoLabel] || 0) + movements.length;
      } catch (err) {
        errors.push({ hoja: sheet.name, error: err.message });
      }
    });

    // ── Fallback: si ninguna hoja matcheó por nombre, probar cada parser ────
    if (sheetsDetectadas.length === 0 && workbook.worksheets.length > 0) {
      const sheetNames = workbook.worksheets.map((s) => `"${s.name}"`).join(', ');
      const sheet      = workbook.worksheets[0];

      let bestMovements = [];
      let bestBanco     = null;

      for (const [bancoLabel, parser] of Object.entries(sheetParsers)) {
        try {
          const result = parser(sheet);
          if (result.length > bestMovements.length) {
            bestMovements = result;
            bestBanco     = bancoLabel;
          }
        } catch (_) { /* ignorar fallo de parser */ }
      }

      if (bestMovements.length > 0) {
        allMovements.push(...bestMovements);
        summary[bestBanco] = bestMovements.length;
      } else {
        errors.push({
          hoja:  sheet.name,
          error: `Auto-detección sin resultado. Hojas encontradas: ${sheetNames}. ` +
                 `Ningún parser reconoció el formato. Selecciona el banco manualmente ` +
                 `en el botón de importación.`,
        });
      }
    }
  }

  return { movements: allMovements, summary, errors };
}

module.exports = { parseBankFile, CATEGORIAS, clasificar };
