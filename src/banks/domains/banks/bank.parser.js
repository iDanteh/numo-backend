/**
 * bankParser.js
 * Normaliza estados de cuenta bancarios desde un archivo Excel multi-hoja.
 *
 * Lógica de discriminación:
 *  - BBVA:    ignora movimientos cuyo concepto contenga 'SQ', 'Traspaso entre cuentas propias' u 'openmx'
 *  - Banamex: ignora movimientos cuyo concepto contenga 'Evopaymx'
 *  - Santander/Azteca: sin discriminación
 *
 * No. de Autorización:
 *  - BBVA:      primer token después del '/' en el concepto
 *  - Banamex:   sub-fila "No. de Autorización: XXXXX"
 *  - Santander: columna ID inmediatamente después de Saldo (vals[8])
 *  - Azteca:    columna ID inmediatamente después de Saldo (vals[7])
 */

const ExcelJS = require('exceljs');
const crypto  = require('crypto');

function makeHash(m) {
  const key = [
    m.banco,
    m.fecha instanceof Date ? m.fecha.toISOString() : String(m.fecha),
    m.saldo   ?? '',
    m.deposito ?? '',
    m.retiro   ?? '',
    (m.concepto || '').substring(0, 120),
  ].join('|');
  return crypto.createHash('sha256').update(key).digest('hex').substring(0, 40);
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

// ── Parsers por banco ─────────────────────────────────────────────────────────

/**
 * Banamex
 * Estructura de columnas (ExcelJS 1-based):
 *   vals[1] = Fecha (Date para filas principales, null para sub-filas)
 *   vals[2] = Concepto / texto de sub-fila
 *   vals[3] = Depósitos (número o '-')
 *   vals[4] = Retiros   (número o '-')
 *   vals[5] = Saldo
 *
 * Cada movimiento puede tener sub-filas con:
 *   "Referencia numérica: DEPOS XXXXXXX"
 *   "No. de Autorización: XXXXXXX"
 *   "Concepto del Pago: XXXXXXX"
 */
function parseBanamex(sheet) {
  const movements = [];
  let current = null;
  let headerSkipped = false;

  // ExcelJS ValueType: 0=Null, 1=Merge, 2=Number, 3=String, 4=Date
  // Las filas principales de Banamex tienen tipo Date (4) en col1.
  // Algunos exports devuelven tipo Number (2) con un serial numérico de Excel.
  // Las sub-filas pueden tener tipo Merge (1) O Null (0); ambos casos
  // ocurren dependiendo de cómo Banamex exportó las celdas combinadas.
  const DATE_TYPE   = 4;
  const NUMBER_TYPE = 2;
  const MERGE_TYPE  = 1;
  const NULL_TYPE   = 0;

  sheet.eachRow((row) => {
    const cell1Type = row.getCell(1).type;
    const v = row.values;
    const col2 = v[2]; // Concepto o texto sub-fila
    const col3 = v[3]; // Depósitos
    const col4 = v[4]; // Retiros
    const col5 = v[5]; // Saldo

    // Saltar header
    if (!headerSkipped) {
      headerSkipped = true;
      return;
    }

    // Una fila principal tiene tipo Date (4) en col1.
    // Si ExcelJS devuelve un serial numérico (tipo 2) que sea válido como fecha, también es fila principal.
    // Las sub-filas tienen tipo Merge (1) o Null (0) — ambos deben procesarse.
    const isMainRow = cell1Type === DATE_TYPE ||
      (cell1Type === NUMBER_TYPE && typeof v[1] === 'number' && v[1] > 25000);
    const isSubRow  = (cell1Type === MERGE_TYPE || cell1Type === NULL_TYPE) && current !== null;

    if (isMainRow) {
      // Guardar movimiento anterior si existe
      if (current) {
        movements.push(buildBanamex(current, isOtros(current.conceptoBase, BANAMEX_OTROS)));
      }

      current = {
        fecha:           normalizeExcelDate(v[1]) ?? new Date(),
        conceptoBase:    cellText(col2),
        lineasExtra:     [],
        deposito:        toNumber(col3),
        retiro:          toNumber(col4),
        saldo:           toNumber(col5),
        numAutorizacion: null,
        refNumerica:     null,
      };
    } else if (isSubRow && current) {
      // Sub-fila: acumular información adicional
      const text = cellText(col2);
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

  // Último movimiento
  if (current) {
    movements.push(buildBanamex(current, isOtros(current.conceptoBase, BANAMEX_OTROS)));
  }

  return movements;
}

// Extrae el primer monto con formato monetario de un texto (ej. "DEP EN EFECTIVO 5,000.00")
const MONTO_RE = /(\d{1,3}(?:,\d{3})*\.\d{2})/;

function buildBanamex(c, otros = false) {
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

  const m = {
    banco:              'Banamex',
    fecha:              c.fecha,
    concepto:           conceptoCompleto,
    deposito,
    retiro,
    saldo:              c.saldo,
    numeroAutorizacion: c.numAutorizacion,
    referenciaNumerica: c.refNumerica,
    status:             otros ? 'otros' : 'no_identificado',
    categoria:          clasificar(conceptoCompleto),
  };
  m.hash = makeHash(m);
  return m;
}

/**
 * BBVA
 * Estructura de columnas (ExcelJS 1-based):
 *   vals[1] = Fecha (Date)
 *   vals[2] = Concepto / Referencia
 *   vals[3] = Cargo  (retiro)
 *   vals[4] = Abono  (depósito)
 *   vals[5] = Saldo
 *
 * Autorización: primer token después del '/' en el concepto.
 */
function parseBBVA(sheet) {
  const movements = [];
  let headerSkipped = false;

  sheet.eachRow((row) => {
    const v = row.values;
    const col1 = v[1]; // Fecha
    const col2 = v[2]; // Concepto
    const col3 = v[3]; // Cargo (retiro)
    const col4 = v[4]; // Abono (depósito)
    const col5 = v[5]; // Saldo

    if (!headerSkipped) {
      headerSkipped = true;
      return;
    }

    const fecha = normalizeExcelDate(col1 instanceof Date ? col1 : toDate(col1));
    if (!fecha) return;

    const concepto = cellText(col2);

    // Extraer autorización: primer token después del '/'
    let numeroAutorizacion = null;
    const slashIdx = concepto.indexOf('/');
    if (slashIdx !== -1) {
      const afterSlash = concepto.substring(slashIdx + 1).trim();
      const firstToken = afterSlash.split(/\s+/)[0];
      if (firstToken) numeroAutorizacion = firstToken;
    }

    const cargoRaw = toNumber(col3);
    const mBBVA = {
      banco:              'BBVA',
      fecha,
      concepto,
      deposito:           toNumber(col4),
      // BBVA exporta cargos como negativos; se guarda el valor absoluto
      retiro:             cargoRaw !== null ? Math.abs(cargoRaw) : null,
      saldo:              toNumber(col5),
      numeroAutorizacion,
      referenciaNumerica: null,
      status:             isOtros(concepto, BBVA_OTROS) ? 'otros' : 'no_identificado',
      categoria:          clasificar(concepto),
    };
    mBBVA.hash = makeHash(mBBVA);
    movements.push(mBBVA);
  });

  return movements;
}

/**
 * Santander
 * Estructura de columnas (ExcelJS 1-based):
 *   vals[1]  = Fecha (string "'DDMMYYYY'")
 *   vals[2]  = Hora
 *   vals[3]  = Sucursal
 *   vals[4]  = Concepto principal
 *   vals[5]  = Signo ('+' o '-')
 *   vals[6]  = Monto
 *   vals[7]  = Saldo
 *   vals[8]  = ID Autorización
 *   vals[9]  = Referencia adicional / Banco origen
 *
 * Sin discriminación.
 */
function parseSantander(sheet) {
  const movements = [];
  let headerSkipped = false;

  sheet.eachRow((row) => {
    const v = row.values;
    const col1 = v[1];  // Fecha
    const col4 = v[4];  // Concepto
    const col5 = v[5];  // Signo +/-
    const col6 = v[6];  // Monto
    const col7 = v[7];  // Saldo
    const col8 = v[8];  // ID Autorización
    const col9 = v[9];  // Referencia adicional

    if (!headerSkipped) {
      headerSkipped = true;
      return;
    }

    const fecha = toDate(col1);
    if (!fecha) return;

    const signo      = cellText(col5);
    const monto      = toNumber(col6);
    const concepto1  = cellText(col4);
    const refExtra   = cellText(col9);
    const concepto   = refExtra ? `${concepto1} | ${refExtra}` : concepto1;

    const mSant = {
      banco:              'Santander',
      fecha,
      concepto,
      deposito:           signo === '+' ? monto : null,
      retiro:             signo === '-' ? monto : null,
      saldo:              toNumber(col7),
      numeroAutorizacion: (col8 !== null && col8 !== undefined && String(col8).trim() !== '') ? String(col8).trim() : null,
      referenciaNumerica: null,
      status:             'no_identificado',
      categoria:          clasificar(concepto),
    };
    mSant.hash = makeHash(mSant);
    movements.push(mSant);
  });

  return movements;
}

/**
 * Azteca
 * Estructura de columnas (ExcelJS 1-based):
 *   vals[1]  = Fecha (Date)
 *   vals[2]  = Fecha duplicada
 *   vals[3]  = Concepto
 *   vals[4]  = Depósito (número o null)
 *   vals[5]  = Retiro   (número negativo o null)
 *   vals[6]  = Saldo
 *   vals[7]  = ID Autorización
 *
 * Sin discriminación.
 */
function parseAzteca(sheet) {
  const movements = [];
  let headerSkipped = false;

  sheet.eachRow((row) => {
    const v = row.values;
    const col1 = v[1]; // Fecha
    const col3 = v[3]; // Concepto
    const col4 = v[4]; // Depósito
    const col5 = v[5]; // Retiro (puede ser negativo)
    const col6 = v[6]; // Saldo
    const col7 = v[7]; // ID Autorización

    if (!headerSkipped) {
      headerSkipped = true;
      return;
    }

    const fecha = normalizeExcelDate(col1 instanceof Date ? col1 : toDate(col1));
    if (!fecha) return;

    const depositoRaw = toNumber(col4);
    const retiroRaw   = toNumber(col5);

    const deposito = depositoRaw !== null && depositoRaw > 0 ? depositoRaw : null;
    const retiro   = retiroRaw   !== null && retiroRaw   < 0 ? Math.abs(retiroRaw) : null;

    const conceptoAzt = cellText(col3);
    const mAzt = {
      banco:              'Azteca',
      fecha,
      concepto:           conceptoAzt,
      deposito,
      retiro,
      saldo:              toNumber(col6),
      numeroAutorizacion: col7 !== null && col7 !== undefined ? String(col7).trim() : null,
      referenciaNumerica: null,
      status:             'no_identificado',
      categoria:          clasificar(conceptoAzt),
    };
    mAzt.hash = makeHash(mAzt);
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

  const sheetParsers = {
    Banamex:   parseBanamex,
    BBVA:      parseBBVA,
    Santander: parseSantander,
    Azteca:    parseAzteca,
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
