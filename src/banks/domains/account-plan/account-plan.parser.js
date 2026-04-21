/**
 * account-plan.parser.js
 *
 * Importa un catálogo de cuentas desde un archivo Excel con estructura SAT.
 *
 * Columnas esperadas (en cualquier orden, detección por encabezado):
 *   Codigo   → código de la cuenta (10 dígitos, ceros de relleno al final)
 *   Nombre   → descripción de la cuenta
 *   CtaMayor → en el catálogo SAT este campo contiene un número de nivel (2, 3, 4…),
 *              NO el código del padre; se ignora para construir la jerarquía.
 *
 * En la importación se clasifican automáticamente:
 *   tipo y naturaleza → inferidos del primer dígito del código
 *   nivel             → calculado de los dígitos significativos del código
 *   ctaMayor/parentId → inferidos del propio código (eliminando los 2 últimos dígitos sig.)
 *
 * Lógica de jerarquía (catálogo SAT de 10 dígitos):
 *   Código sig. 1 dígito  → raíz (sin padre)
 *   Código sig. 2 dígitos → padre tiene 1 dígito sig.
 *   Código sig. N dígitos → padre tiene N-2 dígitos sig.
 *   Ejemplo: 1101010000 → sig "110101" → padre sig "1101" → "1101000000"
 */

const ExcelJS  = require('exceljs');
const repo     = require('./repositories/account-plan.repository');

// Alias de encabezado aceptados por columna
const HEADER_ALIASES = {
  codigo:   ['codigo', 'código', 'clave', 'cuenta', 'num_cuenta', 'num cuenta', 'no_cuenta'],
  nombre:   ['nombre', 'name', 'descripcion', 'descripción', 'desc'],
  ctaMayor: ['ctamayor', 'cta_mayor', 'cta mayor', 'mayor', 'cuenta_mayor', 'agrupador'],
};

// Posiciones por defecto (A=Codigo, B=Nombre, C=CtaMayor)
const DEFAULT_COL_MAP = { codigo: 1, nombre: 2, ctaMayor: 3 };

/** Normaliza texto para comparación: minúsculas, sin acentos, sin espacios extra */
function normalize(str) {
  return String(str || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim();
}

function toStr(val) {
  if (val === null || val === undefined) return '';
  if (typeof val === 'object' && val.richText) {
    return val.richText.map(r => r.text).join('').trim();
  }
  return String(val).trim();
}

function inferTipoNat(codigo) {
  const map = {
    '1': { tipo: 'ACTIVO',  naturaleza: 'DEUDORA'   },
    '2': { tipo: 'PASIVO',  naturaleza: 'ACREEDORA'  },
    '3': { tipo: 'CAPITAL', naturaleza: 'ACREEDORA'  },
    '4': { tipo: 'INGRESO', naturaleza: 'ACREEDORA'  },
    '5': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
    '6': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
    '7': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
    '8': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
    '9': { tipo: 'GASTO',   naturaleza: 'DEUDORA'    },
  };
  return map[String(codigo).trim()[0]] || null;
}

/**
 * Nivel jerárquico para el formato SAT de 10 dígitos con ceros de relleno.
 *   sig.length 1 → nivel 1
 *   sig.length 2 → nivel 2
 *   sig.length 4 → nivel 3  (sig.length / 2 + 1)
 *   sig.length 6 → nivel 4
 *   ...
 */
function codigoToNivel(codigo) {
  const sig = codigo.replace(/0+$/, '') || codigo[0] || '0';
  return sig.length === 1 ? 1 : Math.floor(sig.length / 2) + 1;
}

/**
 * Infiere el código del padre a partir del código de la cuenta.
 * Elimina los 2 últimos dígitos significativos y rellena con ceros.
 * Retorna null si la cuenta es raíz (1 dígito significativo).
 */
function inferParentCodigo(codigo) {
  const sig = codigo.replace(/0+$/, '') || '';
  if (sig.length <= 1) return null;                          // raíz
  const parentSig = sig.length === 2 ? sig[0] : sig.slice(0, sig.length - 2);
  return parentSig.padEnd(codigo.length, '0');
}

/**
 * @param {Buffer} buffer  Buffer del archivo .xlsx
 * @param {object} opts    Opciones: { sheetIndex?, hasHeader? }
 */
async function parseAccountPlanFile(buffer, opts = {}) {
  const sheetIdx  = opts.sheetIndex || 0;
  const hasHeader = opts.hasHeader !== false;

  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(buffer);

  const sheet = workbook.worksheets[sheetIdx];
  if (!sheet) throw new Error('No se encontró la hoja de trabajo en el archivo');

  // ── Detectar columnas por encabezado ─────────────────────────────────────
  let colMap = { ...DEFAULT_COL_MAP };

  if (hasHeader) {
    const headerRow = sheet.getRow(1);
    headerRow.eachCell((cell, colNumber) => {
      const header = normalize(cell.value);
      for (const [field, aliases] of Object.entries(HEADER_ALIASES)) {
        if (aliases.includes(header) && colMap[field] === DEFAULT_COL_MAP[field]) {
          colMap[field] = colNumber;
          break;
        }
      }
    });
  }

  // ── Leer filas ────────────────────────────────────────────────────────────
  const rawRows = [];
  const startRow = hasHeader ? 2 : 1;

  sheet.eachRow((row, rowNumber) => {
    if (rowNumber < startRow) return;

    const v = row.values; // 1-indexed

    const codigoRaw = toStr(v[colMap.codigo]);
    const nombreRaw = toStr(v[colMap.nombre]);
    // El valor de la columna CtaMayor del SAT es un indicador de nivel (2,3,4…),
    // NO el código del padre → se ignora para construir la jerarquía.

    if (!codigoRaw || !nombreRaw) return;

    const inferred = inferTipoNat(codigoRaw);
    if (!inferred) return; // primer dígito no reconocido

    // El padre se infiere del propio código, no de la columna CtaMayor
    const parentCodigo = inferParentCodigo(codigoRaw);

    rawRows.push({
      codigo:       codigoRaw,
      nombre:       nombreRaw,
      ctaMayor:     parentCodigo,   // código real del padre (null si es raíz)
      tipo:         inferred.tipo,
      naturaleza:   inferred.naturaleza,
      nivel:        codigoToNivel(codigoRaw),
      parentCodigo,                 // alias para la segunda pasada
    });
  });

  // ── Ordenar: padres antes que hijos (por longitud de código) ─────────────
  rawRows.sort((a, b) => a.nivel - b.nivel || a.codigo.localeCompare(b.codigo));

  // ── Primera pasada: upsert de cuentas sin parentId ────────────────────────
  let importados   = 0;
  let actualizados = 0;
  let omitidos     = 0;
  const errores    = [];
  const codigoToId = new Map();   // código → PG integer id

  for (const row of rawRows) {
    try {
      const { isNew, record } = await repo.upsertByCodigo({
        codigo:     row.codigo,
        nombre:     row.nombre,
        ctaMayor:   row.ctaMayor,
        tipo:       row.tipo,
        naturaleza: row.naturaleza,
        nivel:      row.nivel,
        isActive:   true,
      });

      codigoToId.set(row.codigo, record.id);
      if (isNew) importados++;
      else       actualizados++;
    } catch (err) {
      errores.push(`${row.codigo}: ${err.message}`);
      omitidos++;
    }
  }

  // ── Segunda pasada: asignar parentId desde código inferido ───────────────
  for (const row of rawRows) {
    if (!row.parentCodigo) continue;

    const parentId = codigoToId.get(row.parentCodigo);
    if (!parentId) continue;   // padre no importado (catálogo parcial), se omite silenciosamente

    try {
      await repo.updateParentId(row.codigo, parentId);
    } catch (err) {
      errores.push(`Parentesco ${row.codigo} → ${row.parentCodigo}: ${err.message}`);
    }
  }

  return { importados, actualizados, omitidos, errores, total: rawRows.length };
}

module.exports = { parseAccountPlanFile };
