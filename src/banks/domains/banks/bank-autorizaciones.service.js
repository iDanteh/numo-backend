'use strict';

const ExcelJS      = require('exceljs');
const BankMovement = require('./BankMovement.model');

// ── Normalización de nombre de banco ─────────────────────────────────────────

const BANCO_MAP = {
  bancomer:  'BBVA',
  bbva:      'BBVA',
  banamex:   'Banamex',
  bnamex:    'Banamex',
  santander: 'Santander',
  azteca:    'Azteca',
  banorte:   'Banorte',
  hsbc:      'HSBC',
};

function normalizarBanco(nombre) {
  if (!nombre) return null;
  return BANCO_MAP[String(nombre).trim().toLowerCase()] ?? String(nombre).trim();
}

// ── Normalización de número de autorización ───────────────────────────────────
// Extrae solo dígitos y elimina ceros iniciales.
//   "0354198"  → "354198"
//   "354198"   → "354198"
//   "  048873011 " → "48873011"
// Devuelve null si no queda ningún dígito significativo.

function normalizarAuth(val) {
  if (val == null || val === '') return null;
  const digits = String(val).trim().replace(/\D/g, '');
  if (!digits) return null;
  const n = parseInt(digits, 10);
  return isNaN(n) ? null : String(n);
}

// Devuelve true si el concepto contiene el número de autorización (normalizado).
// Extrae TODOS los bloques numéricos del texto, normaliza cada uno (quita ceros)
// y compara. Evita falsos positivos por substrings dentro de números mayores.
function conceptoContainsAuth(concepto, authNorm) {
  if (!concepto || !authNorm) return false;
  const bloques = concepto.match(/\d+/g);
  if (!bloques) return false;
  return bloques.some(b => normalizarAuth(b) === authNorm);
}

// ── Parser del Excel de autorizaciones ───────────────────────────────────────
// Columnas (1-based):
//   1: Fecha            4: Banco
//   2: Fecha Deposito   5: Solicitante
//   3: Importe          6: Autorizacion (número, puede venir como tipo Number)

async function parseAutorizaciones(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  const ws = wb.worksheets[0];
  if (!ws) throw new Error('El archivo no contiene hojas válidas');

  const rows = [];
  ws.eachRow({ includeEmpty: false }, (row, idx) => {
    if (idx === 1) return; // skip header
    const autRaw   = row.getCell(6).value;
    const impRaw   = row.getCell(3).value;
    const bancoRaw = row.getCell(4).value;

    const autNorm = normalizarAuth(autRaw);
    const importe = impRaw != null ? Number(impRaw) : null;
    const banco   = normalizarBanco(bancoRaw);

    if (!autNorm || importe == null || isNaN(importe)) return;
    rows.push({ autNorm, importe, banco });
  });

  return rows;
}

// ── Match y actualización ─────────────────────────────────────────────────────

const IMPORTE_TOLERANCIA = 1.00;

function importeOk(mov, importe) {
  const movMonto = mov.deposito ?? mov.retiro ?? 0;
  return Math.abs(movMonto - importe) <= IMPORTE_TOLERANCIA;
}

async function matchAutorizaciones(buffer) {
  const rows = await parseAutorizaciones(buffer);
  if (!rows.length) return { total: 0, matcheados: 0, identificados: 0, sinMatch: 0 };

  // Bancos del Excel para acotar la consulta
  const bancosExcel = [...new Set(rows.map(r => r.banco).filter(Boolean))];

  // Traer todos los movimientos activos de esos bancos
  const movimientos = await BankMovement.find({
    isActive: true,
    ...(bancosExcel.length ? { banco: { $in: bancosExcel } } : {}),
  }).select('_id numeroAutorizacion concepto deposito retiro status banco').lean();

  // ── Índice 1: auth normalizado → movimientos ──────────────────────────────
  const byAuthNorm = new Map();
  for (const m of movimientos) {
    const n = normalizarAuth(m.numeroAutorizacion);
    if (!n) continue;
    if (!byAuthNorm.has(n)) byAuthNorm.set(n, []);
    byAuthNorm.get(n).push(m);
  }

  // ── Índice 2: movimientos sin auth explícita, agrupados por banco ─────────
  // Se usarán para búsqueda dentro del concepto.
  const sinAuthPorBanco = new Map();
  for (const m of movimientos) {
    if (normalizarAuth(m.numeroAutorizacion)) continue; // ya en índice 1
    if (!m.concepto) continue;
    const b = m.banco;
    if (!sinAuthPorBanco.has(b)) sinAuthPorBanco.set(b, []);
    sinAuthPorBanco.get(b).push(m);
  }

  // ── Procesar filas del Excel ──────────────────────────────────────────────
  const idsAIdentificar  = new Set();
  const movIdsMatcheados = new Set(); // evita doble conteo si el Excel tiene duplicados
  const noMatcheados     = [];        // filas sin match para devolver al cliente
  let matcheados = 0;

  for (const row of rows) {
    let encontrado = false;

    // Estrategia 1 — match por numeroAutorizacion normalizado
    const candidatos1 = byAuthNorm.get(row.autNorm) ?? [];
    for (const mov of candidatos1) {
      if (!importeOk(mov, row.importe)) continue;
      encontrado = true;
      const id = mov._id.toString();
      if (!movIdsMatcheados.has(id)) { movIdsMatcheados.add(id); matcheados++; }
      if (mov.status !== 'identificado') idsAIdentificar.add(id);
      break;
    }

    if (encontrado) continue;

    // Estrategia 2 — buscar el auth dentro del concepto
    if (row.banco) {
      const candidatos2 = sinAuthPorBanco.get(row.banco) ?? [];
      for (const mov of candidatos2) {
        if (!conceptoContainsAuth(mov.concepto, row.autNorm)) continue;
        if (!importeOk(mov, row.importe)) continue;
        encontrado = true;
        const id = mov._id.toString();
        if (!movIdsMatcheados.has(id)) { movIdsMatcheados.add(id); matcheados++; }
        if (mov.status !== 'identificado') idsAIdentificar.add(id);
        break;
      }
    }

    if (!encontrado) {
      noMatcheados.push({
        autorizacion: row.autNorm,
        importe:      row.importe,
        banco:        row.banco ?? null,
      });
    }
  }

  // ── Actualización en bulk ─────────────────────────────────────────────────
  let identificados = 0;
  if (idsAIdentificar.size > 0) {
    const ahora = new Date();
    const ops = [...idsAIdentificar].map(id => ({
      updateOne: {
        filter: { _id: id },
        update: {
          $set: {
            status: 'identificado',
            identificadoPor: { userId: 'aut-match', nombre: 'Motor Autorizaciones', fechaId: ahora },
          },
        },
      },
    }));
    const result = await BankMovement.bulkWrite(ops, { ordered: false });
    identificados = result.modifiedCount;
  }

  return {
    total:        rows.length,
    matcheados,
    identificados,
    sinMatch:     noMatcheados.length,
    noMatcheados,
  };
}

module.exports = { matchAutorizaciones };
