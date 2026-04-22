'use strict';

const ExcelJS            = require('exceljs');
const BankMovement       = require('./BankMovement.model');
const ErpCuentaPendiente = require('../erp/ErpCuentaPendiente.model');

// ── Series del ERP que contienen autorizaciones de pago ───────────────────────
const SERIES_CON_AUTH = ['CBT', 'ABO', 'CPF', 'CFC'];
const ERP_TOLERANCE   = 1.00; // $1 MXN — misma tolerancia que el resto del sistema

// ── Normalización de número de autorización ───────────────────────────────────
// Extrae el PRIMER bloque numérico y elimina ceros iniciales.
// Usar el primer bloque (no concatenar todos) evita falsos positivos cuando el
// banco guarda tokens multi-número como "04711358/7607235" (BBVA).
//   "AUT 04711358"     → "4711358"
//   "REF 0118169248"   → "118169248"
//   "D INT 7607235"    → "7607235"
//   "04711358/7607235" → "4711358"
function normalizarAuth(val) {
  if (val == null || val === '') return null;
  const match = String(val).trim().match(/(\d+)/);
  if (!match) return null;
  const n = parseInt(match[1], 10);
  return isNaN(n) ? null : String(n);
}

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

// ── Validación de importe ─────────────────────────────────────────────────────
function importeOk(mov, importe) {
  const movMonto = mov.deposito ?? mov.retiro ?? 0;
  return Math.abs(Math.abs(movMonto) - Math.abs(importe)) <= ERP_TOLERANCE;
}

// ── Búsqueda de auth dentro del concepto ─────────────────────────────────────
function conceptoContainsAuth(concepto, authNorm) {
  if (!concepto || !authNorm) return false;
  const bloques = concepto.match(/\d+/g);
  if (!bloques) return false;
  return bloques.some(b => normalizarAuth(b) === authNorm);
}

// ── Búsqueda en índice respetando movimientos ya usados ───────────────────────
// Prefiere candidatos cuyo importe coincida; si ninguno coincide en importe,
// toma el primero disponible (auth exacta es suficiente evidencia).
function findInIndex(index, autNorm, monto, usedMovIds) {
  const candidatos = index.get(autNorm);
  if (!candidatos?.length) return null;
  const preferred = candidatos.find(m => !usedMovIds.has(m._id.toString()) && importeOk(m, monto));
  if (preferred) return preferred;
  return candidatos.find(m => !usedMovIds.has(m._id.toString())) ?? null;
}

// ══════════════════════════════════════════════════════════════════════════════
// MATCH DESDE ERP (fuente principal)
// ──────────────────────────────────────────────────────────────────────────────
// Lee ErpCuentaPendiente directamente; extrae autorizaciones de formasPago en
// movimientos de series CBT/ABO/CPF/CFC y las cruza con bank_movements.
// Por cada match establece erpIds, erpLinks, saldoErp, status e identificadoPor,
// replicando y mejorando la lógica del antiguo POST /erp/match.
//
// Mejoras sobre el endpoint anterior:
//  · Cada formasPago se procesa individualmente → un CxC puede quedar vinculado
//    a múltiples movimientos bancarios (uno por pago parcial).
//  · Búsqueda también en referenciaNumerica (no solo numeroAutorizacion).
//  · Auth es autoritativa: el importe es preferido pero no bloqueante en 1a/1b.
//  · Fase 2 conserva la estrategia inversa por monto para CxCs sin formasPago.
// ══════════════════════════════════════════════════════════════════════════════
async function matchAutorizacionesDesdeErp({ banco } = {}) {
  const bancoNorm = banco ? normalizarBanco(banco) : null;

  // ── 1. Datos del ERP ───────────────────────────────────────────────────────
  const cxcs = await ErpCuentaPendiente.find({})
    .select('erpId total folioFiscal movimientos')
    .lean();

  if (!cxcs.length) {
    return { total: 0, matcheados: 0, identificados: 0, sinMatch: 0, noMatcheados: [] };
  }

  // ── 2. Movimientos bancarios elegibles ─────────────────────────────────────
  // Solo depósitos sin_identificar que aún no tienen vínculos ERP.
  const movimientos = await BankMovement.find({
    isActive: true,
    status:   'no_identificado',
    deposito: { $gt: 0 },
    erpIds:   { $size: 0 },
    ...(bancoNorm ? { banco: bancoNorm } : {}),
  }).select('_id numeroAutorizacion referenciaNumerica concepto deposito erpIds erpLinks banco').lean();

  if (!movimientos.length) {
    return { total: cxcs.length, matcheados: 0, identificados: 0, sinMatch: cxcs.length, noMatcheados: [] };
  }

  // ── 3. Índices de búsqueda ─────────────────────────────────────────────────
  const byAuthNorm          = new Map(); // numeroAutorizacion norm → movs
  const byRefNorm           = new Map(); // referenciaNumerica norm → movs
  const porConceptoPorBanco = new Map(); // banco → movs con concepto
  const porConceptoTodos    = [];        // todos los anteriores (para banco=null)
  const conAuthPorMonto     = new Map(); // centavos → movs CON auth (fase 2)

  for (const m of movimientos) {
    const na = normalizarAuth(m.numeroAutorizacion);
    if (na) {
      if (!byAuthNorm.has(na)) byAuthNorm.set(na, []);
      byAuthNorm.get(na).push(m);

      // Índice fase 2: solo movimientos que tienen auth (evidencia de pago terminal)
      const key = Math.round((m.deposito || 0) * 100);
      if (!conAuthPorMonto.has(key)) conAuthPorMonto.set(key, []);
      conAuthPorMonto.get(key).push(m);
    }
    const nr = normalizarAuth(m.referenciaNumerica);
    if (nr) {
      if (!byRefNorm.has(nr)) byRefNorm.set(nr, []);
      byRefNorm.get(nr).push(m);
    }
    if (m.concepto) {
      const b = m.banco;
      if (!porConceptoPorBanco.has(b)) porConceptoPorBanco.set(b, []);
      porConceptoPorBanco.get(b).push(m);
      porConceptoTodos.push(m);
    }
  }

  // ── 4. Extraer filas del ERP y separar CxCs sin auth ──────────────────────
  // Una fila = un par único (erpId, autNorm). El mismo auth no se procesa dos
  // veces para la misma CxC (dedup) pero sí puede aparecer en distintas CxCs.
  const rowsConAuth = []; // { autNorm, monto, cxc }
  const cxcsSinAuth = []; // CxCs que no tienen ningún auth en formasPago

  const seenPairs = new Set();
  for (const cxc of cxcs) {
    let hasAuth = false;
    for (const mov of (cxc.movimientos || [])) {
      if (!SERIES_CON_AUTH.includes(mov.serie)) continue;
      for (const fp of (mov.formasPago || [])) {
        const autNorm = normalizarAuth(fp.autorizacion);
        if (!autNorm) continue;
        const pairKey = `${cxc.erpId}:${autNorm}`;
        if (seenPairs.has(pairKey)) continue;
        seenPairs.add(pairKey);
        hasAuth = true;
        rowsConAuth.push({ autNorm, monto: Math.abs(fp.monto ?? 0), cxc });
      }
    }
    if (!hasAuth) cxcsSinAuth.push(cxc);
  }

  // ── 5. Motor de match ──────────────────────────────────────────────────────
  const usedMovIds   = new Set();
  const ops          = [];
  const noMatcheados = [];
  let matcheados     = 0;

  // Registra la operación de vinculación para un movimiento bancario.
  // Calcula saldoErp y status igual que bank.service.js > aplicarLogicaErp.
  function pushOp(mov, cxc, montoLink) {
    usedMovIds.add(mov._id.toString());
    matcheados++;
    const link = {
      erpId:       cxc.erpId,
      saldoActual: montoLink ?? cxc.total ?? null,
      folioFiscal: cxc.folioFiscal ?? null,
      total:       cxc.total ?? null,
    };
    const newLinks = [...(mov.erpLinks || []), link];
    const newIds   = [...(mov.erpIds   || []), cxc.erpId];
    // != null cubre saldoActual = 0 (no tratar como falsy)
    const saldoErp = newLinks.reduce(
      (s, l) => s + (l.saldoActual != null ? l.saldoActual : (l.total ?? 0)),
      0,
    );
    const newStatus = Math.abs((mov.deposito ?? 0) - saldoErp) <= ERP_TOLERANCE
      ? 'identificado'
      : 'no_identificado';
    ops.push({
      updateOne: {
        filter: { _id: mov._id, status: 'no_identificado' },
        update: {
          $set: {
            erpIds:   newIds,
            erpLinks: newLinks,
            saldoErp,
            status:   newStatus,
            // 'erp-auto' permite que POST /erp/match/revert los encuentre y revierta.
            // Array con erpId explícito para que updateErpIds pueda limpiar la entrada
            // al desvincular la CxC.
            identificadoPor: [{ userId: 'erp-auto', nombre: 'Motor ERP', fechaId: new Date(), erpId: cxc.erpId }],
          },
        },
      },
    });
  }

  // ── Fase 1: CxCs con auth — estrategias 1a, 1b, 2 ─────────────────────────
  for (const { autNorm, monto, cxc } of rowsConAuth) {
    let foundMov = null;

    // 1a: match exacto por numeroAutorizacion
    foundMov = findInIndex(byAuthNorm, autNorm, monto, usedMovIds);

    // 1b: match exacto por referenciaNumerica
    if (!foundMov) foundMov = findInIndex(byRefNorm, autNorm, monto, usedMovIds);

    // 2: auth dentro del texto del concepto (con check de importe)
    if (!foundMov) {
      const candidatos = bancoNorm
        ? (porConceptoPorBanco.get(bancoNorm) ?? [])
        : porConceptoTodos;
      for (const m of candidatos) {
        if (usedMovIds.has(m._id.toString())) continue;
        if (!conceptoContainsAuth(m.concepto, autNorm)) continue;
        if (monto && !importeOk(m, monto)) continue;
        foundMov = m;
        break;
      }
    }

    if (foundMov) {
      pushOp(foundMov, cxc, monto || null);
    } else {
      noMatcheados.push({ autorizacion: autNorm, importe: monto, banco: bancoNorm, erpId: cxc.erpId });
    }
  }

  // ── Fase 2: CxCs sin auth — estrategia 3 (inversa por monto) ──────────────
  // Para CxCs que no tienen formasPago con autorizacion, buscamos movimientos
  // bancarios que YA TIENEN auth (evidencia de pago con terminal) y cuyo
  // deposito coincida con el total de la CxC. Se corre después de la fase 1
  // para no desplazar movimientos que una CxC con auth podría reclamar.
  for (const cxc of cxcsSinAuth) {
    if (!cxc.total) continue;
    const key        = Math.round(cxc.total * 100);
    const candidatos = conAuthPorMonto.get(key) ?? [];
    let foundMov     = null;
    for (const m of candidatos) {
      if (usedMovIds.has(m._id.toString())) continue;
      if (Math.abs((m.deposito ?? 0) - cxc.total) > ERP_TOLERANCE) continue;
      foundMov = m;
      break;
    }
    if (foundMov) pushOp(foundMov, cxc, cxc.total);
  }

  // ── 6. Escritura en bulk ───────────────────────────────────────────────────
  let identificados = 0;
  if (ops.length > 0) {
    const result = await BankMovement.bulkWrite(ops, { ordered: false });
    identificados = result.modifiedCount;
  }

  return {
    total:        rowsConAuth.length + cxcsSinAuth.length,
    matcheados,
    identificados,
    sinMatch:     noMatcheados.length,
    noMatcheados,
  };
}

// ══════════════════════════════════════════════════════════════════════════════
// MATCH DESDE EXCEL (compatibilidad — solo actualiza status, sin erpLinks)
// ══════════════════════════════════════════════════════════════════════════════

function buscarEnIndice(indice, autNorm, importe) {
  const candidatos = indice.get(autNorm);
  if (!candidatos?.length) return null;
  if (importe) {
    const conImporte = candidatos.find(m => importeOk(m, importe));
    if (conImporte) return conImporte;
  }
  return candidatos[0];
}

async function ejecutarMatch(rows) {
  if (!rows.length) {
    return { total: 0, matcheados: 0, identificados: 0, sinMatch: 0, noMatcheados: [] };
  }

  const bancosRequeridos = [...new Set(rows.map(r => r.banco).filter(Boolean))];
  const movimientos = await BankMovement.find({
    isActive: true,
    ...(bancosRequeridos.length ? { banco: { $in: bancosRequeridos } } : {}),
  }).select('_id numeroAutorizacion referenciaNumerica concepto deposito retiro status banco').lean();

  const byAuthNorm          = new Map();
  const byRefNorm           = new Map();
  const porConceptoPorBanco = new Map();
  const porConceptoTodos    = [];

  for (const m of movimientos) {
    const na = normalizarAuth(m.numeroAutorizacion);
    if (na) {
      if (!byAuthNorm.has(na)) byAuthNorm.set(na, []);
      byAuthNorm.get(na).push(m);
    }
    const nr = normalizarAuth(m.referenciaNumerica);
    if (nr) {
      if (!byRefNorm.has(nr)) byRefNorm.set(nr, []);
      byRefNorm.get(nr).push(m);
    }
    if (m.concepto) {
      const b = m.banco;
      if (!porConceptoPorBanco.has(b)) porConceptoPorBanco.set(b, []);
      porConceptoPorBanco.get(b).push(m);
      porConceptoTodos.push(m);
    }
  }

  const idsAIdentificar  = new Set();
  const movIdsMatcheados = new Set();
  const noMatcheados     = [];
  let matcheados = 0;

  for (const row of rows) {
    let mov = null;

    mov = buscarEnIndice(byAuthNorm, row.autNorm, row.importe);
    if (!mov) mov = buscarEnIndice(byRefNorm, row.autNorm, row.importe);
    if (!mov) {
      const candidatos = row.banco
        ? (porConceptoPorBanco.get(row.banco) ?? [])
        : porConceptoTodos;
      for (const m of candidatos) {
        if (!conceptoContainsAuth(m.concepto, row.autNorm)) continue;
        if (row.importe && !importeOk(m, row.importe)) continue;
        mov = m;
        break;
      }
    }

    if (mov) {
      matcheados++;
      movIdsMatcheados.add(mov._id.toString());
      if (mov.status !== 'identificado') idsAIdentificar.add(mov._id.toString());
    } else {
      noMatcheados.push({ autorizacion: row.autNorm, importe: row.importe ?? null, banco: row.banco ?? null });
    }
  }

  let identificados = 0;
  if (idsAIdentificar.size > 0) {
    const ahora = new Date();
    const ops = [...idsAIdentificar].map(id => ({
      updateOne: {
        filter: { _id: id },
        update: { $set: { status: 'identificado', identificadoPor: [{ userId: 'aut-match', nombre: 'Motor ERP', fechaId: ahora }] } },
      },
    }));
    const result = await BankMovement.bulkWrite(ops, { ordered: false });
    identificados = result.modifiedCount;
  }

  return { total: rows.length, matcheados, identificados, sinMatch: noMatcheados.length, noMatcheados };
}

async function parseAutorizaciones(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  const ws = wb.worksheets[0];
  if (!ws) throw new Error('El archivo no contiene hojas válidas');
  const rows = [];
  ws.eachRow({ includeEmpty: false }, (row, idx) => {
    if (idx === 1) return;
    const autNorm = normalizarAuth(row.getCell(6).value);
    const impRaw  = row.getCell(3).value;
    const banco   = normalizarBanco(row.getCell(4).value);
    const importe = impRaw != null ? Number(impRaw) : null;
    if (!autNorm || importe == null || isNaN(importe)) return;
    rows.push({ autNorm, importe, banco });
  });
  return rows;
}

async function matchAutorizaciones(buffer) {
  return ejecutarMatch(await parseAutorizaciones(buffer));
}

module.exports = { matchAutorizacionesDesdeErp, matchAutorizaciones };
