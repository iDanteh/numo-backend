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
  bancomer:        'BBVA',
  bbva:            'BBVA',
  'bbva bancomer': 'BBVA',
  'bbva mexico':   'BBVA',
  'bbva méxico':   'BBVA',
  banamex:         'Banamex',
  bnamex:          'Banamex',
  citibanamex:     'Banamex',
  citi:            'Banamex',
  santander:       'Santander',
  'banco santander': 'Santander',
  azteca:          'Azteca',
  'banco azteca':  'Azteca',
  banorte:         'Banorte',
  'banco banorte': 'Banorte',
  hsbc:            'HSBC',
  inbursa:         'Inbursa',
  scotiabank:      'Scotiabank',
  banbajio:        'BanBajío',
  'banbajío':      'BanBajío',
};

function normalizarBanco(nombre) {
  if (!nombre) return null;
  const s = String(nombre).trim()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // quitar acentos para comparar
    .toLowerCase();
  // Búsqueda exacta
  if (BANCO_MAP[s]) return BANCO_MAP[s];
  // Búsqueda por subcadena: "banco azteca" → "azteca" está en el mapa
  for (const [key, val] of Object.entries(BANCO_MAP)) {
    if (s.includes(key) || key.includes(s)) return val;
  }
  return String(nombre).trim();
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
// Orden de preferencia:
//   1. no_identificado + importe correcto + banco correcto
//   2. no_identificado + importe correcto (cualquier banco)
//   3. no_identificado (banco correcto)
//   4. no_identificado (cualquier banco)
//   5. cualquier status + importe correcto
//   6. cualquier candidato disponible
// El argumento `banco` es opcional: si se pasa, se prioriza (no se fuerza).
function findInIndex(index, autNorm, monto, usedMovIds, banco) {
  const all = index.get(autNorm);
  if (!all?.length) return null;
  const pool = all.filter(m => !usedMovIds.has(m._id.toString()));
  if (!pool.length) return null;

  const noId   = pool.filter(m => m.status === 'no_identificado');
  const source = noId.length ? noId : pool;

  return (
    source.find(m => banco && m.banco === banco && importeOk(m, monto)) ??
    source.find(m => importeOk(m, monto)) ??
    source.find(m => banco && m.banco === banco) ??
    source[0]
  );
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
    .select('erpId total folioFiscal serie folioExterno movimientos')
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
      erpId:        cxc.erpId,
      saldoActual:  montoLink ?? cxc.total ?? null,
      folioFiscal:  cxc.folioFiscal  ?? null,
      total:        cxc.total        ?? null,
      serie:        cxc.serie        ?? null,
      folioExterno: cxc.folioExterno ?? null,
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

  // ── 7. Filtrar noMatcheados que ya estaban vinculados ──────────────────────
  // Un CxC puede aparecer como "sin match" porque el movimiento ya fue
  // identificado en una corrida anterior (excluido por status/erpIds en paso 2).
  // Verificar en bulk antes de reportarlos como sin match real.
  let trueSinMatch = noMatcheados;
  if (noMatcheados.length > 0) {
    const pendingErpIds = noMatcheados.map(nm => nm.erpId).filter(Boolean);
    const yaVinculados  = pendingErpIds.length
      ? await BankMovement.distinct('erpIds', { isActive: true, erpIds: { $in: pendingErpIds } })
      : [];
    const yaVinculadosSet = new Set(yaVinculados.map(String));
    trueSinMatch = noMatcheados.filter(nm => !yaVinculadosSet.has(String(nm.erpId)));
  }

  return {
    total:        rowsConAuth.length + cxcsSinAuth.length,
    matcheados,
    identificados,
    sinMatch:     trueSinMatch.length,
    noMatcheados: trueSinMatch,
  };
}

// ══════════════════════════════════════════════════════════════════════════════
// MATCH DESDE EXCEL (compatibilidad — solo actualiza status, sin erpLinks)
// ══════════════════════════════════════════════════════════════════════════════

// Igual que findInIndex pero sin argumento usedMovIds separado; recibe el Set directamente.
function buscarEnIndice(indice, autNorm, importe, banco, usedMovIds) {
  return findInIndex(indice, autNorm, importe, usedMovIds, banco);
}

async function ejecutarMatch(rows) {
  if (!rows.length) {
    return { total: 0, matcheados: 0, identificados: 0, yaIdentificados: 0, sinMatch: 0, noMatcheados: [] };
  }

  // ── Carga de movimientos ──────────────────────────────────────────────────
  // Se cargan todos los movimientos activos sin restricción de banco porque un
  // número de autorización puede estar registrado bajo un banco diferente al que
  // indica el Excel (e.g. BBVA en Excel → Banamex en DB). El banco del Excel se
  // usa como criterio de preferencia (no de exclusión) dentro de findInIndex.
  const movimientos = await BankMovement.find({
    isActive: true,
  }).select('_id numeroAutorizacion referenciaNumerica concepto deposito retiro status banco').lean();

  // ── Índices de búsqueda ───────────────────────────────────────────────────
  const byAuthNorm          = new Map();
  const byRefNorm           = new Map();
  const porConceptoPorBanco = new Map();
  const porConceptoTodos    = [];

  // Fase 4: identificados sin auth registrada, buscables por banco+monto y solo monto.
  // Cubre el caso típico de BBVA "DEPOSITO EN EFECTIVO" (sin "/" en concepto →
  // numeroAutorizacion = null en DB) que fue identificado vía ERP o manual.
  const idPorBancoMonto = new Map(); // `${banco}|${centavos}` → [mov]
  const idPorMonto      = new Map(); // `${centavos}`          → [mov]

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
    if (m.status === 'identificado') {
      const centavos = Math.round((m.deposito ?? m.retiro ?? 0) * 100);
      const kbm = `${m.banco ?? ''}|${centavos}`;
      const km  = String(centavos);
      if (!idPorBancoMonto.has(kbm)) idPorBancoMonto.set(kbm, []);
      idPorBancoMonto.get(kbm).push(m);
      if (!idPorMonto.has(km)) idPorMonto.set(km, []);
      idPorMonto.get(km).push(m);
    }
  }

  // ── Motor de match ────────────────────────────────────────────────────────
  const usedMovIds         = new Set(); // evita que dos filas consuman el mismo movimiento
  const idsAIdentificar    = new Set();
  // movId → autNorm: movimientos donde faltaba auth en DB y ahora podemos completarlo
  const idsActualizarAuth  = new Map();
  const noMatcheados       = [];
  const yaIdentificadosArr = [];
  let matcheados = 0;

  for (const row of rows) {
    let mov = null;

    // 1a: por numeroAutorizacion (respeta banco y usedMovIds)
    mov = buscarEnIndice(byAuthNorm, row.autNorm, row.importe, row.banco, usedMovIds);

    // 1b: por referenciaNumerica
    if (!mov) mov = buscarEnIndice(byRefNorm, row.autNorm, row.importe, row.banco, usedMovIds);

    // 2: auth dentro del concepto
    if (!mov) {
      const candidatos = row.banco
        ? (porConceptoPorBanco.get(row.banco) ?? [])
        : porConceptoTodos;
      // Preferir no_identificado primero
      const ordenados = [
        ...candidatos.filter(m => m.status === 'no_identificado'),
        ...candidatos.filter(m => m.status !== 'no_identificado'),
      ];
      for (const m of ordenados) {
        if (usedMovIds.has(m._id.toString())) continue;
        if (!conceptoContainsAuth(m.concepto, row.autNorm)) continue;
        if (row.importe && !importeOk(m, row.importe)) continue;
        mov = m;
        break;
      }
    }

    if (mov) {
      matcheados++;
      usedMovIds.add(mov._id.toString());
      // Si el movimiento no tiene auth registrado, aprovechamos para guardarlo
      if (!mov.numeroAutorizacion && row.autNorm) {
        idsActualizarAuth.set(mov._id.toString(), row.autNorm);
      }
      if (mov.status !== 'identificado') idsAIdentificar.add(mov._id.toString());
    } else {
      // ── Fase 4: fallback por importe + banco entre ya identificados ─────────
      // Ocurre cuando el movimiento fue identificado (ERP/manual) pero su
      // numeroAutorizacion es null en DB (ej. BBVA "DEPOSITO EN EFECTIVO").
      // Se reporta como "ya identificado", no como "sin match".
      let yaIdMov = null;
      if (row.importe) {
        const centavos = Math.round(row.importe * 100);
        // Preferencia: banco + monto; fallback: solo monto
        const candidatosBM = row.banco
          ? (idPorBancoMonto.get(`${row.banco}|${centavos}`) ?? [])
          : [];
        const candidatosM  = idPorMonto.get(String(centavos)) ?? [];
        const pool = candidatosBM.length ? candidatosBM : candidatosM;

        for (const m of pool) {
          if (usedMovIds.has(m._id.toString())) continue;
          if (!importeOk(m, row.importe)) continue;
          yaIdMov = m;
          break;
        }
      }

      if (yaIdMov) {
        usedMovIds.add(yaIdMov._id.toString());
        yaIdentificadosArr.push({ autorizacion: row.autNorm, importe: row.importe ?? null, banco: row.banco ?? null });
        // Completar el auth faltante en DB para que futuras ejecuciones lo encuentren en fase 1
        if (!yaIdMov.numeroAutorizacion && row.autNorm) {
          idsActualizarAuth.set(yaIdMov._id.toString(), row.autNorm);
        }
      } else {
        noMatcheados.push({ autorizacion: row.autNorm, importe: row.importe ?? null, banco: row.banco ?? null });
      }
    }
  }

  // ── Escritura en bulk ─────────────────────────────────────────────────────
  let identificados = 0;
  if (idsAIdentificar.size > 0) {
    const ahora = new Date();
    const ops = [...idsAIdentificar].map(id => {
      const upd = {
        $set: {
          status:          'identificado',
          identificadoPor: [{ userId: 'aut-match', nombre: 'Motor ERP', fechaId: ahora }],
        },
      };
      // Incluir auth si lo tenemos y no estaba en DB — queda vinculado desde esta corrida
      if (idsActualizarAuth.has(id)) {
        upd.$set.numeroAutorizacion = idsActualizarAuth.get(id);
        idsActualizarAuth.delete(id); // evitar doble escritura
      }
      return { updateOne: { filter: { _id: id }, update: upd } };
    });
    const result = await BankMovement.bulkWrite(ops, { ordered: false });
    identificados = result.modifiedCount;
  }

  // Actualizar auth en movimientos ya identificados donde faltaba (fase 4)
  if (idsActualizarAuth.size > 0) {
    const authOps = [...idsActualizarAuth.entries()].map(([id, autNorm]) => ({
      updateOne: {
        filter: { _id: id },
        update: { $set: { numeroAutorizacion: autNorm } },
      },
    }));
    await BankMovement.bulkWrite(authOps, { ordered: false });
  }

  return {
    total:           rows.length,
    matcheados,
    identificados,
    yaIdentificados: yaIdentificadosArr.length,
    sinMatch:        noMatcheados.length,
    noMatcheados,
  };
}

// Normaliza texto de encabezado para comparación: minúsculas sin acentos.
function normHeader(val) {
  return String(val ?? '').trim().toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

// Columnas soportadas y sus alias en el encabezado.
// El primer alias que coincida gana.
const HEADER_ALIASES = {
  monto:          ['monto', 'importe', 'amount'],
  banco:          ['banco', 'bank', 'institucion', 'institución'],
  autorizacion:   ['autorizacion', 'autorización', 'no. autorizacion', 'no. autorización',
                   'num autorizacion', 'num. autorizacion', 'numero autorizacion',
                   'número de autorización', 'auth', 'authorization'],
};

async function parseAutorizaciones(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  const ws = wb.worksheets[0];
  if (!ws) throw new Error('El archivo no contiene hojas válidas');

  // ── Detectar columnas por encabezado ──────────────────────────────────────
  // Fallback a los índices originales si no se encuentran encabezados conocidos.
  let colMonto = 3, colBanco = 4, colAuth = 6;

  const headerRow = ws.getRow(1);
  const found     = {};
  headerRow.eachCell((cell, colNum) => {
    const h = normHeader(cell.value);
    for (const [campo, aliases] of Object.entries(HEADER_ALIASES)) {
      if (!found[campo] && aliases.includes(h)) {
        found[campo] = colNum;
      }
    }
  });

  if (found.monto)        colMonto = found.monto;
  if (found.banco)        colBanco = found.banco;
  if (found.autorizacion) colAuth  = found.autorizacion;

  // ── Parsear filas de datos ────────────────────────────────────────────────
  const rows = [];
  ws.eachRow({ includeEmpty: false }, (row, idx) => {
    if (idx === 1) return; // saltar encabezado
    const autNorm = normalizarAuth(row.getCell(colAuth).value);
    const impRaw  = row.getCell(colMonto).value;
    const banco   = normalizarBanco(row.getCell(colBanco).value);
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