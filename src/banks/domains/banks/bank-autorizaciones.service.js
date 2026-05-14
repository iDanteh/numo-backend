'use strict';

const ExcelJS            = require('exceljs');
const mongoose           = require('mongoose');
const BankMovement       = require('./BankMovement.model');
const ErpCuentaPendiente = require('../erp/ErpCuentaPendiente.model');
const { sincronizarCuentasPendientes } = require('../erp/erp-sync.service');

// ── Series del ERP que contienen autorizaciones de pago ───────────────────────
const SERIES_CON_AUTH = ['CBT', 'ABO', 'CPF', 'CFC'];
const ERP_TOLERANCE   = 1.00; // $1 MXN — misma tolerancia que el resto del sistema

// Horas máximas sin actualizar el caché ERP antes de emitir aviso de frescura.
const ERP_CACHE_MAX_AGE_HOURS = Number(process.env.ERP_CACHE_MAX_AGE_HOURS ?? 24);

// Ventana de fecha para match ERP: el depósito bancario debe estar dentro de
// ±N días de la fechaRealPago / fechaAfectacion de la CxC.
// Se usa solo como criterio de preferencia, no de exclusión total.
const DATE_MATCH_WINDOW_MS = Number(process.env.ERP_DATE_WINDOW_DAYS ?? 30) * 24 * 60 * 60 * 1000;

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

// Extrae todos los bloques numéricos normalizados a partir del SEGUNDO bloque.
// Cubre el caso BBVA donde numeroAutorizacion = "04711358/7607235" pero el ERP
// registró "7607235" (segundo bloque) como autorizacion en formasPago.
// Se usa para construir un índice alternativo (byAuthNormAlt) de movimientos.
function normalizarAuthBloques(val) {
  if (val == null || val === '') return [];
  const bloques = String(val).trim().match(/\d+/g);
  if (!bloques || bloques.length < 2) return [];
  const primero = normalizarAuth(val);
  return [...new Set(
    bloques.slice(1).map(b => {
      const n = parseInt(b, 10);
      return isNaN(n) ? null : String(n);
    }).filter(b => b !== null && b !== primero),
  )];
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
// Parámetros opcionales:
//   banco  — banco preferido (no excluyente)
//   fecha  — fecha de referencia de la CxC para filtro de proximidad temporal
//   strict — true: no retorna candidato sin validar importe cuando monto > 0
//            false (default): comportamiento anterior — útil para match Excel
//
// Orden de preferencia (cuando strict=true y hay fecha):
//   1. dentro de ventana de fecha + importe correcto + banco correcto
//   2. dentro de ventana de fecha + importe correcto
//   3. importe correcto (fuera de ventana, pero amount sí matchea)
//   4. dentro de ventana de fecha + banco correcto
//   5. (solo si monto=0 o strict=false) cualquier candidato disponible
function findInIndex(index, autNorm, monto, usedMovIds, banco, fecha, strict = false) {
  const all = index.get(autNorm);
  if (!all?.length) return null;
  const pool = all.filter(m => !usedMovIds.has(m._id.toString()));
  if (!pool.length) return null;

  const noId   = pool.filter(m => m.status === 'no_identificado');
  const source = noId.length ? noId : pool;

  // Retorna true si el movimiento cae dentro de la ventana temporal de la CxC.
  // Cuando no hay fecha de referencia o el movimiento no tiene fecha, siempre pasa.
  const enVentana = (m) => {
    if (!fecha || !m.fecha) return true;
    return Math.abs(new Date(m.fecha).getTime() - new Date(fecha).getTime()) <= DATE_MATCH_WINDOW_MS;
  };

  return (
    source.find(m => enVentana(m) && banco && m.banco === banco && importeOk(m, monto)) ??
    source.find(m => enVentana(m) && importeOk(m, monto)) ??
    source.find(m => importeOk(m, monto)) ??                          // importe ok sin restricción de fecha
    source.find(m => enVentana(m) && banco && m.banco === banco) ??   // banco ok dentro de ventana
    (strict && monto > 0 ? null : source.find(m => enVentana(m))) ?? // fallback sin amount solo si !strict
    (strict && monto > 0 ? null : source[0])                         // último recurso solo si !strict
  );
}

// ── Bulk write con transacción (con fallback a standalone) ────────────────────
// Detecta el tipo de topología ANTES de intentar startSession() para evitar
// que la sesión quede bufferizada y provoque un timeout de 10s en standalone.
// En producción con replica set se usa transacción para ACID completo.
async function ejecutarBulkConTransaccion(ops) {
  // Detectar topología sin abrir sesión: si es 'Single' (standalone) o desconocida
  // → ir directo al bulkWrite sin transacción.
  const topologyType = mongoose.connection.client?.topology?.description?.type;
  const esReplicaSet = topologyType === 'ReplicaSetWithPrimary'
    || topologyType === 'ReplicaSetNoPrimary'
    || topologyType === 'Sharded';

  if (!esReplicaSet) {
    return BankMovement.bulkWrite(ops, { ordered: false });
  }

  let session = null;
  try {
    session = await mongoose.connection.startSession();
    session.startTransaction();
    const result = await BankMovement.bulkWrite(ops, { ordered: false, session });
    await session.commitTransaction();
    return result;
  } catch (err) {
    if (session?.inTransaction?.()) {
      try { await session.abortTransaction(); } catch (_) { /* ignorar */ }
    }
    // Fallback por si la detección de topología no fue suficiente
    const sinSoporte = err.code === 20
      || /transaction numbers are only allowed/i.test(err.message)
      || /replica/i.test(err.message);
    if (sinSoporte) {
      return BankMovement.bulkWrite(ops, { ordered: false });
    }
    throw err;
  } finally {
    if (session) {
      try { await session.endSession(); } catch (_) { /* ignorar */ }
    }
  }
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
async function matchAutorizacionesDesdeErp({ banco, fechaDesde } = {}, { onProgress } = {}) {
  const bancoNorm = banco ? normalizarBanco(banco) : null;

  onProgress?.({ phase: 'sync-cache', pct: 5, msg: 'Verificando caché ERP...' });
  // ── 0. Auto-sync del caché ERP ─────────────────────────────────────────────
  // Si el caché supera ERP_CACHE_MAX_AGE_HOURS o está vacío, descarga los datos
  // frescos del ERP antes de matchear. Esto garantiza que todas las CxC del
  // período estén disponibles y evita matches parciales por caché incompleto.
  // Si el ERP no está configurado o falla, continúa con el caché existente
  // (el cacheWarning alertará al usuario al final).
  if (process.env.ERP_CAJA_BASE_URL) {
    const newest = await ErpCuentaPendiente
      .findOne({}).sort({ lastSeenAt: -1 }).select('lastSeenAt').lean();
    const ageMs = newest?.lastSeenAt
      ? Date.now() - new Date(newest.lastSeenAt).getTime()
      : Infinity;
    if (ageMs > ERP_CACHE_MAX_AGE_HOURS * 3600 * 1000) {
      await sincronizarCuentasPendientes(fechaDesde ? { fechaDesde } : {}).catch(() => {});
    }
  }

  onProgress?.({ phase: 'loading-cxc', pct: 10, msg: 'Cargando cuentas por cobrar del ERP...' });
  // ── 1. Datos del ERP ───────────────────────────────────────────────────────
  // fechaDesde (opcional): filtra CxCs cuya fechaAfectacion o fechaRealPago sea
  // mayor o igual a la fecha indicada, evitando procesar histórico antiguo.
  const cxcFilter = {};
  if (fechaDesde) {
    const desde = new Date(fechaDesde);
    cxcFilter.$or = [
      { fechaAfectacion: { $gte: desde } },
      { fechaRealPago:   { $gte: desde } },
    ];
  }

  const cxcs = await ErpCuentaPendiente.find(cxcFilter)
    .select('erpId total folioFiscal serie folioExterno movimientos lastSeenAt fechaRealPago fechaAfectacion')
    .lean();

  if (!cxcs.length) {
    return { total: 0, matcheados: 0, identificados: 0, sinMatch: 0, noMatcheados: [], cacheWarning: null };
  }

  // ── Verificación de frescura del caché ERP ─────────────────────────────────
  // Si el registro más reciente supera ERP_CACHE_MAX_AGE_HOURS, se incluye un
  // aviso en la respuesta para que el usuario refresque el caché antes de
  // confiar plenamente en los resultados.
  let cacheWarning = null;
  const maxLastSeenMs = cxcs.reduce((max, c) => {
    const t = c.lastSeenAt ? new Date(c.lastSeenAt).getTime() : 0;
    return t > max ? t : max;
  }, 0);
  if (maxLastSeenMs > 0) {
    const cacheAgeHours = (Date.now() - maxLastSeenMs) / (1000 * 60 * 60);
    if (cacheAgeHours > ERP_CACHE_MAX_AGE_HOURS) {
      cacheWarning = `El caché del ERP tiene ${Math.round(cacheAgeHours)}h sin actualizarse. `
        + `Ejecuta GET /erp/cuentas-pendientes para refrescar antes de correr el match.`;
    }
  }

  onProgress?.({ phase: 'loading-mov', pct: 25, msg: `${cxcs.length} CxC del ERP · cargando movimientos bancarios...` });
  // ── 2. Movimientos bancarios elegibles ─────────────────────────────────────
  // Dos casos:
  //   a) Movimiento limpio: nunca tocado por ERP (erpIds vacío).
  //   b) Movimiento parcial: vinculado en corrida anterior por erp-auto pero
  //      status sigue 'no_identificado' porque el saldoErp no cubrió el depósito
  //      (caché incompleto). Puede recibir CxC adicionales en esta corrida.
  // Nota: $size:0 no puede usar índices. Se reemplaza por $eq:[] (mismo resultado,
  // permite que MongoDB use el índice { isActive:1, status:1, deposito:1 }).
  const movimientos = await BankMovement.find({
    isActive: true,
    status:   'no_identificado',
    deposito: { $gt: 0 },
    $or: [
      { erpIds: { $eq: [] } },
      { 'identificadoPor.userId': 'erp-auto' },
    ],
    ...(bancoNorm ? { banco: bancoNorm } : {}),
  }).select('_id numeroAutorizacion referenciaNumerica concepto deposito erpIds erpLinks banco fecha').lean();

  if (!movimientos.length) {
    return { total: cxcs.length, matcheados: 0, identificados: 0, sinMatch: cxcs.length, noMatcheados: [], cacheWarning };
  }

  onProgress?.({ phase: 'indexing', pct: 40, msg: `${movimientos.length} movimientos · construyendo índices de búsqueda...` });
  // ── 3. Índices de búsqueda ─────────────────────────────────────────────────
  const byAuthNorm          = new Map(); // numeroAutorizacion (primer bloque) → movs
  const byAuthNormAlt       = new Map(); // numeroAutorizacion (bloques 2..N) → movs  ← 1c
  const byRefNorm           = new Map(); // referenciaNumerica norm → movs
  const porConceptoPorBanco = new Map(); // banco → movs con concepto
  const porConceptoTodos    = [];        // todos los anteriores (para banco=null)

  for (const m of movimientos) {
    const na = normalizarAuth(m.numeroAutorizacion);
    if (na) {
      if (!byAuthNorm.has(na)) byAuthNorm.set(na, []);
      byAuthNorm.get(na).push(m);
    }
    // Índice alternativo con bloques secundarios del token del banco.
    // Cubre "04711358/7607235" → también indexado como "7607235".
    for (const altBlock of normalizarAuthBloques(m.numeroAutorizacion)) {
      if (!byAuthNormAlt.has(altBlock)) byAuthNormAlt.set(altBlock, []);
      byAuthNormAlt.get(altBlock).push(m);
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

  // ── 4. Extraer filas del ERP — solo CxCs con auth en formasPago ──────────
  // Una fila = un par único (erpId, autNorm). El mismo auth no se procesa dos
  // veces para la misma CxC (dedup) pero sí puede aparecer en distintas CxCs.
  // CxCs sin ningún auth en formasPago se ignoran (Fase 1B eliminada).
  const rowsConAuth = []; // { autNorm, movTotal, cxc }

  const seenPairs = new Set();
  for (const cxc of cxcs) {
    for (const mov of (cxc.movimientos || [])) {
      if (!SERIES_CON_AUTH.includes(mov.serie)) continue;
      for (const fp of (mov.formasPago || [])) {
        const autNorm = normalizarAuth(fp.autorizacion);
        if (!autNorm) continue;
        const pairKey = `${cxc.erpId}:${autNorm}`;
        if (seenPairs.has(pairKey)) continue;
        seenPairs.add(pairKey);
        // movTotal: usamos fp.monto (importe específico de esta formasPago) cuando está
        // disponible, porque es más preciso que mov.total en CxC con pagos mixtos.
        // Fallback a mov.total y luego a 0 si ninguno está presente.
        rowsConAuth.push({ autNorm, movTotal: Math.abs(fp.monto ?? mov.total ?? 0), cxc });
      }
    }
  }

  // ── 5. Motor de match ──────────────────────────────────────────────────────
  const usedMovIds   = new Set();
  const ops          = [];
  const noMatcheados = [];
  let matcheados     = 0;
  let identificados  = 0;

  // Registra la operación de vinculación para un movimiento bancario.
  // Recibe un grupo de CxC que comparten el mismo número de autorización y
  // deben quedar vinculadas al mismo depósito bancario (relación N:1).
  // saldoActual de cada link = movTotal (total del ABO/CBT/CPF/CFC de esa CxC).
  // saldoErp = suma de todos los movTotals → se compara contra deposito.
  // El filtro del bulkWrite permite tanto movimientos limpios como parciales
  // (erp-auto previo) y protege contra race conditions con otros usuarios.
  function pushGroupOp(mov, grupo) {
    // Filtrar CxCs que ya estaban vinculadas en una corrida anterior.
    // Permite completar un match parcial sin duplicar links existentes.
    const existingIds = new Set(mov.erpIds || []);
    const grupoNuevo  = grupo.filter(({ cxc }) => !existingIds.has(cxc.erpId));

    if (!grupoNuevo.length) return; // todas las CxC del grupo ya vinculadas

    usedMovIds.add(mov._id.toString());
    matcheados += grupoNuevo.length;

    // Preservar links existentes y añadir solo los nuevos
    const newLinks = [...(mov.erpLinks || [])];
    const newIds   = [...(mov.erpIds   || [])];

    for (const { movTotal, cxc } of grupoNuevo) {
      newLinks.push({
        erpId:        cxc.erpId,
        saldoActual:  movTotal,          // total del ABO/CBT/CPF/CFC que aporta esta CxC
        folioFiscal:  cxc.folioFiscal  ?? null,
        total:        cxc.total        ?? null,
        serie:        cxc.serie        ?? null,
        folioExterno: cxc.folioExterno ?? null,
      });
      newIds.push(cxc.erpId);
    }

    // != null cubre saldoActual = 0 (no tratar como falsy)
    const saldoErp = newLinks.reduce(
      (s, l) => s + (l.saldoActual != null ? l.saldoActual : (l.total ?? 0)),
      0,
    );
    const newStatus = Math.abs((mov.deposito ?? 0) - saldoErp) <= ERP_TOLERANCE
      ? 'identificado'
      : 'no_identificado';

    if (newStatus === 'identificado') identificados++;

    ops.push({
      updateOne: {
        // Acepta tanto movimientos limpios (sin vínculos) como los parcialmente
        // vinculados por erp-auto en corridas anteriores.
        // Excluye movimientos identificados por usuarios humanos (protección ACID).
        filter: {
          _id:    mov._id,
          status: 'no_identificado',
          $or: [
            { erpIds: { $size: 0 } },
            { 'identificadoPor.userId': 'erp-auto' },
          ],
        },
        update: {
          $set: {
            erpIds:   newIds,
            erpLinks: newLinks,
            saldoErp,
            status:   newStatus,
            identificadoPor: [{ userId: 'erp-auto', nombre: 'Motor ERP', fechaId: new Date() }],
          },
        },
      },
    });
  }

  onProgress?.({ phase: 'matching', pct: 55, msg: `Cruzando ${rowsConAuth.length} autorizaciones ERP (fase 1)...` });
  // ── Fase 1: CxCs con auth — agrupar por número de autorización (N CxC → 1 movimiento)
  // Todas las CxC que comparten el mismo autNorm deben vincularse al mismo depósito.
  // El importe de validación es la suma de movTotals del grupo.
  const groupsByAuth = new Map(); // autNorm → [{ movTotal, cxc }]
  for (const { autNorm, movTotal, cxc } of rowsConAuth) {
    if (!groupsByAuth.has(autNorm)) groupsByAuth.set(autNorm, []);
    groupsByAuth.get(autNorm).push({ movTotal, cxc });
  }

  for (const [autNorm, grupo] of groupsByAuth) {
    // Suma de los importes de cada formasPago del grupo (N CxC vinculadas al mismo auth)
    const grupoTotal = grupo.reduce((s, r) => s + r.movTotal, 0);

    // Fecha representativa del grupo: la más temprana entre fechaRealPago y fechaAfectacion.
    // Se usa para priorizar movimientos bancarios temporalmente cercanos al pago del ERP.
    const grupoFecha = grupo.reduce((earliest, { cxc }) => {
      const d = cxc.fechaRealPago ?? cxc.fechaAfectacion ?? null;
      if (!d) return earliest;
      if (!earliest) return d;
      return new Date(d) < new Date(earliest) ? d : earliest;
    }, null);

    let foundMov = null;

    // 1a: match exacto por numeroAutorizacion (primer bloque) — con fecha y modo estricto
    foundMov = findInIndex(byAuthNorm, autNorm, grupoTotal, usedMovIds, undefined, grupoFecha, true);

    // 1b: match exacto por referenciaNumerica — con fecha y modo estricto
    if (!foundMov) foundMov = findInIndex(byRefNorm, autNorm, grupoTotal, usedMovIds, undefined, grupoFecha, true);

    // 1c: fallback con bloques secundarios del token (ej. BBVA "xxx/yyy") — con fecha y modo estricto
    if (!foundMov) foundMov = findInIndex(byAuthNormAlt, autNorm, grupoTotal, usedMovIds, undefined, grupoFecha, true);

    // 2: auth dentro del texto del concepto — exige importe correcto (modo estricto en línea)
    if (!foundMov) {
      const candidatos = bancoNorm
        ? (porConceptoPorBanco.get(bancoNorm) ?? [])
        : porConceptoTodos;
      for (const m of candidatos) {
        if (usedMovIds.has(m._id.toString())) continue;
        if (!conceptoContainsAuth(m.concepto, autNorm)) continue;
        if (grupoTotal && !importeOk(m, grupoTotal)) continue; // importe siempre requerido aquí
        foundMov = m;
        break;
      }
    }

    if (foundMov) {
      pushGroupOp(foundMov, grupo);
    } else {
      for (const { movTotal, cxc } of grupo) {
        noMatcheados.push({ autorizacion: autNorm, importe: movTotal, banco: bancoNorm, erpId: cxc.erpId });
      }
    }
  }

  onProgress?.({ phase: 'writing', pct: 85, msg: `Guardando ${ops.length} asociación(es) en la base de datos...` });
  // ── 6. Escritura en bulk (con transacción si el entorno lo soporta) ─────────
  if (ops.length > 0) {
    await ejecutarBulkConTransaccion(ops);
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
    total:        rowsConAuth.length,
    matcheados,
    identificados,
    sinMatch:     trueSinMatch.length,
    noMatcheados: trueSinMatch,
    cacheWarning,
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