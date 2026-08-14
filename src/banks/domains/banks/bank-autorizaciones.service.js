'use strict';

const ExcelJS            = require('exceljs');
const mongoose           = require('mongoose');
const BankMovement       = require('./BankMovement.model');
const ErpCuentaPendiente = require('../erp/ErpCuentaPendiente.model');
const {
  SERIES_CON_AUTH,
  normalizarAuth,
  normalizarAuthBloques,
} = require('../erp/erp-auth.utils');
const { resolvePrimeraIdentificacion } = require('./identificacion-timestamp.util');

const ERP_TOLERANCE   = 1.00; // $1 MXN — misma tolerancia que el resto del sistema

// Ventana de fecha para match ERP: el depósito bancario debe estar dentro de
// ±N días de la fechaRealPago / fechaAfectacion de la CxC.
// Se usa solo como criterio de preferencia, no de exclusión total.
const DATE_MATCH_WINDOW_MS = Number(process.env.ERP_DATE_WINDOW_DAYS ?? 30) * 24 * 60 * 60 * 1000;

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
  afirme:          'Afirme',
  intercam:        'Intercam',
  nu:              'Nu',
  spin:            'Spin',
  'hey banco':     'Hey Banco',
  heybanco:        'Hey Banco',
  albo:            'Albo',
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

// ── Fuzzy match de autorización (Levenshtein) ────────────────────────────────
// Calcula la distancia de edición mínima entre dos strings (inserciones,
// borrados y sustituciones). Implementación iterativa O(m·n) tiempo, O(n) espacio.
// Para strings de autorización típicos (6–12 dígitos) el costo es despreciable.
function levenshtein(a, b) {
  if (a === b) return 0;
  if (!a.length) return b.length;
  if (!b.length) return a.length;
  const prev = Array.from({ length: b.length + 1 }, (_, i) => i);
  for (let i = 0; i < a.length; i++) {
    const curr = [i + 1];
    for (let j = 0; j < b.length; j++) {
      curr[j + 1] = a[i] === b[j]
        ? prev[j]
        : 1 + Math.min(prev[j], prev[j + 1], curr[j]);
    }
    prev.splice(0, prev.length, ...curr);
  }
  return prev[b.length];
}

// Distancia máxima permitida según la longitud de la autorización más larga.
// ≤9 dígitos → 1 (ej. "1864626" vs "18646261"); ≥10 dígitos → 2.
// Esto equivale aproximadamente al "95% de coincidencia" para estos rangos.
function fuzzyAuthMaxDist(maxLen) {
  return maxLen >= 10 ? 2 : 1;
}

// ── Búsqueda de auth dentro del concepto ─────────────────────────────────────
function conceptoContainsAuth(concepto, authNorm) {
  if (!concepto || !authNorm) return false;
  const bloques = concepto.match(/\d+/g);
  if (!bloques) return false;
  return bloques.some(b => normalizarAuth(b) === authNorm);
}

// IDs de usuario que corresponden a motores automáticos (nunca a humanos).
// Cualquier userId fuera de esta lista en identificadoPor indica intervención humana.
const MOTOR_USERIDS = ['erp-auto', 'aut-match'];

// Extrae todos los bloques numéricos de ≥5 dígitos del concepto de un movimiento.
// Se usa como fallback cuando numeroAutorizacion y referenciaNumerica son null,
// cubriendo el caso en que el parser del estado de cuenta no separó el campo.
// Mínimo 5 dígitos: evita cantidades, años (2024) y otros números cortos.
function extraerTokensConcepto(concepto) {
  if (!concepto) return [];
  const bloques = String(concepto).match(/\d{5,}/g);
  if (!bloques) return [];
  return [...new Set(
    bloques.map(b => { const n = parseInt(b, 10); return isNaN(n) ? null : String(n); })
           .filter(Boolean),
  )];
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
// MATCH DESDE ERP — flujo directo por autorización explícita
// ──────────────────────────────────────────────────────────────────────────────
// Opera exclusivamente sobre los datos en erp_cuentas_pendientes (sin sync).
// Solo vincula CxC que tengan una autorización registrada en formasPago que
// coincida exactamente con el numeroAutorizacion o referenciaNumerica del
// movimiento bancario. Sin tokens de concepto, sin coincidencias por texto libre.
//
// Flujo:
//  1. Cargar movimientos bancarios elegibles (no_identificado + deposito > 0).
//  2. Extraer authsConcretas: solo campos estructurados (numeroAutorizacion,
//     referenciaNumerica, bloques BBVA "xxx/yyy"). ≤ ~500 elementos.
//  3. Query CxC: { _autsNorm: { $in: [...authsConcretas] } } — usa índice.
//  4. Indexar movimientos en Maps para O(1) lookup por auth.
//  5. Motor de match (tres rutas: auth, referencia, bloque alt).
//     setImmediate cada 500 iter — event loop no bloqueado.
//  6. Escritura bulk con transacción ACID en replica set.
//
// Características:
//  · La query CxC escala O(log N) gracias al índice _autsNorm.
//  · CxC sin autorización en formasPago son ignoradas completamente.
//  · Cada formasPago se procesa individualmente → una CxC puede vincularse
//    a múltiples movimientos bancarios (pagos parciales).
//  · Modo estricto en rutas 1a/1b/1c: importe requerido cuando grupoTotal > 0.
// ══════════════════════════════════════════════════════════════════════════════
async function matchAutorizacionesDesdeErp({ banco, fechaDesde } = {}, { onProgress } = {}) {
  const bancoNorm = banco ? normalizarBanco(banco) : null;

  onProgress?.({ phase: 'loading-mov', pct: 5, msg: 'Cargando movimientos bancarios...' });
  // ── 1. Movimientos bancarios elegibles ─────────────────────────────────────
  // Criterios de inclusión:
  //   · status 'no_identificado' + deposito > 0 — solo depósitos pendientes.
  //   · Sin intervención humana: ninguna entrada en identificadoPor tiene userId
  //     fuera de los motores conocidos. $not+$elemMatch es la única forma MongoDB
  //     de garantizar "ningún elemento del array cumple la condición".
  //     Esto excluye movimientos que un usuario tocó aunque status siga pendiente.
  const movimientos = await BankMovement.find({
    isActive: true,
    status:   'no_identificado',
    deposito: { $gt: 0 },
    // Protección: ningún humano ha intervenido en este movimiento
    identificadoPor: { $not: { $elemMatch: { userId: { $nin: [...MOTOR_USERIDS, null] } } } },
    ...(bancoNorm ? { banco: bancoNorm } : {}),
  }).select('_id numeroAutorizacion referenciaNumerica concepto deposito erpIds erpLinks banco fecha status primeraIdentificacionAt primeraIdentificacionPor').lean();

  if (!movimientos.length) {
    return { total: 0, matcheados: 0, identificados: 0, sinMatch: 0, noMatcheados: [] };
  }

  // ── Pre-computar _idStr y fechaMs — elimina conversiones repetidas en el hot path ──
  // ObjectId.toString() y new Date() son costosos cuando se llaman N veces por cada
  // candidato dentro del loop de matching. Se calculan una sola vez aquí.
  for (const m of movimientos) {
    m._idStr  = m._id.toString();
    m.fechaMs = m.fecha ? new Date(m.fecha).getTime() : null;
  }

  // ── 2. Construir authsConcretas — SOLO campos estructurados ──────────────────
  // Incluye únicamente valores de numeroAutorizacion, referenciaNumerica y bloques
  // secundarios (ej. BBVA "xxx/yyy"). Garantiza un $in ≤ ~500 elementos → el índice
  // _autsNorm opera en O(log N). Agregar tokens del concepto aquí inflaría el $in a
  // miles de elementos y degradaría la query a CollScan — se manejan en Fase B lazy.
  const authsConcretas = new Set();
  for (const m of movimientos) {
    const na = normalizarAuth(m.numeroAutorizacion);
    if (na) authsConcretas.add(na);
    for (const alt of normalizarAuthBloques(m.numeroAutorizacion)) authsConcretas.add(alt);
    const nr = normalizarAuth(m.referenciaNumerica);
    if (nr) authsConcretas.add(nr);
  }

  // ── Helper interno: construye el filtro $match para queries al ERP ──────────
  const buildCxcMatchFilter = (authSet) => {
    const f = { _autsNorm: { $in: [...authSet] } };
    if (fechaDesde) {
      const desde = new Date(fechaDesde);
      f.$or = [{ fechaAfectacion: { $gte: desde } }, { fechaRealPago: { $gte: desde } }];
    }
    return f;
  };

  // Proyección compartida entre fases — recorta movimientos a SERIES_CON_AUTH,
  // reduce datos transferidos desde MongoDB a Node.js.
  const stageProyeccion = {
    $project: {
      erpId: 1, total: 1, folioFiscal: 1, serie: 1, folioExterno: 1, tipoPago: 1,
      fechaRealPago: 1, fechaAfectacion: 1,
      movimientos: {
        $filter: {
          input: '$movimientos',
          as:    'mov',
          cond:  { $in: ['$$mov.serie', SERIES_CON_AUTH] },
        },
      },
      // true si la CxC tiene al menos un movimiento de retención fiscal (serie RET)
      tieneRetencion: {
        $gt: [
          {
            $size: {
              $filter: {
                input: '$movimientos',
                as:    'ret',
                cond:  { $eq: ['$$ret.serie', 'RET'] },
              },
            },
          },
          0,
        ],
      },
    },
  };

  onProgress?.({ phase: 'loading-cxc', pct: 20, msg: `${movimientos.length} movimientos · cargando CxC (${authsConcretas.size} auths explícitas)...` });
  // ── 3. Query ERP — $in pequeño, usa índice _autsNorm ────────────────────────
  // El match opera exclusivamente sobre los datos almacenados en erp_cuentas_pendientes.
  // Solo se cargan CxC que tengan al menos una auth en _autsNorm que coincida.
  const cxcsA = authsConcretas.size > 0
    ? await ErpCuentaPendiente.aggregate([{ $match: buildCxcMatchFilter(authsConcretas) }, stageProyeccion])
    : [];

  onProgress?.({ phase: 'indexing', pct: 40, msg: `${cxcsA.length} CxC / ${movimientos.length} movimientos · construyendo índices de búsqueda...` });
  // ── Índices de búsqueda — O(1) lookup por auth ───────────────────────────────
  const byAuthNorm    = new Map(); // numeroAutorizacion (primer bloque) → movs
  const byAuthNormAlt = new Map(); // numeroAutorizacion (bloques 2..N) → movs  ← 1c
  const byRefNorm     = new Map(); // referenciaNumerica norm → movs

  for (const m of movimientos) {
    const na = normalizarAuth(m.numeroAutorizacion);
    if (na) {
      if (!byAuthNorm.has(na)) byAuthNorm.set(na, []);
      byAuthNorm.get(na).push(m);
    }
    // Índice alternativo con bloques secundarios — cubre "04711358/7607235" → "7607235"
    for (const altBlock of normalizarAuthBloques(m.numeroAutorizacion)) {
      if (!byAuthNormAlt.has(altBlock)) byAuthNormAlt.set(altBlock, []);
      byAuthNormAlt.get(altBlock).push(m);
    }
    const nr = normalizarAuth(m.referenciaNumerica);
    if (nr) {
      if (!byRefNorm.has(nr)) byRefNorm.set(nr, []);
      byRefNorm.get(nr).push(m);
    }
    // Los tokens de concepto se indexan en Fase B (lazy) — no aquí.
    // Indexarlos todos en Fase A haría crecer byAuthNorm innecesariamente.
  }

  // ── Estado del motor ─────────────────────────────────────────────────────────
  const usedMovIds   = new Set();
  const ops          = [];
  const noMatcheados = [];
  let matcheados     = 0;
  let identificados  = 0;
  let totalRows      = 0;

  // Registra la operación de vinculación para un movimiento bancario.
  // Recibe un grupo de CxC que comparten el mismo número de autorización y
  // deben quedar vinculadas al mismo depósito bancario (relación N:1).
  // saldoActual de cada link = movTotal (total del ABO/CBT/CPF/CFC de esa CxC).
  // saldoErp = suma de todos los movTotals → se compara contra deposito.
  // El filtro del bulkWrite permite tanto movimientos limpios como parciales
  // (erp-auto previo) y protege contra race conditions con otros usuarios.
  function pushGroupOp(mov, grupo) {
    const existingIds = new Set(mov.erpIds || []);
    const grupoNuevo  = grupo.filter(({ cxc }) => !existingIds.has(cxc.erpId));
    if (!grupoNuevo.length) return; // todas las CxC del grupo ya vinculadas

    usedMovIds.add(mov._idStr);
    matcheados += grupoNuevo.length;

    const newLinks = [...(mov.erpLinks || [])];
    const newIds   = [...(mov.erpIds   || [])];
    for (const { movTotal, cxc } of grupoNuevo) {
      newLinks.push({
        erpId:          cxc.erpId,
        saldoActual:    movTotal,          // total del ABO/CBT/CPF/CFC que aporta esta CxC
        folioFiscal:    cxc.folioFiscal    ?? null,
        total:          cxc.total          ?? null,
        serie:          cxc.serie          ?? null,
        folioExterno:   cxc.folioExterno   ?? null,
        tieneRetencion: cxc.tieneRetencion ?? false,
        tipoPago:       cxc.tipoPago ? String(cxc.tipoPago).trim().toUpperCase() : null,
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

    // Motor automático, sin `user` real detrás — el helper solo setea
    // userId/nombre=null si es la primera vez que este movimiento se identifica.
    const { primeraIdentificacionAt, primeraIdentificacionPor } =
      resolvePrimeraIdentificacion(newStatus, mov, null);

    ops.push({
      updateOne: {
        // Protección ACID: replica la misma condición de la query inicial.
        // Si entre la lectura y esta escritura un humano intervino en el movimiento,
        // el filtro no matchea y MongoDB descarta la operación silenciosamente.
        filter: {
          _id:             mov._id,
          status:          'no_identificado',
          identificadoPor: { $not: { $elemMatch: { userId: { $nin: [...MOTOR_USERIDS, null] } } } },
        },
        update: {
          $set: {
            erpIds:   newIds,
            erpLinks: newLinks,
            saldoErp,
            status:   newStatus,
            identificadoPor: [{ userId: 'erp-auto', nombre: 'Motor ERP', fechaId: new Date() }],
            primeraIdentificacionAt,
            primeraIdentificacionPor,
          },
        },
      },
    });
  }

  // ── Helper: extrae filas (autNorm, movTotal, cxc) de un array de CxC ────────
  // Solo genera filas para CxC que tengan autorización en formasPago.
  // CxC sin auth en ningún formasPago son descartadas silenciosamente.
  // erpIdsIgnorar: Set de erpIds a omitir (evita duplicados en llamadas sucesivas).
  const seenPairs = new Set();
  function extraerRows(cxcs, erpIdsIgnorar) {
    const rows = [];
    for (const cxc of cxcs) {
      if (erpIdsIgnorar?.has(cxc.erpId)) continue;
      for (const mov of (cxc.movimientos || [])) {
        if (!SERIES_CON_AUTH.includes(mov.serie)) continue;
        for (const fp of (mov.formasPago || [])) {
          const autNorm = normalizarAuth(fp.autorizacion);
          if (!autNorm) continue;
          const pairKey = `${cxc.erpId}:${autNorm}`;
          if (seenPairs.has(pairKey)) continue;
          seenPairs.add(pairKey);
          // fp.monto es más preciso en pagos mixtos (ej. tarjeta + efectivo), donde
          // fp.monto es la porción cubierta por esta forma de pago específica.
          // EXCEPCIÓN: pago masivo (1 transferencia → N CxC). El ERP registra el monto
          // total de la transferencia en fp.monto de CADA CxC, no la contribución
          // individual. En ese caso fp.monto > abs(mov.total) y usamos mov.total.
          const movTotalAbs = Math.abs(mov.total ?? 0);
          const fpMontoAbs  = Math.abs(fp.monto  ?? 0);
          const movTotal    = fpMontoAbs > movTotalAbs && movTotalAbs > 0
            ? movTotalAbs
            : (fpMontoAbs || movTotalAbs);
          rows.push({ autNorm, movTotal, cxc });
        }
      }
    }
    return rows;
  }

  // ── Búsqueda de movimiento bancario candidato ─────────────────────────────
  // Versión especializada para el motor ERP. Diferencias clave vs findInIndex:
  //   · Sin allocaciones de array intermedio — itera directamente sobre candidates.
  //   · Usa _idStr y fechaMs pre-computados (sin toString() ni new Date() en hot path).
  //   · Exige AMBOS fecha (ventana) y importe — no existe fallback "importe sin fecha".
  //     Si una auth coincide pero el depósito está fuera de la ventana temporal,
  //     se descarta para evitar falsos positivos en auths que se repiten en el tiempo.
  //   · Sin split noId/source: la query inicial ya garantiza status='no_identificado'.
  function hallarMov(index, autNorm, grupoTotal, grupoFechaMs) {
    const candidates = index.get(autNorm);
    if (!candidates?.length) return null;
    for (const m of candidates) {
      if (usedMovIds.has(m._idStr)) continue;
      if (!importeOk(m, grupoTotal)) continue;
      if (grupoFechaMs !== null && m.fechaMs !== null &&
          Math.abs(m.fechaMs - grupoFechaMs) > DATE_MATCH_WINDOW_MS) continue;
      return m;
    }
    return null;
  }

  // ── Motor de match — ejecuta el loop sobre un conjunto de filas CxC ─────────
  // Diseño single-thread con setImmediate: cede el event loop cada 500 iteraciones.
  // Worker Threads serían overhead — el bottleneck es I/O (MongoDB), no CPU.
  // El loop de matching corre en < 50 ms para volúmenes típicos (~5K grupos).
  async function ejecutarFaseDeMatch(rows, pctStart, pctEnd, phaseLabel) {
    if (!rows.length) return;
    totalRows += rows.length;

    // Agrupar por auth: N CxC con el mismo autNorm → 1 movimiento bancario
    const groupsByAuth = new Map();
    for (const { autNorm, movTotal, cxc } of rows) {
      if (!groupsByAuth.has(autNorm)) groupsByAuth.set(autNorm, []);
      groupsByAuth.get(autNorm).push({ movTotal, cxc });
    }

    const totalGrupos  = groupsByAuth.size;
    let procesados     = 0;
    let lastPctEmitido = pctStart;

    for (const [autNorm, grupo] of groupsByAuth) {
      // Suma de los importes de cada formasPago del grupo
      const grupoTotal = grupo.reduce((s, r) => s + r.movTotal, 0);

      // Timestamp representativo: el más temprano entre fechaRealPago y fechaAfectacion.
      // Entero (ms) — evita new Date() por cada movimiento candidato en hallarMov.
      const grupoFechaMs = grupo.reduce((earliest, { cxc }) => {
        const d = cxc.fechaRealPago ?? cxc.fechaAfectacion ?? null;
        if (!d) return earliest;
        const ms = new Date(d).getTime();
        return earliest === null || ms < earliest ? ms : earliest;
      }, null);

      let foundMov = null;

      // 1a: numeroAutorizacion (primer bloque) — fecha + importe estrictos
      foundMov = hallarMov(byAuthNorm, autNorm, grupoTotal, grupoFechaMs);
      // 1b: referenciaNumerica — fecha + importe estrictos
      if (!foundMov) foundMov = hallarMov(byRefNorm, autNorm, grupoTotal, grupoFechaMs);
      // 1c: bloques secundarios (ej. BBVA "xxx/yyy") — fecha + importe estrictos
      if (!foundMov) foundMov = hallarMov(byAuthNormAlt, autNorm, grupoTotal, grupoFechaMs);

      if (foundMov) {
        pushGroupOp(foundMov, grupo);
      } else {
        for (const { movTotal, cxc } of grupo) {
          noMatcheados.push({
            autorizacion:  autNorm,
            importe:       movTotal,
            banco:         bancoNorm,
            erpId:         cxc.erpId         ?? null,
            folioExterno:  cxc.folioExterno  ?? null,
            serie:         cxc.serie         ?? null,
            folioFiscal:   cxc.folioFiscal   ?? null,
            fechaRealPago: cxc.fechaRealPago ?? null,
          });
        }
      }

      procesados++;
      if (totalGrupos > 0) {
        const pct = pctStart + Math.round((procesados / totalGrupos) * (pctEnd - pctStart));
        if (pct >= lastPctEmitido + 5) {
          lastPctEmitido = pct;
          onProgress?.({ phase: 'matching', pct, msg: `${phaseLabel}: ${procesados} de ${totalGrupos}` });
        }
      }

      // Ceder el event loop cada 500 iteraciones — el loop es más liviano que antes
      // (sin búsqueda de texto libre), así que podemos yieldar con menor frecuencia.
      if (procesados % 500 === 0) {
        await new Promise(r => setImmediate(r));
      }
    }
  }

  onProgress?.({ phase: 'matching', pct: 55, msg: `Cruzando ${cxcsA.length} CxC contra ${movimientos.length} movimientos...` });
  await ejecutarFaseDeMatch(extraerRows(cxcsA, null), 55, 70, 'Fase A (auths explícitas)');

  // ── Fase B: concepto como fallback — lazy, solo para movimientos sin auth estructurada ──
  // Condición de inclusión: movimiento (a) no matcheó en Fase A, Y (b) no tiene
  // ningún campo estructurado de auth. Si tiene numeroAutorizacion pero no matcheó,
  // agregar tokens de concepto no aportaría nada útil.
  // Los tokens deben existir EXACTAMENTE en _autsNorm de la CxC — sin texto libre.
  // La precisión es idéntica a Fase A: fecha ±N días + importe ±$1 obligatorios.
  const movSinMatchSinAuth = movimientos.filter(m =>
    !usedMovIds.has(m._idStr) &&
    !normalizarAuth(m.numeroAutorizacion) &&
    !normalizarAuth(m.referenciaNumerica) &&
    m.concepto,
  );

  if (movSinMatchSinAuth.length > 0) {
    // Pre-computar tokens una sola vez por movimiento — se reutilizan al indexar.
    for (const m of movSinMatchSinAuth) {
      m._conceptoTokens = extraerTokensConcepto(m.concepto);
    }

    const authsConcepto = new Set();
    for (const m of movSinMatchSinAuth) {
      for (const t of m._conceptoTokens) authsConcepto.add(t);
    }
    // Excluir auths ya consultadas en Fase A para no recargar las mismas CxC.
    for (const a of authsConcretas) authsConcepto.delete(a);

    if (authsConcepto.size > 0) {
      onProgress?.({ phase: 'loading-cxc-b', pct: 72, msg: `Fase B: ${authsConcepto.size} tokens · ${movSinMatchSinAuth.length} movimientos sin auth estructurada...` });
      const cxcsB = await ErpCuentaPendiente.aggregate([
        { $match: buildCxcMatchFilter(authsConcepto) },
        stageProyeccion,
      ]);

      if (cxcsB.length > 0) {
        // Incorporar estos movimientos al índice byAuthNorm con sus tokens de concepto.
        // hallarMov los encontrará sin ninguna lógica adicional.
        for (const m of movSinMatchSinAuth) {
          for (const t of m._conceptoTokens) {
            if (!byAuthNorm.has(t)) byAuthNorm.set(t, []);
            byAuthNorm.get(t).push(m);
          }
        }

        const erpIdsA = new Set(cxcsA.map(c => c.erpId));
        const rowsB   = extraerRows(cxcsB, erpIdsA);
        if (rowsB.length > 0) {
          onProgress?.({ phase: 'matching', pct: 75, msg: `Fase B: cruzando ${rowsB.length} filas (concepto)...` });
          await ejecutarFaseDeMatch(rowsB, 75, 85, 'Fase B (concepto)');
        }
      }
    }
  }

  onProgress?.({ phase: 'writing', pct: 85, msg: `Guardando ${ops.length} asociación(es) en la base de datos...` });
  // ── Escritura en bulk (con transacción si el entorno lo soporta) ─────────
  if (ops.length > 0) {
    await ejecutarBulkConTransaccion(ops);
  }

  // ── Filtrar noMatcheados que ya estaban vinculados en corridas anteriores ───
  // Un CxC puede aparecer como "sin match" si su movimiento ya fue identificado
  // antes de esta corrida (excluido del query de movimientos elegibles).
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
    total:        totalRows,
    matcheados,
    identificados,
    sinMatch:     trueSinMatch.length,
    noMatcheados: trueSinMatch,
  };
}

// ══════════════════════════════════════════════════════════════════════════════
// MATCH DESDE EXCEL (compatibilidad — solo actualiza status, sin erpLinks)
// ══════════════════════════════════════════════════════════════════════════════

// Igual que findInIndex pero con firma adaptada al motor Excel.
// fecha es el valor de la columna Fecha del Excel: se usa como preferencia de proximidad
// temporal dentro de findInIndex (sin ser criterio de exclusión — strict=false).
function buscarEnIndice(indice, autNorm, importe, banco, usedMovIds, fecha) {
  return findInIndex(indice, autNorm, importe, usedMovIds, banco, fecha);
}

// user: { userId, nombre } — identifica quién ejecutó la carga del Excel.
// userId suele ser el Auth0 sub; nombre es el nombre del usuario en la DB.
async function ejecutarMatch(rows, user) {
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
  }).select('_id numeroAutorizacion referenciaNumerica concepto deposito retiro status banco fecha primeraIdentificacionAt primeraIdentificacionPor').lean();

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

  // Pre-filtro para la Fase 3F (fuzzy): solo movimientos con auth estructurada
  // y status no_identificado. Se construye una sola vez y se itera en el fallback.
  // El filtro por importe dentro del loop de 3F es muy selectivo → la iteración
  // es rápida incluso con miles de movimientos con auth.
  const movConAuth = movimientos.filter(m =>
    m.status === 'no_identificado' &&
    (normalizarAuth(m.numeroAutorizacion) || normalizarAuth(m.referenciaNumerica)),
  );

  // ── Motor de match ────────────────────────────────────────────────────────
  const usedMovIds         = new Set(); // evita que dos filas consuman el mismo movimiento
  // movId → { primeraIdentificacionAt, primeraIdentificacionPor } — antes era un Set<string>
  // de movIds; se amplió a Map para poder resolver primeraIdentificacion() una sola vez por
  // movimiento (en el momento en que se decide agregarlo, con el `mov` todavía en memoria)
  // y reusar ese resultado al armar el $set del bulkWrite más abajo, sin tener que volver
  // a buscar el `mov` original por id. idsAIdentificar no se usa en ningún otro lado del
  // archivo (confirmado por grep) — el cambio de tipo es seguro.
  const idsAIdentificar    = new Map();
  // movId → autNorm: movimientos donde faltaba auth en DB y ahora podemos completarlo
  const idsActualizarAuth  = new Map();
  const noMatcheados       = [];
  const yaIdentificadosArr = [];
  // Lista completa de matcheados con detalle para el Excel de resultado.
  // Incluye fase 1/2/3 (auth) y fase 4 (banco+monto), con estado diferenciado.
  const matcheadosList     = [];
  let matcheados = 0;

  for (const row of rows) {
    let mov = null;

    // 1a: por numeroAutorizacion (respeta banco, usedMovIds y fecha como preferencia)
    mov = buscarEnIndice(byAuthNorm, row.autNorm, row.importe, row.banco, usedMovIds, row.fecha);

    // 1b: por referenciaNumerica
    if (!mov) mov = buscarEnIndice(byRefNorm, row.autNorm, row.importe, row.banco, usedMovIds, row.fecha);

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

    // ── Fase 3F: fuzzy match por autorización ────────────────────────────────
    // Se activa solo cuando las fases exactas fallaron y la fila tiene fecha e
    // importe disponibles (guardianes obligatorios para evitar falsos positivos).
    // Compara usando Levenshtein ≤ 1 (auths <10 dígitos) ó ≤ 2 (≥10 dígitos).
    // Guarda el movimiento identificado en matcheadosList con estado diferenciado.
    if (!mov && row.fecha && row.importe) {
      const refMs = row.fecha.getTime();
      for (const m of movConAuth) {
        if (usedMovIds.has(m._id.toString())) continue;
        // Importe exacto — filtro muy selectivo, hace la iteración eficiente
        if (!importeOk(m, row.importe)) continue;
        // Fecha dentro de la ventana — obligatorio para evitar homonimia temporal
        if (m.fecha && Math.abs(new Date(m.fecha).getTime() - refMs) > DATE_MATCH_WINDOW_MS) continue;
        // Banco — si ambos están disponibles deben coincidir
        if (row.banco && m.banco && row.banco !== m.banco) continue;

        const authsMovimiento = [
          normalizarAuth(m.numeroAutorizacion),
          normalizarAuth(m.referenciaNumerica),
        ].filter(Boolean);

        const maxLen  = Math.max(row.autNorm.length, ...authsMovimiento.map(a => a.length));
        const maxDist = fuzzyAuthMaxDist(maxLen);
        const esFuzzy = authsMovimiento.some(a => levenshtein(row.autNorm, a) <= maxDist);
        if (!esFuzzy) continue;

        // Marcar el movimiento como encontrado vía fuzzy para distinguirlo
        // en matcheadosList. El objeto viene de .lean() — podemos añadir props.
        m._fuzzyMatch = true;
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
      const esNuevo = mov.status !== 'identificado';
      if (esNuevo) {
        // Se resuelve aquí, con `mov` todavía en memoria (viene de .lean() más arriba),
        // en vez de volver a buscarlo al armar el bulkWrite. La transición siempre es
        // hacia 'identificado' en esta rama (ver bulkWrite de idsAIdentificar abajo).
        const primeraId = resolvePrimeraIdentificacion(
          'identificado', mov, { _id: user.userId, nombre: user.nombre },
        );
        idsAIdentificar.set(mov._id.toString(), primeraId);
      }
      matcheadosList.push({
        autorizacion: row.autNorm,
        importe:      row.importe ?? null,
        banco:        row.banco   ?? null,
        estado:       esNuevo
          ? (mov._fuzzyMatch ? 'Nuevo identificado (fuzzy)' : 'Nuevo identificado')
          : 'Ya identificado',
      });
    } else {
      // ── Fase 4: fallback por importe + banco entre ya identificados ─────────
      // Ocurre cuando el movimiento fue identificado (ERP/manual) pero su
      // numeroAutorizacion es null en DB (ej. BBVA "DEPOSITO EN EFECTIVO").
      // Se reporta como "ya identificado", no como "sin match".
      // Cuando hay varios candidatos con el mismo monto, se prioriza el más próximo
      // temporalmente a la fecha del Excel (si está disponible).
      let yaIdMov = null;
      if (row.importe) {
        const centavos = Math.round(row.importe * 100);
        // Preferencia: banco + monto; fallback: solo monto
        const candidatosBM = row.banco
          ? (idPorBancoMonto.get(`${row.banco}|${centavos}`) ?? [])
          : [];
        const candidatosM  = idPorMonto.get(String(centavos)) ?? [];
        const pool = candidatosBM.length ? candidatosBM : candidatosM;

        // Filtrar primero, luego ordenar por proximidad temporal si hay fecha
        const validos = pool.filter(m => !usedMovIds.has(m._id.toString()) && importeOk(m, row.importe));
        if (validos.length > 1 && row.fecha) {
          const refMs = row.fecha.getTime();
          validos.sort((a, b) => {
            const da = a.fecha ? Math.abs(new Date(a.fecha).getTime() - refMs) : Infinity;
            const db = b.fecha ? Math.abs(new Date(b.fecha).getTime() - refMs) : Infinity;
            return da - db;
          });
        }
        yaIdMov = validos[0] ?? null;
      }

      if (yaIdMov) {
        usedMovIds.add(yaIdMov._id.toString());
        yaIdentificadosArr.push({ autorizacion: row.autNorm, importe: row.importe ?? null, banco: row.banco ?? null });
        matcheadosList.push({
          autorizacion: row.autNorm,
          importe:      row.importe ?? null,
          banco:        row.banco   ?? null,
          estado:       'Ya identificado (sin auth)',
        });
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
    const ops = [...idsAIdentificar.entries()].map(([id, primeraId]) => {
      const upd = {
        $set: {
          status:          'identificado',
          // Registrar al usuario real que cargó el Excel, no un motor genérico.
          identificadoPor: [{ userId: user.userId, nombre: user.nombre, fechaId: ahora }],
          primeraIdentificacionAt:  primeraId.primeraIdentificacionAt,
          primeraIdentificacionPor: primeraId.primeraIdentificacionPor,
        },
      };
      // Incluir auth si lo tenemos y no estaba en DB — queda vinculado desde esta corrida
      if (idsActualizarAuth.has(id)) {
        upd.$set.numeroAutorizacion = idsActualizarAuth.get(id);
        idsActualizarAuth.delete(id); // evitar doble escritura
      }
      return {
        updateOne: {
          // Protección ACID: solo actualizar si el movimiento sigue sin identificar
          // y ningún humano lo ha tocado entre la lectura y esta escritura.
          // Espeja la misma condición del motor ERP para consistencia.
          filter: {
            _id:             id,
            status:          'no_identificado',
            identificadoPor: { $not: { $elemMatch: { userId: { $nin: [...MOTOR_USERIDS, null] } } } },
          },
          update: upd,
        },
      };
    });
    const result = await BankMovement.bulkWrite(ops, { ordered: false });
    identificados = result.modifiedCount;
  }

  // Actualizar auth en movimientos ya identificados donde faltaba (fase 4).
  // Solo se tocan movimientos identificados por motores automáticos: si un humano
  // fue quien identificó el movimiento, no se sobreescribe su registro.
  // El filtro adicional `numeroAutorizacion: null` garantiza idempotencia: si
  // en una corrida anterior ya se grabó el auth, esta op no hace nada.
  if (idsActualizarAuth.size > 0) {
    const authOps = [...idsActualizarAuth.entries()].map(([id, autNorm]) => ({
      updateOne: {
        filter: {
          _id:                id,
          numeroAutorizacion: null,
          identificadoPor:    { $not: { $elemMatch: { userId: { $nin: [...MOTOR_USERIDS, null] } } } },
        },
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
    matcheadosList,
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
  fecha:          ['fecha', 'date'],
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
  // colFecha comienza en null: si no se detecta no se enviará fecha al motor.
  let colFecha = null, colMonto = 3, colBanco = 4, colAuth = 6;

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

  if (found.fecha)        colFecha = found.fecha;
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

    // Fecha de la fila: se usa como criterio de preferencia en el matching.
    // ExcelJS puede devolver Date o string según cómo esté guardada la celda.
    let fecha = null;
    if (colFecha != null) {
      const raw = row.getCell(colFecha).value;
      const d   = raw instanceof Date ? raw : (raw != null ? new Date(raw) : null);
      if (d && !isNaN(d.getTime())) fecha = d;
    }

    rows.push({ autNorm, importe, banco, fecha });
  });
  return rows;
}

async function matchAutorizaciones(buffer, user) {
  return ejecutarMatch(await parseAutorizaciones(buffer), user);
}

module.exports = {
  matchAutorizacionesDesdeErp,
  matchAutorizaciones,
  // Exportado únicamente para test de regresión (bank-autorizaciones.service.ejecutarMatch.test.js).
  // No es parte de la API pública del módulo — matchAutorizaciones() sigue siendo el entry point real.
  ejecutarMatch,
  // Exportados para traspasos-internos.service.js: reusa el mismo helper de bulkWrite con
  // detección de topología (replica set vs standalone) y el mismo normalizador de nombre
  // de banco, en vez de duplicar la lógica.
  ejecutarBulkConTransaccion,
  normalizarBanco,
};