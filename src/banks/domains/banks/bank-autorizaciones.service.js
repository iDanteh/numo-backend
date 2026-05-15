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
// MATCH DESDE ERP — flujo invertido two-phase
// ──────────────────────────────────────────────────────────────────────────────
// Opera exclusivamente sobre los datos en erp_cuentas_pendientes (sin sync).
//
// Flujo:
//  1. Cargar movimientos bancarios elegibles (no_identificado + deposito > 0).
//  2. Extraer authsConcretas: solo campos estructurados (numeroAutorizacion,
//     referenciaNumerica, bloques BBVA "xxx/yyy"). ≤ ~500 elementos.
//  3A. Query CxC Fase A: { _autsNorm: { $in: [...authsConcretas] } } — usa índice.
//  3B. Query CxC Fase B (lazy): tokens ≥5 dígitos del concepto, extraídos SOLO
//      de los movimientos que no resolvieron en Fase A. ≤ ~500 elementos.
//  4. Indexar movimientos en Maps para O(1) lookup por auth.
//  5. Motor de match (cuatro rutas: auth, referencia, bloque alt, concepto).
//     setImmediate cada 100 iter — event loop no bloqueado.
//  6. Escritura bulk con transacción ACID en replica set.
//
// Características:
//  · La query CxC escala O(log N) gracias al índice _autsNorm.
//  · Cada formasPago se procesa individualmente → una CxC puede vincularse
//    a múltiples movimientos bancarios (pagos parciales).
//  · Modo estricto en rutas 1a/1b/1c: importe requerido cuando grupoTotal > 0.
// ══════════════════════════════════════════════════════════════════════════════
async function matchAutorizacionesDesdeErp({ banco, fechaDesde } = {}, { onProgress } = {}) {
  const bancoNorm = banco ? normalizarBanco(banco) : null;

  onProgress?.({ phase: 'loading-mov', pct: 5, msg: 'Cargando movimientos bancarios...' });
  // ── 1. Movimientos bancarios elegibles ─────────────────────────────────────
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
    return { total: 0, matcheados: 0, identificados: 0, sinMatch: 0, noMatcheados: [] };
  }

  // ── 2. Construir authsConcretas — solo campos explícitos (sin tokens de concepto) ──
  // El authNormSet original mezclaba auths estructuradas con tokens del concepto
  // (texto libre), produciendo arrays $in de hasta 15 000 elementos que degradan
  // el índice _autsNorm a CollScan.
  // La separación en dos fases resuelve esto:
  //   Fase A: authsConcretas ≤ ~500 → $in pequeño, índice B-tree eficiente O(log N)
  //   Fase B: tokens de concepto extraídos lazy SOLO de movimientos sin match en A
  //           → $in ~200-500 elementos, no de todos los movimientos
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
      erpId: 1, total: 1, folioFiscal: 1, serie: 1, folioExterno: 1,
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

  onProgress?.({ phase: 'loading-cxc', pct: 20, msg: `${movimientos.length} movimientos · cargando CxC Fase A (${authsConcretas.size} auths explícitas)...` });
  // ── 3A. Query ERP Fase A — $in pequeño, usa índice _autsNorm ────────────────
  // El match opera exclusivamente sobre los datos almacenados en erp_cuentas_pendientes.
  // No hay dependencia de frescura del caché: se usa lo que esté en la colección.
  const cxcsA = authsConcretas.size > 0
    ? await ErpCuentaPendiente.aggregate([{ $match: buildCxcMatchFilter(authsConcretas) }, stageProyeccion])
    : [];

  onProgress?.({ phase: 'indexing', pct: 40, msg: `${cxcsA.length} CxC (fase A) / ${movimientos.length} movimientos · construyendo índices de búsqueda...` });
  // ── Índices de búsqueda — construidos una sola vez, compartidos entre fases ──
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
    if (m.concepto) {
      const b = m.banco;
      if (!porConceptoPorBanco.has(b)) porConceptoPorBanco.set(b, []);
      porConceptoPorBanco.get(b).push(m);
      porConceptoTodos.push(m);
    }
  }

  // ── Estado compartido entre ambas fases ────────────────────────────────────
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

    usedMovIds.add(mov._id.toString());
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
            { erpIds: { $eq: [] } },
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

  // ── Helper: extrae filas (autNorm, movTotal, cxc) de un array de CxC ────────
  // seenPairs es compartido entre fases para no reenviar el mismo (erpId, autNorm)
  // dos veces aunque la misma CxC sea cargada en ambas fases.
  // erpIdsIgnorar: Set de erpIds cuyas CxC ya fueron procesadas exitosamente.
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

  // ── Motor de match — ejecuta el loop sobre un conjunto de filas CxC ─────────
  // Diseño single-thread con setImmediate: cede el event loop cada 100 iteraciones
  // para garantizar que los eventos de socket (progress) sean emitidos en tiempo
  // real sin necesidad de Worker Threads.
  // Worker Threads serían overhead en este caso — el bottleneck es I/O (MongoDB),
  // no CPU. El loop de matching corre en < 50 ms para volúmenes típicos (~5K grupos).
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

      // Fecha representativa: la más temprana entre fechaRealPago y fechaAfectacion
      const grupoFecha = grupo.reduce((earliest, { cxc }) => {
        const d = cxc.fechaRealPago ?? cxc.fechaAfectacion ?? null;
        if (!d) return earliest;
        if (!earliest) return d;
        return new Date(d) < new Date(earliest) ? d : earliest;
      }, null);

      let foundMov = null;

      // 1a: match por numeroAutorizacion (primer bloque) — con fecha, modo estricto
      foundMov = findInIndex(byAuthNorm, autNorm, grupoTotal, usedMovIds, undefined, grupoFecha, true);
      // 1b: match por referenciaNumerica — con fecha, modo estricto
      if (!foundMov) foundMov = findInIndex(byRefNorm, autNorm, grupoTotal, usedMovIds, undefined, grupoFecha, true);
      // 1c: bloques secundarios del token (ej. BBVA "xxx/yyy") — con fecha, modo estricto
      if (!foundMov) foundMov = findInIndex(byAuthNormAlt, autNorm, grupoTotal, usedMovIds, undefined, grupoFecha, true);

      // 2: auth dentro del texto del concepto — exige importe correcto
      if (!foundMov) {
        const candidatos = bancoNorm
          ? (porConceptoPorBanco.get(bancoNorm) ?? [])
          : porConceptoTodos;
        for (const m of candidatos) {
          if (usedMovIds.has(m._id.toString())) continue;
          if (!conceptoContainsAuth(m.concepto, autNorm)) continue;
          if (grupoTotal && !importeOk(m, grupoTotal)) continue;
          foundMov = m;
          break;
        }
      }

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

      // Ceder el event loop cada 100 iteraciones — permite emitir eventos de socket
      // (progress) sin bloquear el procesamiento ni requerir Worker Threads.
      if (procesados % 100 === 0) {
        await new Promise(r => setImmediate(r));
      }
    }
  }

  onProgress?.({ phase: 'matching', pct: 55, msg: `Fase A: cruzando ${cxcsA.length} CxC (auths explícitas)...` });
  // ── Fase A: matching sobre CxC cargadas con auths concretas ────────────────
  await ejecutarFaseDeMatch(extraerRows(cxcsA, null), 55, 70, 'Fase A (auths explícitas)');

  // ── Fase B: fallback por concepto — lazy, solo para movimientos sin match ───
  // Los tokens del concepto se extraen ÚNICAMENTE de los movimientos que no
  // resolvieron en Fase A — no de todos los movimientos. Esto mantiene el $in
  // en ~200-500 elementos en lugar de los 10K-15K del enfoque original.
  const movSinMatchA = movimientos.filter(m => !usedMovIds.has(m._id.toString()) && m.concepto);

  if (movSinMatchA.length > 0) {
    const authsConcepto = new Set();
    for (const m of movSinMatchA) {
      for (const b of (m.concepto.match(/\d+/g) || [])) {
        const n = parseInt(b, 10);
        const s = isNaN(n) ? null : String(n);
        if (s && s.length >= 5) authsConcepto.add(s);
      }
    }
    // Eliminar auths ya consultadas en Fase A para no recargar las mismas CxC
    for (const a of authsConcretas) authsConcepto.delete(a);

    if (authsConcepto.size > 0) {
      onProgress?.({ phase: 'loading-cxc-b', pct: 65, msg: `Fase B: consultando ERP (${authsConcepto.size} tokens · ${movSinMatchA.length} movimientos pendientes)...` });

      const cxcsB = await ErpCuentaPendiente.aggregate([
        { $match: buildCxcMatchFilter(authsConcepto) },
        stageProyeccion,
      ]);

      // Omitir CxC ya procesadas en Fase A para evitar duplicar filas y ops
      const erpIdsA = new Set(cxcsA.map(c => c.erpId));
      const rowsB   = extraerRows(cxcsB, erpIdsA);

      if (rowsB.length > 0) {
        onProgress?.({ phase: 'matching', pct: 70, msg: `Fase B: cruzando ${rowsB.length} filas (concepto)...` });
        await ejecutarFaseDeMatch(rowsB, 70, 80, 'Fase B (concepto)');
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