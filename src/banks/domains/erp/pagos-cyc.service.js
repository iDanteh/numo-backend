'use strict';

const ExcelJS            = require('exceljs');
const mongoose           = require('mongoose');
const BankMovement       = require('../banks/BankMovement.model');
const ErpCuentaPendiente = require('./ErpCuentaPendiente.model');

const MOTOR_ID     = 'pagos-cyc';
const MOTOR_NOMBRE = 'Excel Pagos CYC';

// Todos los userIds de motores conocidos — usado en el guard ACID del bulkWrite
// para distinguir identificaciones automáticas de las hechas por un humano.
const MOTOR_USER_IDS_ALL = new Set([
  'erp-auto', 'aut-match', 'refact-cyc', 'mostrador-cyc', MOTOR_ID,
]);

// ── Normaliza texto para matching tolerante (lowercase + colapsa espacios) ────
function normalizeConcepto(str) {
  return String(str ?? '').toLowerCase().trim().replace(/\s+/g, ' ');
}

// ── Elimina ceros a la izquierda de un número de autorización ────────────────
// Espeja la lógica de normalizeAuthNum() del parser de bancos.
function stripLeadingZeros(val) {
  const s = String(val ?? '').trim().replace(/^'+/, '');
  if (!s) return null;
  return s.replace(/^0+(?=\d)/, '') || s;
}

// ── Extrae candidatos de número de autorización desde una descripción ─────────
// Orden de precedencia:
//   1. BBVA-style: primer bloque numérico después del '/'
//   2. Prefijo "Aut." explícito
//   3. Genérico: secuencias de 5–18 dígitos
function extractAuthCandidates(desc) {
  if (!desc) return [];
  const s          = String(desc).trim();
  const seen       = new Set();
  const candidates = [];

  const push = (raw) => {
    const norm = stripLeadingZeros(raw);
    if (norm && !seen.has(norm)) { seen.add(norm); candidates.push(norm); }
  };

  // 1. BBVA-style: bloque numérico inmediatamente después del '/'
  const slashIdx = s.indexOf('/');
  if (slashIdx !== -1) {
    const after = s.substring(slashIdx + 1).trimStart();
    const m = after.match(/^(\d+)/);
    if (m) push(m[1]);
  }

  // 2. Prefijo "Aut." explícito
  for (const m of s.matchAll(/\bAut\.?\s*(\d+)/gi)) {
    push(m[1]);
  }

  // 3. Genérico: secuencias de 5–18 dígitos
  for (const m of s.matchAll(/\d{5,18}/g)) {
    push(m[0]);
  }

  return candidates;
}

// ── Normaliza el nombre de un banco a forma canónica ─────────────────────────
// Mapea variantes comerciales al mismo identificador, de modo que
// "Bancomer", "BBVA Bancomer" y "BBVA" resulten equivalentes.
// Esto es crítico para PAGOS CYC porque el Excel del cliente usa "Bancomer"
// mientras que la BD almacena el nombre que el parser asignó en la importación.
function canonicalizeBanco(name) {
  if (!name) return '';
  const n = normalizeConcepto(name);
  if (n.includes('bancomer') || n.includes('bbva'))       return 'bbva';
  if (n.includes('banamex')  || n.includes('citibanamex')) return 'banamex';
  if (n.includes('santander'))                             return 'santander';
  if (n.includes('azteca'))                                return 'azteca';
  if (n.includes('scotiabank'))                            return 'scotiabank';
  if (n.includes('banorte'))                               return 'banorte';
  if (n.includes('spin') || n.includes('nu ') || n.includes('nu\b')) return 'nu';
  return n;
}

// ── Matching tolerante de nombre de banco con canonicalización ────────────────
// Usando la forma canónica de ambos lados absorbemos variantes como:
//   "Bancomer" (Excel) ↔ "BBVA" (DB)
//   "BBVA Bancomer"    ↔ "BBVA"
//   "Citibanamex"      ↔ "Banamex"
function bancosCoinciden(excelBanco, dbBanco) {
  if (!excelBanco || !dbBanco) return false;
  const e = canonicalizeBanco(excelBanco);
  const d = canonicalizeBanco(dbBanco);
  return e === d || e.includes(d) || d.includes(e);
}

// ── Selecciona el mejor movimiento de un pool ─────────────────────────────────
// Prioriza el que tenga banco coincidente; si no, devuelve el primero disponible.
function pickBest(pool, usedMovIds, bancoNorm) {
  const disponibles = pool.filter(m => !usedMovIds.has(m._id.toString()));
  if (bancoNorm) {
    return disponibles.find(m => bancosCoinciden(bancoNorm, m.banco)) ?? disponibles[0] ?? null;
  }
  return disponibles[0] ?? null;
}

// ── Parsea "A0-251102469" o "A0- 251203597" → { serie, folio } ───────────────
function parsearFolio(raw) {
  const str = String(raw ?? '').trim();
  const m = str.match(/^([A-Z][A-Z0-9]*)\s*-\s*(\d+)/);
  if (m) return { serie: m[1], folio: m[2] };
  return null;
}

// ── Extrae todos los folios de una celda VENTAS ───────────────────────────────
// Soporta separadores: salto de línea, espacio, coma, punto y coma
function extraerFolios(rawCelda) {
  return String(rawCelda ?? '').trim()
    .split(/[\n\r\s,;]+/)
    .map(s => s.trim())
    .filter(Boolean)
    .map(parsearFolio)
    .filter(Boolean);
}

// ── Parse del Excel PAGOS CYC ─────────────────────────────────────────────────
// Columnas: FECHA(1) DESCRIPCIÓN(2) MONTO(3) BANCO(4) VENTAS(5)
// Nota: no hay columna CLIENTE en este formato (a diferencia de Mostrador CYC).
async function parseExcel(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  const ws = wb.worksheets[0];
  if (!ws) throw new Error('El archivo no contiene hojas válidas');

  const rowsConVentas = [];
  const rowsSinVentas = [];

  ws.eachRow({ includeEmpty: false }, (row, idx) => {
    if (idx === 1) return; // encabezado

    const fechaRaw       = row.getCell(1).value;
    const descripcionRaw = row.getCell(2).value;
    const montoRaw       = row.getCell(3).value;
    const bancoRaw       = row.getCell(4).value;
    const ventasRaw      = row.getCell(5).value;

    const importe     = montoRaw       != null ? Number(montoRaw)                   : null;
    const descripcion = descripcionRaw != null ? String(descripcionRaw).trim()      : null;
    const banco       = bancoRaw       != null ? String(bancoRaw).trim()            : null;
    const fecha       = fechaRaw instanceof Date ? fechaRaw : null;
    const folios      = extraerFolios(ventasRaw);

    const entry = { fila: idx, fecha, descripcion, importe, banco };

    if (!folios.length || importe == null || isNaN(importe) || importe <= 0) {
      rowsSinVentas.push({
        ...entry,
        ventasRaw: ventasRaw != null ? String(ventasRaw).trim() : null,
      });
    } else {
      rowsConVentas.push({ ...entry, folios });
    }
  });

  return { rowsConVentas, rowsSinVentas };
}

// ═════════════════════════════════════════════════════════════════════════════
// SERVICIO PRINCIPAL
// ─────────────────────────────────────────────────────────────────────────────
// Flujo idéntico a mostrador-cyc pero con mejoras en el matching de banco:
//
//  1. Parse Excel → filas con/sin VENTAS
//  2. Lookup masivo CxC en erp_cuentas_pendientes por serie + folio
//  3. Cargar movimientos filtrados por importe ($in selectivo)
//     → 5 índices en memoria:
//       Tier 1: concepto_norm|centavos|auth:XXXXXX  (concepto + auth)
//       Tier 2: auth:XXXXXX|centavos / ref:XXXXXX|centavos  (id + importe)
//       Tier 3: concepto_norm|centavos|date:YYYY-MM-DD  (fecha exacta)
//       Tier 4: concepto_norm|centavos  (fallback puro)
//  4. Motor de match fila a fila (Tier 1 → 2 → 3 → 4)
//     · Guard pre-escritura: skip si status='identificado' O erpLinks > 0
//     · canonicalizeBanco(): "Bancomer" ≡ "BBVA" en todos los tiers
//  5. Bulk write ACID
//     · Filter: isActive, sin identificación humana ($nor), sin erpLinks
//     · Fallback automático si la topología no es replica set
//  6. Retornar resumen completo con detalles de cada fila
// ═════════════════════════════════════════════════════════════════════════════
async function procesarPagosCyc(buffer, usuarioId, usuarioNombre) {
  // ── 1. Parse ────────────────────────────────────────────────────────────────
  const { rowsConVentas, rowsSinVentas } = await parseExcel(buffer);
  const total = rowsConVentas.length + rowsSinVentas.length;

  const baseResult = {
    total,
    relacionados:   0,
    escritos:       0,
    ignorados:      rowsSinVentas.length,
    errors: { folioNoEncontrado: 0, sinMovimientoBancario: 0, yaIdentificado: 0 },
    detalleRelacionados:  [],
    detalleNoMatcheados:  [],
    detalleIgnorados: rowsSinVentas.map(r => ({
      fila:        r.fila,
      fecha:       r.fecha,
      descripcion: r.descripcion,
      importe:     r.importe,
      banco:       r.banco,
    })),
    advertencias: [],
  };

  if (!rowsConVentas.length) return baseResult;

  // ── 2. Lookup masivo de CxC en erp_cuentas_pendientes ────────────────────
  // Estrategia $in separado: más eficiente que $or de N pares cuando la colección
  // tiene índice en serie + folio por separado. El ajuste fino (par exacto) se
  // hace en memoria con el Set de claves "SERIE|FOLIO".
  const allFolioKeys = new Set();
  const seriesSet    = new Set();
  const foliosSet    = new Set();

  for (const row of rowsConVentas) {
    for (const { serie, folio } of row.folios) {
      allFolioKeys.add(`${serie}|${folio}`);
      seriesSet.add(serie);
      foliosSet.add(folio);
    }
  }

  const cxcDocs = allFolioKeys.size > 0
    ? await ErpCuentaPendiente.find(
        {
          serie: { $in: [...seriesSet] },
          folio: { $in: [...foliosSet] },
        },
        {
          erpId: 1, serie: 1, folio: 1,
          serieExterna: 1, folioExterno: 1, folioFiscal: 1,
          saldoActual: 1, total: 1,
        },
      ).lean()
    : [];

  // Índice O(1): "SERIE|FOLIO" → CxC (descarta pares cruzados del $in)
  const cxcByFolio = new Map();
  for (const cxc of cxcDocs) {
    const key = `${cxc.serie}|${cxc.folio}`;
    if (allFolioKeys.has(key)) cxcByFolio.set(key, cxc);
  }

  // ── 3. Cargar movimientos filtrados por importe ───────────────────────────
  // Consulta selectiva: solo los importes presentes en el Excel.
  // Reduce el dataset de potencialmente decenas de miles a solo los candidatos.
  const uniqueImportes = [...new Set(
    rowsConVentas
      .filter(r => r.importe != null)
      .map(r => r.importe),
  )];

  const movimientos = uniqueImportes.length > 0
    ? await BankMovement.find(
        { isActive: true, deposito: { $in: uniqueImportes } },
        {
          _id: 1, concepto: 1, deposito: 1, banco: 1, fecha: 1,
          numeroAutorizacion: 1, referenciaNumerica: 1,
          status: 1, erpIds: 1, erpLinks: 1, identificadoPor: 1, folio: 1,
        },
      ).lean()
    : [];

  // ── 5 índices en memoria para matching multi-nivel ────────────────────────
  //
  // Tier 1 — byAuthNum       "concepto_norm|centavos|auth:XXXXXX"
  //   Máxima precisión: concepto + importe + auth coinciden.
  //
  // Tier 2a — byAuthImporte  "auth:XXXXXX|centavos"
  //   Auth + importe sin necesitar que concepto coincida.
  //   Útil para "Sin concepto Aut.11591" o SPEI con descripción abreviada.
  //   Seguridad graduada: banco fuzzy → único candidato → rechazar.
  //
  // Tier 2b — byRefImporte   "ref:XXXXXX|centavos"
  //   Igual que 2a pero usando referenciaNumerica (Banamex y otros).
  //
  // Tier 3 — byFecha         "concepto_norm|centavos|date:YYYY-MM-DD"
  //   Concepto + importe + fecha exacta. Rompe empates cuando no hay auth.
  //
  // Tier 4 — byConceptoImporte  "concepto_norm|centavos"
  //   Fallback puro.
  //
  // bancosCoinciden() usa canonicalizeBanco() en todos los tiers, lo que
  // garantiza que "Bancomer" (Excel) coincida con "BBVA" (DB).
  const byConceptoImporte = new Map(); // Tier 4
  const byFecha           = new Map(); // Tier 3
  const byRefImporte      = new Map(); // Tier 2b
  const byAuthImporte     = new Map(); // Tier 2a
  const byAuthNum         = new Map(); // Tier 1

  for (const m of movimientos) {
    const centavos = Math.round((m.deposito ?? 0) * 100);
    const baseKey  = `${normalizeConcepto(m.concepto)}|${centavos}`;

    // Tier 4
    if (!byConceptoImporte.has(baseKey)) byConceptoImporte.set(baseKey, []);
    byConceptoImporte.get(baseKey).push(m);

    // Tier 3
    if (m.fecha) {
      const dk = `${baseKey}|date:${new Date(m.fecha).toISOString().slice(0, 10)}`;
      if (!byFecha.has(dk)) byFecha.set(dk, []);
      byFecha.get(dk).push(m);
    }

    if (m.numeroAutorizacion) {
      // Tier 2a
      const ak2 = `auth:${m.numeroAutorizacion}|${centavos}`;
      if (!byAuthImporte.has(ak2)) byAuthImporte.set(ak2, []);
      byAuthImporte.get(ak2).push(m);

      // Tier 1
      const ak1 = `${baseKey}|auth:${m.numeroAutorizacion}`;
      if (!byAuthNum.has(ak1)) byAuthNum.set(ak1, []);
      byAuthNum.get(ak1).push(m);
    }

    // Tier 2b
    if (m.referenciaNumerica) {
      const rk = `ref:${m.referenciaNumerica}|${centavos}`;
      if (!byRefImporte.has(rk)) byRefImporte.set(rk, []);
      byRefImporte.get(rk).push(m);
    }
  }

  // ── 4. Motor de match ───────────────────────────────────────────────────────
  const usedMovIds          = new Set();
  const ops                 = [];
  const advertencias        = [];
  const detalleRelacionados = [];
  const detalleNoMatcheados = [];
  let relacionados      = 0;
  let folioNoEncontrado = 0;
  let sinMovBancario    = 0;
  let yaIdentificado    = 0;

  for (const row of rowsConVentas) {
    const { fila, fecha, descripcion, importe, banco, folios } = row;

    // 4a. Resolver CxC por folio ──────────────────────────────────────────────
    const cxcsResueltas   = [];
    const foliosFaltantes = [];

    for (const { serie, folio } of folios) {
      const cxc = cxcByFolio.get(`${serie}|${folio}`);
      if (cxc) cxcsResueltas.push(cxc);
      else     foliosFaltantes.push(`${serie}-${folio}`);
    }

    if (!cxcsResueltas.length) {
      folioNoEncontrado++;
      detalleNoMatcheados.push({
        fila, fecha, descripcion, importe, banco,
        folios:   folios.map(f => `${f.serie}-${f.folio}`),
        razon:    'folio_no_encontrado',
        detalle:  `Folio(s) no encontrados en la base de datos: ${folios.map(f => `${f.serie}-${f.folio}`).join(', ')}`,
        candidato: null,
      });
      continue;
    }

    // 4b. Búsqueda multi-nivel Tier 1 → 2 → 3 → 4 ───────────────────────────
    const centavos  = Math.round((importe ?? 0) * 100);
    const baseKey   = `${normalizeConcepto(descripcion)}|${centavos}`;
    const bancoNorm = banco; // se pasa como string a bancosCoinciden vía pickBest
    const authCands = extractAuthCandidates(descripcion);

    let foundMov = null;

    // Tier 1 — concepto_norm + importe + auth
    if (authCands.length > 0) {
      for (const auth of authCands) {
        const pool = byAuthNum.get(`${baseKey}|auth:${auth}`) ?? [];
        foundMov = pickBest(pool, usedMovIds, bancoNorm);
        if (foundMov) break;
      }
    }

    // Tier 2 — identificador + importe (sin concepto)
    // Seguridad graduada: banco fuzzy → único candidato disponible → rechazar.
    if (!foundMov && authCands.length > 0) {
      outer2: for (const auth of authCands) {
        for (const [prefix, idx] of [['auth', byAuthImporte], ['ref', byRefImporte]]) {
          const pool        = idx.get(`${prefix}:${auth}|${centavos}`) ?? [];
          const disponibles = pool.filter(m => !usedMovIds.has(m._id.toString()));
          if (!disponibles.length) continue;

          // 1. Banco coincide (con canonicalización)
          if (bancoNorm) {
            foundMov = disponibles.find(m => bancosCoinciden(bancoNorm, m.banco)) ?? null;
          }

          // 2. Único candidato disponible → auth/ref + importe lo identifican
          if (!foundMov && disponibles.length === 1) {
            foundMov = disponibles[0];
          }

          if (foundMov) break outer2;
        }
      }
    }

    // Tier 3 — concepto_norm + importe + fecha exacta
    if (!foundMov && fecha instanceof Date && !isNaN(fecha.getTime())) {
      const dateStr = fecha.toISOString().slice(0, 10);
      const pool    = byFecha.get(`${baseKey}|date:${dateStr}`) ?? [];
      foundMov      = pickBest(pool, usedMovIds, bancoNorm);
    }

    // Tier 4 — concepto_norm + importe (fallback puro)
    if (!foundMov) {
      const pool = byConceptoImporte.get(baseKey) ?? [];
      foundMov   = pickBest(pool, usedMovIds, bancoNorm);
    }

    if (!foundMov) {
      sinMovBancario++;
      detalleNoMatcheados.push({
        fila, fecha, descripcion, importe, banco,
        folios:   folios.map(f => `${f.serie}-${f.folio}`),
        razon:    'sin_movimiento_bancario',
        detalle:  `No se encontró movimiento bancario con concepto e importe ($${
          (importe ?? 0).toLocaleString('es-MX', { minimumFractionDigits: 2 })
        }) coincidentes`,
        candidato: null,
      });
      continue;
    }

    // 4c. Guard pre-escritura: NO sobreescribir trabajo previo ────────────────
    // Se salta el movimiento si:
    //   · status === 'identificado' (ya fue procesado por cualquier medio)
    //   · erpLinks.length > 0 (ya tiene CxC vinculadas)
    // Esto protege tanto el trabajo humano como el de otros motores.
    // El guard ACID en el bulkWrite (paso 5) agrega una segunda capa de
    // protección específicamente contra race conditions con trabajo humano.
    const yaConCxC = foundMov.status === 'identificado'
      || (foundMov.erpLinks?.length ?? 0) > 0;

    if (yaConCxC) {
      yaIdentificado++;
      detalleNoMatcheados.push({
        fila, fecha, descripcion, importe, banco,
        folios:   folios.map(f => `${f.serie}-${f.folio}`),
        razon:    'ya_identificado',
        detalle:  foundMov.status === 'identificado'
          ? 'El movimiento bancario ya está marcado como identificado.'
          : `El movimiento bancario ya tiene ${foundMov.erpLinks.length} CxC vinculada(s).`,
        candidato: {
          movId:    foundMov._id.toString(),
          movFolio: foundMov.folio ?? null,
          concepto: foundMov.concepto ?? null,
          deposito: foundMov.deposito ?? null,
          banco:    foundMov.banco    ?? null,
          status:   foundMov.status   ?? null,
        },
      });
      continue;
    }

    // 4d. Vincular ────────────────────────────────────────────────────────────
    relacionados++;
    usedMovIds.add(foundMov._id.toString());

    if (foliosFaltantes.length > 0) {
      advertencias.push({ fila, foliosFaltantes });
    }

    const newLinks = cxcsResueltas.map(cxc => ({
      erpId:          cxc.erpId,
      saldoActual:    cxc.saldoActual ?? null,
      folioFiscal:    cxc.folioFiscal  ?? null,
      total:          cxc.total        ?? null,
      serie:          cxc.serie        ?? null,
      folioExterno:   cxc.folioExterno ?? null,
      tieneRetencion: false,
    }));
    const newIds  = cxcsResueltas.map(c => c.erpId);
    const saldoErp = newLinks.reduce((s, l) => {
      const ref = (l.saldoActual != null && l.saldoActual > 0)
        ? l.saldoActual
        : (l.total ?? 0);
      return s + ref;
    }, 0);
    const uuidXML = newLinks.find(l => l.folioFiscal)?.folioFiscal?.toUpperCase() ?? null;

    ops.push({
      updateOne: {
        // Guard ACID (doble capa):
        //  · isActive: true — no tocar movimientos eliminados
        //  · $nor identificadoPor: no sobreescribir si hay userId humano
        //  · erpLinks.0 $exists false: no sobreescribir si ya tiene CxC vinculadas
        filter: {
          _id:      foundMov._id,
          isActive: true,
          $nor: [{
            identificadoPor: {
              $elemMatch: { userId: { $nin: [...MOTOR_USER_IDS_ALL] } },
            },
          }],
          'erpLinks.0': { $exists: false },
        },
        update: {
          $set: {
            erpIds:   newIds,
            erpLinks: newLinks,
            saldoErp,
            uuidXML,
            status:   'identificado',
            identificadoPor: [{
              userId:  usuarioId    ?? MOTOR_ID,
              nombre:  usuarioNombre ?? MOTOR_NOMBRE,
              fechaId: new Date(),
            }],
          },
        },
      },
    });

    detalleRelacionados.push({
      fila,
      fecha,
      descripcion,
      importe,
      banco,
      folios:            folios.map(f => `${f.serie}-${f.folio}`),
      foliosEncontrados: cxcsResueltas.map(c => `${c.serie}-${c.folio}`),
      foliosFaltantes,
      movId:             foundMov._id.toString(),
      movFolio:          foundMov.folio ?? null,
      cxcCount:          cxcsResueltas.length,
    });
  }

  // ── 5. Bulk write ACID ────────────────────────────────────────────────────
  // Mismo patrón de fallback automático (replica set → standalone) que
  // refacturaciones-cyc.service.js y mostrador-cyc.service.js.
  let escritos = 0;

  if (ops.length > 0) {
    const topologyType = mongoose.connection.client?.topology?.description?.type;
    const esReplicaSet = ['ReplicaSetWithPrimary', 'ReplicaSetNoPrimary', 'Sharded']
      .includes(topologyType);

    if (esReplicaSet) {
      let session = null;
      try {
        session = await mongoose.connection.startSession();
        session.startTransaction();
        const result = await BankMovement.bulkWrite(ops, { ordered: false, session });
        await session.commitTransaction();
        escritos = result.modifiedCount;
      } catch (err) {
        if (session?.inTransaction?.()) {
          try { await session.abortTransaction(); } catch (_) { /* ignorar */ }
        }
        const sinSoporte = err.code === 20
          || /transaction numbers are only allowed/i.test(err.message);
        if (sinSoporte) {
          const result = await BankMovement.bulkWrite(ops, { ordered: false });
          escritos = result.modifiedCount;
        } else {
          throw err;
        }
      } finally {
        if (session) {
          try { await session.endSession(); } catch (_) { /* ignorar */ }
        }
      }
    } else {
      const result = await BankMovement.bulkWrite(ops, { ordered: false });
      escritos = result.modifiedCount;
    }
  }

  // ── 6. Resultado ─────────────────────────────────────────────────────────
  return {
    total,
    relacionados,
    escritos,
    ignorados: rowsSinVentas.length,
    errors: { folioNoEncontrado, sinMovimientoBancario: sinMovBancario, yaIdentificado },
    detalleRelacionados,
    detalleNoMatcheados,
    detalleIgnorados: rowsSinVentas.map(r => ({
      fila:        r.fila,
      fecha:       r.fecha,
      descripcion: r.descripcion,
      importe:     r.importe,
      banco:       r.banco,
    })),
    advertencias,
  };
}

// ═════════════════════════════════════════════════════════════════════════════
// GENERADOR DE EXCEL (export del resultado)
// ─────────────────────────────────────────────────────────────────────────────
// 3 hojas: Relacionados · No Relacionados · Ignorados
// Sin columna CLIENTE (no existe en el formato PAGOS CYC).
// ═════════════════════════════════════════════════════════════════════════════
async function generarExcelPagosCyc(resultado) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'Numo — Pagos CYC';
  wb.created = new Date();

  const HEADER_FILL = {
    type: 'pattern', pattern: 'solid',
    fgColor: { argb: 'FF6D28D9' },   // violeta corporativo
  };
  const HEADER_FONT = { bold: true, color: { argb: 'FFFFFFFF' }, size: 10 };

  const OK_FILL   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD1FAE5' } }; // verde
  const ERR_FILL  = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEE2E2' } }; // rojo
  const WARN_FILL = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFEF9C3' } }; // amarillo
  const GRAY_FILL = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF3F4F6' } }; // gris

  function formatFecha(d) {
    if (!d) return '';
    const dt = d instanceof Date ? d : new Date(d);
    if (isNaN(dt.getTime())) return '';
    return dt.toLocaleDateString('es-MX', { day: '2-digit', month: '2-digit', year: 'numeric' });
  }

  function styleHeader(ws) {
    ws.getRow(1).eachCell(cell => {
      cell.fill = HEADER_FILL;
      cell.font = HEADER_FONT;
      cell.alignment = { vertical: 'middle', horizontal: 'center' };
    });
    ws.getRow(1).height = 20;
  }

  const RAZON_LABEL = {
    folio_no_encontrado:     'Folio no encontrado',
    sin_movimiento_bancario: 'Sin movimiento bancario',
    ya_identificado:         'Ya identificado',
  };

  // ── Hoja 1: Relacionados ──────────────────────────────────────────────────
  const wsRel = wb.addWorksheet('Relacionados');
  wsRel.columns = [
    { header: 'Fila',              key: 'fila',     width:  6 },
    { header: 'Fecha',             key: 'fecha',    width: 12 },
    { header: 'Descripción',       key: 'desc',     width: 55 },
    { header: 'Monto',             key: 'monto',    width: 13 },
    { header: 'Banco',             key: 'banco',    width: 13 },
    { header: 'Folios Vinculados', key: 'foliosEnc', width: 35 },
    { header: 'Folios Faltantes',  key: 'foliosFalt', width: 25 },
    { header: 'CxC Vinculadas',    key: 'cxcCount', width: 14 },
    { header: 'Mov. Folio',        key: 'movFolio', width: 12 },
    { header: 'Mov. ID',           key: 'movId',    width: 28 },
  ];
  styleHeader(wsRel);

  for (const r of (resultado.detalleRelacionados ?? [])) {
    const row = wsRel.addRow({
      fila:      r.fila,
      fecha:     formatFecha(r.fecha),
      desc:      r.descripcion ?? '',
      monto:     r.importe,
      banco:     r.banco ?? '',
      foliosEnc: (r.foliosEncontrados ?? []).join(', '),
      foliosFalt:(r.foliosFaltantes   ?? []).join(', '),
      cxcCount:  r.cxcCount,
      movFolio:  r.movFolio ?? '',
      movId:     r.movId,
    });
    row.eachCell(cell => { cell.fill = OK_FILL; });
    if ((r.foliosFaltantes ?? []).length > 0) {
      row.getCell('foliosFalt').fill = WARN_FILL;
      row.getCell('foliosFalt').font = { color: { argb: 'FF92400E' } };
    }
  }
  wsRel.getColumn('monto').numFmt = '#,##0.00';
  wsRel.autoFilter = { from: 'A1', to: wsRel.lastColumn.letter + '1' };

  // ── Hoja 2: No Relacionados ───────────────────────────────────────────────
  const wsNoRel = wb.addWorksheet('No Relacionados');
  wsNoRel.columns = [
    { header: 'Fila',        key: 'fila',    width:  6 },
    { header: 'Fecha',       key: 'fecha',   width: 12 },
    { header: 'Descripción', key: 'desc',    width: 55 },
    { header: 'Monto',       key: 'monto',   width: 13 },
    { header: 'Banco',       key: 'banco',   width: 13 },
    { header: 'Folio(s)',    key: 'folios',  width: 35 },
    { header: 'Razón',       key: 'razon',   width: 24 },
    { header: 'Detalle',     key: 'detalle', width: 60 },
  ];
  styleHeader(wsNoRel);

  for (const r of (resultado.detalleNoMatcheados ?? [])) {
    const row = wsNoRel.addRow({
      fila:    r.fila,
      fecha:   formatFecha(r.fecha),
      desc:    r.descripcion ?? '',
      monto:   r.importe,
      banco:   r.banco ?? '',
      folios:  (r.folios ?? []).join(', '),
      razon:   RAZON_LABEL[r.razon] ?? r.razon,
      detalle: r.detalle,
    });
    const fill = r.razon === 'ya_identificado' ? WARN_FILL : ERR_FILL;
    row.eachCell(cell => { cell.fill = fill; });
  }
  wsNoRel.getColumn('monto').numFmt = '#,##0.00';
  wsNoRel.autoFilter = { from: 'A1', to: wsNoRel.lastColumn.letter + '1' };

  // ── Hoja 3: Ignorados ─────────────────────────────────────────────────────
  const wsIgn = wb.addWorksheet('Ignorados');
  wsIgn.columns = [
    { header: 'Fila',        key: 'fila',  width:  6 },
    { header: 'Fecha',       key: 'fecha', width: 12 },
    { header: 'Descripción', key: 'desc',  width: 55 },
    { header: 'Monto',       key: 'monto', width: 13 },
    { header: 'Banco',       key: 'banco', width: 13 },
  ];
  styleHeader(wsIgn);

  for (const r of (resultado.detalleIgnorados ?? [])) {
    const row = wsIgn.addRow({
      fila:  r.fila,
      fecha: formatFecha(r.fecha),
      desc:  r.descripcion ?? '',
      monto: r.importe ?? '',
      banco: r.banco ?? '',
    });
    row.eachCell(cell => { cell.fill = GRAY_FILL; });
  }
  wsIgn.getColumn('monto').numFmt = '#,##0.00';

  return wb.xlsx.writeBuffer();
}

module.exports = { procesarPagosCyc, generarExcelPagosCyc };
