'use strict';

const ExcelJS            = require('exceljs');
const mongoose           = require('mongoose');
const BankMovement       = require('../banks/BankMovement.model');
const ErpCuentaPendiente = require('./ErpCuentaPendiente.model');

const MOTOR_ID     = 'mostrador-cyc';
const MOTOR_NOMBRE = 'Excel Mostrador CYC';

// Todos los userIds de motores (no humanos) — para el guard ACID en bulkWrite
const MOTOR_USER_IDS_ALL = new Set(['erp-auto', 'aut-match', 'refact-cyc', MOTOR_ID]);

// ── Normaliza un concepto/descripción para matching tolerante ────────────────
// Convierte a minúsculas, colapsa espacios múltiples y elimina bordes.
// Aplica tanto al índice de movimientos (paso 3) como a la búsqueda (paso 4b).
function normalizeConcepto(str) {
  return String(str ?? '').toLowerCase().trim().replace(/\s+/g, ' ');
}

// ── Elimina ceros a la izquierda de un número de autorización ────────────────
// Espeja la lógica de normalizeAuthNum() del parser de bancos para que
// "00623771" (Excel) y "623771" (DB) sean equivalentes en la búsqueda.
function stripLeadingZeros(val) {
  const s = String(val ?? '').trim().replace(/^'+/, '');
  if (!s) return null;
  return s.replace(/^0+(?=\d)/, '') || s;
}

// ── Extrae candidatos de número de autorización desde una descripción ─────────
// Estrategia de extracción (orden de precedencia):
//
//   1. BBVA-style: primer bloque numérico después del '/' en el concepto.
//      ej. "DEPOSITO / 00623771 EMPRESA SA" → "623771"
//
//   2. Prefijo "Aut." (ej. "Sin concepto Aut.11591"):
//      el banco a veces genera descripciones abreviadas donde el único
//      dato útil es la etiqueta "Aut." seguida del número.
//
//   3. Genérico: todas las secuencias de 5–18 dígitos consecutivos.
//      El límite superior es 18 para cubrir claves de rastreo SPEI
//      (ej. "1276240017137", 13 dígitos) y referencias largas de Santander.
//
// Todos los candidatos se normalizan (sin ceros a la izquierda) para coincidir
// con el campo numeroAutorizacion de la BD, que se almacena ya normalizado.
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

  // 2. Prefijo "Aut." explícito — captura "Sin concepto Aut.11591" etc.
  for (const m of s.matchAll(/\bAut\.?\s*(\d+)/gi)) {
    push(m[1]);
  }

  // 3. Genérico: secuencias de 5–18 dígitos (cubre SPEI y auth cortos)
  for (const m of s.matchAll(/\d{5,18}/g)) {
    push(m[0]);
  }

  return candidates;
}

// ── Matching tolerante de nombre de banco ─────────────────────────────────────
// Compara dos nombres de banco normalizados usando contains bidireccional para
// absorber variantes como "BBVA Bancomer" ↔ "BBVA" o "Banamex" ↔ "Citibanamex".
// La dirección bidireccional cubre tanto el caso en que el Excel es más largo
// ("BBVA Bancomer") como más corto ("BBVA") que el valor almacenado en BD.
function bancosCoinciden(excelBanco, dbBanco) {
  if (!excelBanco || !dbBanco) return false;
  const e = normalizeConcepto(excelBanco);
  const d = normalizeConcepto(dbBanco);
  return e === d || e.includes(d) || d.includes(e);
}

// ── Selecciona el mejor movimiento de un pool para una fila ──────────────────
// Prioriza el movimiento cuyo banco coincida (con bancosCoinciden) con el Excel.
// Si no hay match de banco, devuelve el primero disponible (fallback sin pérdida).
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

// ── Parse del Excel ───────────────────────────────────────────────────────────
// Columnas: FECHA(1) DESCRIPCIÓN(2) IMPORTE(3) BANCO(4) VENTAS(5) CLIENTE(6)
async function parseExcel(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  const ws = wb.worksheets[0];
  if (!ws) throw new Error('El archivo no contiene hojas válidas');

  const rowsConVentas  = [];
  const rowsSinVentas  = [];

  ws.eachRow({ includeEmpty: false }, (row, idx) => {
    if (idx === 1) return; // encabezado

    const fechaRaw       = row.getCell(1).value;
    const descripcionRaw = row.getCell(2).value;
    const importeRaw     = row.getCell(3).value;
    const bancoRaw       = row.getCell(4).value;
    const ventasRaw      = row.getCell(5).value;
    const clienteRaw     = row.getCell(6).value;

    const importe     = importeRaw  != null ? Number(importeRaw)               : null;
    const descripcion = descripcionRaw != null ? String(descripcionRaw).trim() : null;
    const banco       = bancoRaw    != null ? String(bancoRaw).trim()           : null;
    const cliente     = clienteRaw  != null ? String(clienteRaw).trim()         : null;
    const fecha       = fechaRaw instanceof Date ? fechaRaw                     : null;
    const folios      = extraerFolios(ventasRaw);

    const entry = { fila: idx, fecha, descripcion, importe, banco, cliente };

    // Sin folios o importe inválido → ignorar
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
// Flujo:
//  1. Parse Excel → filas con/sin VENTAS
//  2. Lookup masivo CxC por serie + folio (determinístico)
//  3. Cargar movimientos bancarios → 4 índices en memoria:
//       Tier 1: concepto_norm|centavos|auth:XXXXXX  (concepto + auth)
//       Tier 2: auth:XXXXXX|centavos               (auth + importe, sin concepto)
//       Tier 3: concepto_norm|centavos|date:YYYY-MM-DD  (fecha exacta)
//       Tier 4: concepto_norm|centavos              (fallback)
//  4. Por cada fila con VENTAS:
//     · Resolver CxC por folio(s)
//     · Buscar BankMovement: Tier 1 → 2 → 3 → 4 (+ preferencia por banco)
//     · Saltar si ya tiene erpLinks o status='identificado'
//     · Preparar updateOne
//  5. Bulk write ACID
//  6. Retornar resumen completo
// ═════════════════════════════════════════════════════════════════════════════
async function procesarMostradorCyc(buffer, usuarioId, usuarioNombre) {
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
      cliente:     r.cliente,
    })),
    advertencias: [],
  };

  if (!rowsConVentas.length) return baseResult;

  // ── 2. Lookup masivo de CxC ──────────────────────────────────────────────────
  // Estrategia: $in separado por serie y folio en lugar de $or con N pares.
  // El $or de N pares no tiene índice compuesto → full scan × N condiciones.
  // Con $in de folios + $in de series MongoDB hace un solo recorrido filtrado,
  // y el ajuste fino (par exacto) lo hacemos en memoria con el Set de claves.
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

  // Índice O(1): "SERIE|FOLIO" → CxC
  // Filtramos solo los pares exactos que pedimos (el $in es un producto
  // cartesiano que puede traer (A0, folioDeB1) — lo descartamos aquí).
  const cxcByFolio = new Map();
  for (const cxc of cxcDocs) {
    const key = `${cxc.serie}|${cxc.folio}`;
    if (allFolioKeys.has(key)) cxcByFolio.set(key, cxc);
  }

  // ── 3. Cargar movimientos filtrados por importe (consulta selectiva) ─────────
  // NO cargamos toda la colección. Extraemos los importes únicos del Excel y
  // hacemos un $in sobre el campo indexado `deposito`. Esto puede reducir el
  // dataset de decenas de miles de documentos a solo los candidatos relevantes.
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

  // ── Cinco índices en memoria para matching multi-nivel ───────────────────────
  //
  // Tier 1 — byAuthNum       "concepto_norm|centavos|auth:XXXXXX"
  //   Más discriminante: concepto + importe + auth coinciden. Identifica
  //   unívocamente el movimiento aunque haya depósitos recurrentes del
  //   mismo cliente con igual concepto e importe.
  //
  // Tier 2 — byAuthImporte   "auth:XXXXXX|centavos"
  //           byRefImporte    "ref:XXXXXX|centavos"
  //   Número identificador + importe, SIN requerir que el concepto del
  //   Excel coincida con el concepto en BD. Se intenta primero con
  //   numeroAutorizacion y luego con referenciaNumerica (Banamex y otros
  //   bancos que almacenan el número útil como referencia, no como auth).
  //   Seguridad graduada:
  //     · Si el banco del Excel coincide con bancosCoinciden() → aceptar.
  //     · Si no coincide pero el pool tiene UN SOLO candidato disponible
  //       → aceptar: auth/ref + importe únicos son garantía suficiente.
  //     · Si hay múltiples sin coincidencia de banco → no aceptar en T2.
  //
  // Tier 3 — byFecha         "concepto_norm|centavos|date:YYYY-MM-DD"
  //   Concepto + importe + fecha exacta. Rompe empates cuando el auth no
  //   está disponible (Banamex efectivo, BBVA SPEI sin barra '/').
  //
  // Tier 4 — byConceptoImporte  "concepto_norm|centavos"
  //   Fallback puro: mismo comportamiento que la versión original.
  //
  // Todos los índices usan normalizeConcepto() → tolerancia a case y espacios.
  // Los centavos evitan problemas de precisión float.
  const byConceptoImporte = new Map(); // Tier 4
  const byFecha           = new Map(); // Tier 3
  const byRefImporte      = new Map(); // Tier 2b (referenciaNumerica)
  const byAuthImporte     = new Map(); // Tier 2a (numeroAutorizacion)
  const byAuthNum         = new Map(); // Tier 1

  for (const m of movimientos) {
    const centavos = Math.round((m.deposito ?? 0) * 100);
    const baseKey  = `${normalizeConcepto(m.concepto)}|${centavos}`;

    // Tier 4: siempre
    if (!byConceptoImporte.has(baseKey)) byConceptoImporte.set(baseKey, []);
    byConceptoImporte.get(baseKey).push(m);

    // Tier 3: si el movimiento tiene fecha
    if (m.fecha) {
      const dk = `${baseKey}|date:${new Date(m.fecha).toISOString().slice(0, 10)}`;
      if (!byFecha.has(dk)) byFecha.set(dk, []);
      byFecha.get(dk).push(m);
    }

    if (m.numeroAutorizacion) {
      // Tier 2a: numeroAutorizacion + importe (sin concepto)
      const ak2 = `auth:${m.numeroAutorizacion}|${centavos}`;
      if (!byAuthImporte.has(ak2)) byAuthImporte.set(ak2, []);
      byAuthImporte.get(ak2).push(m);

      // Tier 1: auth + concepto + importe
      const ak1 = `${baseKey}|auth:${m.numeroAutorizacion}`;
      if (!byAuthNum.has(ak1)) byAuthNum.set(ak1, []);
      byAuthNum.get(ak1).push(m);
    }

    // Tier 2b: referenciaNumerica + importe (Banamex y bancos que almacenan
    // el número de referencia separado del número de autorización)
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
  let relacionados        = 0;
  let folioNoEncontrado   = 0;
  let sinMovBancario      = 0;
  let yaIdentificado      = 0;

  for (const row of rowsConVentas) {
    const { fila, fecha, descripcion, importe, banco, cliente, folios } = row;

    // 4a. Resolver CxC por folio ──────────────────────────────────────────────
    const cxcsResueltas   = [];
    const foliosFaltantes = [];

    for (const { serie, folio } of folios) {
      const cxc = cxcByFolio.get(`${serie}|${folio}`);
      if (cxc) cxcsResueltas.push(cxc);
      else     foliosFaltantes.push(`${serie}-${folio}`);
    }

    if (!cxcsResueltas.length) {
      // Ningún folio existe en DB → no hay CxC que vincular
      folioNoEncontrado++;
      detalleNoMatcheados.push({
        fila, fecha, descripcion, importe, banco, cliente,
        folios:   folios.map(f => `${f.serie}-${f.folio}`),
        razon:    'folio_no_encontrado',
        detalle:  `Folio(s) no encontrados en la base de datos: ${folios.map(f => `${f.serie}-${f.folio}`).join(', ')}`,
        candidato: null,
      });
      continue;
    }

    // 4b. Búsqueda multi-nivel: Tier 1 → 2 → 3 → 4 ──────────────────────────
    //
    // Tier 1: concepto_norm + importe + auth
    //   Máxima precisión. El concepto del Excel coincide con el de la BD Y
    //   el auth también. Resuelve depósitos recurrentes con auth en concepto.
    //
    // Tier 2: auth + importe (sin concepto), banco estricto
    //   El concepto del Excel NO necesita coincidir con la BD. Resuelve:
    //     · "Sin concepto Aut.11591" — descripción abreviada sin concepto real
    //     · SPEI donde Santander almacena el auth en col8 y el concepto
    //       del Excel difiere del concepto importado
    //   Requiere coincidencia de banco (no hace fallback cross-banco) porque
    //   sin el ancla del concepto el riesgo de falso positivo es mayor.
    //
    // Tier 3: concepto_norm + importe + fecha exacta (día)
    //   Auth no disponible en DB o no extraíble del texto. La fecha rompe
    //   empates entre movimientos con igual concepto e importe (SPEI BBVA
    //   sin barra, Banamex efectivo recurrente).
    //
    // Tier 4: concepto_norm + importe — fallback puro (comportamiento original)
    //
    // pickBest() prioriza banco coincidente en Tiers 1, 3 y 4.
    // Tier 2 solo acepta banco coincidente; si no hay match, pasa al Tier 3.
    const centavos  = Math.round((importe ?? 0) * 100);
    const baseKey   = `${normalizeConcepto(descripcion)}|${centavos}`;
    const bancoNorm = normalizeConcepto(banco);
    const authCands = extractAuthCandidates(descripcion);

    let foundMov = null;

    // Tier 1 — auth + concepto + importe
    if (authCands.length > 0) {
      for (const auth of authCands) {
        const pool = byAuthNum.get(`${baseKey}|auth:${auth}`) ?? [];
        foundMov = pickBest(pool, usedMovIds, bancoNorm);
        if (foundMov) break;
      }
    }

    // Tier 2 — identificador + importe (sin necesitar que concepto coincida) ──
    // Intenta primero numeroAutorizacion (byAuthImporte) y luego
    // referenciaNumerica (byRefImporte) para el mismo candidato numérico.
    // Seguridad graduada: banco fuzzy → único candidato → rechazar.
    if (!foundMov && authCands.length > 0) {
      outer2: for (const auth of authCands) {
        for (const [prefix, idx] of [['auth', byAuthImporte], ['ref', byRefImporte]]) {
          const pool       = idx.get(`${prefix}:${auth}|${centavos}`) ?? [];
          const disponibles = pool.filter(m => !usedMovIds.has(m._id.toString()));
          if (!disponibles.length) continue;

          // 1. Banco fuzzy (contains bidireccional → absorbe "BBVA Bancomer" ↔ "BBVA")
          if (bancoNorm) {
            foundMov = disponibles.find(m => bancosCoinciden(bancoNorm, m.banco)) ?? null;
          }

          // 2. Único candidato disponible → auth/ref + importe lo identifican
          //    unívocamente aunque el banco no coincida por nombre exacto.
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
        fila, fecha, descripcion, importe, banco, cliente,
        folios:   folios.map(f => `${f.serie}-${f.folio}`),
        razon:    'sin_movimiento_bancario',
        detalle:  `No se encontró movimiento bancario con concepto e importe ($${
          (importe ?? 0).toLocaleString('es-MX', { minimumFractionDigits: 2 })
        }) coincidentes`,
        candidato: null,
      });
      continue;
    }

    // 4c. Protección de sobreescritura: saltar si ya tiene CxC o está identificado
    // El usuario solicitó explícitamente no tocar movimientos que:
    //   · status === 'identificado'
    //   · erpLinks.length > 0 (ya vinculado por cualquier medio)
    const yaConCxC = foundMov.status === 'identificado'
      || (foundMov.erpLinks?.length ?? 0) > 0;

    if (yaConCxC) {
      yaIdentificado++;
      detalleNoMatcheados.push({
        fila, fecha, descripcion, importe, banco, cliente,
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
    const newIds = cxcsResueltas.map(c => c.erpId);

    // Misma lógica de saldoErp que refacturaciones-cyc.service.js
    const saldoErp = newLinks.reduce((s, l) => {
      const ref = (l.saldoActual != null && l.saldoActual > 0)
        ? l.saldoActual
        : (l.total ?? 0);
      return s + ref;
    }, 0);
    const uuidXML = newLinks.find(l => l.folioFiscal)?.folioFiscal?.toUpperCase() ?? null;

    ops.push({
      updateOne: {
        // Guard ACID: solo escribir si no existe identificación humana ni CxC links
        // en el momento de la escritura (cubre race conditions).
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
              userId:  usuarioId   ?? MOTOR_ID,
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
      cliente,
      folios:            folios.map(f => `${f.serie}-${f.folio}`),
      foliosEncontrados: cxcsResueltas.map(c => `${c.serie}-${c.folio}`),
      foliosFaltantes,
      movId:             foundMov._id.toString(),
      movFolio:          foundMov.folio ?? null,
      cxcCount:          cxcsResueltas.length,
    });
  }

  // ── 5. Bulk write (mismo patrón ACID que refacturaciones-cyc.service.js) ────
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

  // ── 6. Resultado ────────────────────────────────────────────────────────────
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
      cliente:     r.cliente,
    })),
    advertencias,
  };
}

// ═════════════════════════════════════════════════════════════════════════════
// GENERADOR DE EXCEL (export del resultado)
// ─────────────────════════════════════════════════════════════════════════════
async function generarExcelMostradorCyc(resultado) {
  const wb = new ExcelJS.Workbook();
  wb.creator  = 'Numo — Mostrador CYC';
  wb.created  = new Date();

  const HEADER_FILL = {
    type: 'pattern', pattern: 'solid',
    fgColor: { argb: 'FF1D4ED8' },      // azul corporativo
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
    folio_no_encontrado:    'Folio no encontrado',
    sin_movimiento_bancario: 'Sin movimiento bancario',
    ya_identificado:        'Ya identificado',
  };

  // ── Hoja 1: Relacionados ───────────────────────────────────────────────────
  const wsRel = wb.addWorksheet('Relacionados');
  wsRel.columns = [
    { header: 'Fila',               key: 'fila',              width:  6 },
    { header: 'Fecha',              key: 'fecha',             width: 12 },
    { header: 'Descripción',        key: 'descripcion',       width: 50 },
    { header: 'Importe',            key: 'importe',           width: 12 },
    { header: 'Banco',              key: 'banco',             width: 12 },
    { header: 'Cliente',            key: 'cliente',           width: 28 },
    { header: 'Folios Vinculados',  key: 'foliosEnc',         width: 32 },
    { header: 'Folios Faltantes',   key: 'foliosFalt',        width: 25 },
    { header: 'CxC Vinculadas',     key: 'cxcCount',          width: 13 },
    { header: 'Mov. Folio',         key: 'movFolio',          width: 12 },
    { header: 'Mov. ID',            key: 'movId',             width: 28 },
  ];
  styleHeader(wsRel);

  for (const r of (resultado.detalleRelacionados ?? [])) {
    const row = wsRel.addRow({
      fila:        r.fila,
      fecha:       formatFecha(r.fecha),
      descripcion: r.descripcion ?? '',
      importe:     r.importe,
      banco:       r.banco ?? '',
      cliente:     r.cliente ?? '',
      foliosEnc:   (r.foliosEncontrados ?? []).join(', '),
      foliosFalt:  (r.foliosFaltantes   ?? []).join(', '),
      cxcCount:    r.cxcCount,
      movFolio:    r.movFolio ?? '',
      movId:       r.movId,
    });
    row.eachCell(cell => { cell.fill = OK_FILL; });
    if ((r.foliosFaltantes ?? []).length > 0) {
      // Advertencia: algunos folios no encontrados — resaltar diferente
      row.getCell('foliosFalt').fill = WARN_FILL;
      row.getCell('foliosFalt').font = { color: { argb: 'FF92400E' } };
    }
  }

  wsRel.getColumn('importe').numFmt = '#,##0.00';
  wsRel.autoFilter = { from: 'A1', to: wsRel.lastColumn.letter + '1' };

  // ── Hoja 2: No Relacionados ────────────────────────────────────────────────
  const wsNoRel = wb.addWorksheet('No Relacionados');
  wsNoRel.columns = [
    { header: 'Fila',        key: 'fila',        width:  6 },
    { header: 'Fecha',       key: 'fecha',        width: 12 },
    { header: 'Descripción', key: 'descripcion',  width: 50 },
    { header: 'Importe',     key: 'importe',      width: 12 },
    { header: 'Banco',       key: 'banco',        width: 12 },
    { header: 'Cliente',     key: 'cliente',      width: 28 },
    { header: 'Folio(s)',    key: 'folios',       width: 32 },
    { header: 'Razón',       key: 'razon',        width: 22 },
    { header: 'Detalle',     key: 'detalle',      width: 58 },
  ];
  styleHeader(wsNoRel);

  for (const r of (resultado.detalleNoMatcheados ?? [])) {
    const row = wsNoRel.addRow({
      fila:        r.fila,
      fecha:       formatFecha(r.fecha),
      descripcion: r.descripcion ?? '',
      importe:     r.importe,
      banco:       r.banco ?? '',
      cliente:     r.cliente ?? '',
      folios:      (r.folios ?? []).join(', '),
      razon:       RAZON_LABEL[r.razon] ?? r.razon,
      detalle:     r.detalle,
    });
    const fill = r.razon === 'ya_identificado' ? WARN_FILL : ERR_FILL;
    row.eachCell(cell => { cell.fill = fill; });
  }

  wsNoRel.getColumn('importe').numFmt = '#,##0.00';
  wsNoRel.autoFilter = { from: 'A1', to: wsNoRel.lastColumn.letter + '1' };

  // ── Hoja 3: Ignorados ──────────────────────────────────────────────────────
  const wsIgn = wb.addWorksheet('Ignorados');
  wsIgn.columns = [
    { header: 'Fila',        key: 'fila',        width:  6 },
    { header: 'Fecha',       key: 'fecha',        width: 12 },
    { header: 'Descripción', key: 'descripcion',  width: 50 },
    { header: 'Importe',     key: 'importe',      width: 12 },
    { header: 'Banco',       key: 'banco',        width: 12 },
    { header: 'Cliente',     key: 'cliente',      width: 28 },
  ];
  styleHeader(wsIgn);

  for (const r of (resultado.detalleIgnorados ?? [])) {
    const row = wsIgn.addRow({
      fila:        r.fila,
      fecha:       formatFecha(r.fecha),
      descripcion: r.descripcion ?? '',
      importe:     r.importe ?? '',
      banco:       r.banco ?? '',
      cliente:     r.cliente ?? '',
    });
    row.eachCell(cell => { cell.fill = GRAY_FILL; });
  }

  wsIgn.getColumn('importe').numFmt = '#,##0.00';

  return wb.xlsx.writeBuffer();
}

module.exports = { procesarMostradorCyc, generarExcelMostradorCyc };
