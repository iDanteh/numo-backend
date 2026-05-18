'use strict';

const ExcelJS            = require('exceljs');
const mongoose           = require('mongoose');
const BankMovement       = require('../banks/BankMovement.model');
const ErpCuentaPendiente = require('./ErpCuentaPendiente.model');
const { normalizarAuth, SERIES_CON_AUTH } = require('./erp-auth.utils');

const TOLERANCIA_MXN = 1.00;
const MOTOR_ID       = 'refact-cyc';
const MOTOR_NOMBRE   = 'Excel Refacturaciones CYC';

// Reutiliza el mismo mapa de normalización que bank-autorizaciones.service.js
const BANCO_MAP = {
  bancomer:          'BBVA',
  bbva:              'BBVA',
  'bbva bancomer':   'BBVA',
  'bbva mexico':     'BBVA',
  'bbva méxico':     'BBVA',
  banamex:           'Banamex',
  bnamex:            'Banamex',
  citibanamex:       'Banamex',
  citi:              'Banamex',
  santander:         'Santander',
  'banco santander': 'Santander',
  azteca:            'Azteca',
  'banco azteca':    'Azteca',
  banorte:           'Banorte',
  'banco banorte':   'Banorte',
  hsbc:              'HSBC',
  inbursa:           'Inbursa',
  scotiabank:        'Scotiabank',
  banbajio:          'BanBajío',
  'banbajío':        'BanBajío',
};

function normalizarBanco(nombre) {
  if (!nombre) return null;
  const s = String(nombre).trim()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
  if (BANCO_MAP[s]) return BANCO_MAP[s];
  for (const [key, val] of Object.entries(BANCO_MAP)) {
    if (s.includes(key) || key.includes(s)) return val;
  }
  return String(nombre).trim();
}

// ── Parsea "A0-251102175" o "A0- 251203597" → { serie, folio } ───────────────
function parsearFolioExterno(raw) {
  const str = String(raw ?? '').trim();
  const m = str.match(/^([A-Z][A-Z0-9]*)\s*-\s*(\d+)/);
  if (m) return { serie: m[1], folio: m[2] };
  return null;
}

// ── Extrae todos los folios de una celda (multi-folio separado por \n , ;) ────
function extraerFolios(rawCelda) {
  return String(rawCelda ?? '').trim()
    .split(/[\n,;]+/)
    .map(s => s.trim())
    .filter(Boolean)
    .map(parsearFolioExterno)
    .filter(Boolean);
}

// ── Extrae tokens numéricos ≥5 dígitos del CONCEPTO bancario ─────────────────
// Consistente con el umbral de Fase B en bank-autorizaciones.service.js.
// Al normalizarlos (quitar ceros iniciales) quedan comparables con los valores
// almacenados en numeroAutorizacion / referenciaNumerica de BankMovement.
function extraerTokensConcepto(concepto) {
  if (!concepto) return [];
  return [...new Set(
    (String(concepto).match(/\d+/g) ?? [])
      .map(t => normalizarAuth(t))
      .filter(t => t !== null && t.length >= 5),
  )];
}

// ── Monto de pago real de una CxC ─────────────────────────────────────────────
// El root.saldoActual es el saldo pendiente ACTUAL (puede ser 0 si ya fue pagado)
// y root.total es el total de la factura original — ninguno es el monto depositado.
// El valor correcto es la suma del |total| de los movimientos ABO/CBT/CPF/CFC:
// esos son los registros de cobro que genera el ERP al aplicar el pago bancario.
//
// Ejemplo: CxC con saldoActual=0, total=17438.9
//   movimientos[ABO].total = -16741.34  → montoPago = 16741.34  ← correcto
//
// Fallback (cuando no hay movimientos de pago): saldoActual → total (orden de
// preferencia descendente por precisión).
function calcularMontoPago(cxc) {
  const movsPago = (cxc.movimientos ?? []).filter(m => SERIES_CON_AUTH.includes(m.serie));
  if (movsPago.length > 0) {
    return movsPago.reduce((s, m) => s + Math.abs(m.total ?? 0), 0);
  }
  // Fallback: saldoActual si es > 0, sino total de la factura
  const saldo = Math.abs(cxc.saldoActual ?? 0);
  return saldo > 0 ? saldo : Math.abs(cxc.total ?? 0);
}

// ── Valida importe dentro de la tolerancia de $1 MXN ─────────────────────────
function importeOk(deposito, importe) {
  return Math.abs(Math.abs(deposito ?? 0) - Math.abs(importe ?? 0)) <= TOLERANCIA_MXN;
}

// ── Busca en un índice Map respetando movimientos ya usados ──────────────────
// Orden de preferencia: banco correcto > cualquier banco.
function buscarEnIndice(indice, token, importe, banco, usedMovIds) {
  const candidatos = indice.get(token);
  if (!candidatos?.length) return null;

  const pool = candidatos.filter(m => !usedMovIds.has(m._id.toString()) && importeOk(m.deposito, importe));
  if (!pool.length) return null;

  return pool.find(m => m.banco === banco) ?? pool[0];
}

// ── Determina si un movimiento fue identificado por un usuario humano ─────────
const MOTOR_USER_IDS = new Set(['erp-auto', 'aut-match', MOTOR_ID]);

function tieneIdentificacionHumana(mov) {
  return (mov.identificadoPor ?? []).some(e => !MOTOR_USER_IDS.has(e.userId));
}

// ── Parse del archivo Excel ───────────────────────────────────────────────────
async function parseExcel(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  const ws = wb.worksheets[0];
  if (!ws) throw new Error('El archivo no contiene hojas válidas');

  const rows = [];
  ws.eachRow({ includeEmpty: false }, (row, idx) => {
    if (idx === 1) return; // saltar encabezado

    const conceptoRaw = row.getCell(1).value;
    const importeRaw  = row.getCell(2).value;
    const bancoRaw    = row.getCell(3).value;
    const foliosRaw   = row.getCell(4).value;

    const importe = importeRaw != null ? Number(importeRaw) : null;
    const folios  = extraerFolios(foliosRaw);

    // Fila inválida: sin folios o sin importe parseable
    if (!folios.length || importe == null || isNaN(importe) || importe <= 0) return;

    rows.push({
      fila:     idx,
      concepto: conceptoRaw != null ? String(conceptoRaw).trim() : null,
      importe,
      banco:    normalizarBanco(bancoRaw),
      folios,
    });
  });

  return rows;
}

// ═════════════════════════════════════════════════════════════════════════════
// SERVICIO PRINCIPAL
// ─────────────────────────────────────────────────────────────────────────────
// Flujo:
//  1. Parse Excel → filas { fila, concepto, importe, banco, folios[] }
//  2. Lookup masivo CxC por serieExterna + folioExterno (exacto, sin ambigüedad)
//  3. Cargar BankMovements activos con deposito > 0 → construir índices auth/ref
//  4. Por cada fila:
//     · Tier 1 (AUTO): tokens numéricos del CONCEPTO → byAuthNorm / byRefNorm
//                      + importe ± $1 → vinculación automática
//     · Tier 2 (REVIEW): sin tokens numéricos en CONCEPTO → match solo por
//                        importe + banco → NO se escribe en DB; se retorna
//                        como candidato para revisión manual
//     · Sin match → se reporta con razón detallada
//  5. Bulk write de los AUTO en una sola operación
//  6. Retornar resumen + detalle de no matcheados (incluyendo REVIEW) para UI
// ═════════════════════════════════════════════════════════════════════════════
async function procesarRefacturacionesCyc(buffer, usuarioId, usuarioNombre) {
  // ── 1. Parse Excel ──────────────────────────────────────────────────────────
  const excelRows = await parseExcel(buffer);
  if (!excelRows.length) {
    return {
      total: 0, auto: 0, review: 0, escritos: 0,
      errors: { folioNoEncontrado: 0, sinMovBancario: 0 },
      detalleNoMatcheados: [],
    };
  }

  // ── 2. Lookup masivo de CxC ─────────────────────────────────────────────────
  // Recopilamos todos los pares serie/folio del Excel en una sola query $or.
  // Esto es O(1) desde el punto de vista de round-trips a MongoDB.
  const folioFilters = [];
  const allFolioKeys = new Set();
  for (const row of excelRows) {
    for (const { serie, folio } of row.folios) {
      const key = `${serie}|${folio}`;
      if (!allFolioKeys.has(key)) {
        allFolioKeys.add(key);
        folioFilters.push({ serieExterna: serie, folioExterno: folio });
      }
    }
  }

  const cxcDocs = await ErpCuentaPendiente.find(
    { $or: folioFilters },
    {
      erpId: 1, serieExterna: 1, folioExterno: 1,
      saldoActual: 1, total: 1, folioFiscal: 1, serie: 1,
      // Necesitamos movimientos para calcular el monto de pago real (ABO/CBT/CPF/CFC)
      // El root.saldoActual puede ser 0 (CxC ya liquidada) y root.total es el total
      // original de la factura — ninguno refleja lo que pagó este depósito bancario.
      movimientos: 1,
    },
  ).lean();

  // Índice O(1): "SERIE|FOLIO" → CxC
  const cxcByFolio = new Map();
  for (const cxc of cxcDocs) {
    cxcByFolio.set(`${cxc.serieExterna}|${cxc.folioExterno}`, cxc);
  }

  // ── 3. Cargar movimientos bancarios e índices ───────────────────────────────
  const movimientos = await BankMovement.find(
    { isActive: true, deposito: { $gt: 0 } },
    {
      _id: 1, numeroAutorizacion: 1, referenciaNumerica: 1,
      concepto: 1, deposito: 1, banco: 1, status: 1,
      erpIds: 1, erpLinks: 1, identificadoPor: 1,
    },
  ).lean();

  const byAuthNorm = new Map(); // normalizarAuth(numeroAutorizacion) → [mov]
  const byRefNorm  = new Map(); // normalizarAuth(referenciaNumerica)  → [mov]

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
  }

  // Índice adicional para Tier 2: importe+banco → mov
  // Se construye solo sobre movimientos elegibles para evitar falsos positivos.
  const byImporteBanco = new Map(); // `${banco}|${centavos}` → [mov]
  for (const m of movimientos) {
    const centavos = Math.round((m.deposito ?? 0) * 100);
    const k = `${m.banco ?? ''}|${centavos}`;
    if (!byImporteBanco.has(k)) byImporteBanco.set(k, []);
    byImporteBanco.get(k).push(m);
  }

  // ── 4. Motor de match ───────────────────────────────────────────────────────
  const usedMovIds         = new Set();
  const ops                = [];
  const detalleNoMatcheados = [];
  let autoCount   = 0;
  let reviewCount = 0;
  let folioNoEncontrado = 0;
  let sinMovBancario    = 0;

  for (const row of excelRows) {
    // 4a. Resolver CxC para los folios de esta fila ─────────────────────────
    const cxcsResueltas   = [];
    const foliosFaltantes = [];
    for (const { serie, folio } of row.folios) {
      const cxc = cxcByFolio.get(`${serie}|${folio}`);
      if (cxc) cxcsResueltas.push(cxc);
      else     foliosFaltantes.push(`${serie}-${folio}`);
    }

    if (!cxcsResueltas.length) {
      // Ningún folio encontrado en DB → no hay CxC que vincular
      folioNoEncontrado++;
      detalleNoMatcheados.push({
        fila:     row.fila,
        concepto: row.concepto,
        importe:  row.importe,
        banco:    row.banco,
        folios:   row.folios.map(f => `${f.serie}-${f.folio}`),
        razon:    'folio_no_encontrado',
        detalle:  `Folio(s) no encontrados en la base de datos: ${row.folios.map(f => `${f.serie}-${f.folio}`).join(', ')}`,
        candidato: null,
      });
      continue;
    }

    // 4b. Buscar BankMovement ──────────────────────────────────────────────
    let foundMov  = null;
    let confianza = null;

    const tokens = extraerTokensConcepto(row.concepto);

    // Tier 1: token del CONCEPTO → numeroAutorizacion / referenciaNumerica + importe
    // Intento primero con banco preferido, luego sin restricción de banco.
    for (const paso of ['conBanco', 'sinBanco']) {
      const bancoFiltro = paso === 'conBanco' ? row.banco : null;

      for (const token of tokens) {
        // Buscar en byAuthNorm
        const candidatosAuth = byAuthNorm.get(token) ?? [];
        for (const m of candidatosAuth) {
          if (usedMovIds.has(m._id.toString())) continue;
          if (!importeOk(m.deposito, row.importe)) continue;
          if (bancoFiltro && m.banco !== bancoFiltro) continue;
          foundMov  = m;
          confianza = 'auto';
          break;
        }
        if (foundMov) break;

        // Buscar en byRefNorm
        const candidatosRef = byRefNorm.get(token) ?? [];
        for (const m of candidatosRef) {
          if (usedMovIds.has(m._id.toString())) continue;
          if (!importeOk(m.deposito, row.importe)) continue;
          if (bancoFiltro && m.banco !== bancoFiltro) continue;
          foundMov  = m;
          confianza = 'auto';
          break;
        }
        if (foundMov) break;
      }
      if (foundMov) break;
    }

    // Tier 2: sin tokens numéricos en CONCEPTO → fallback por importe + banco
    // IMPORTANTE: solo activamos este tier cuando el concepto NO tiene tokens
    // (texto libre puro como "DEPOSITO EN EFECTIVO"). Si hay tokens pero ninguno
    // matcheó → es un error real, no un candidato de revisión.
    if (!foundMov && tokens.length === 0) {
      const centavos = Math.round(row.importe * 100);
      const k = `${row.banco ?? ''}|${centavos}`;
      const pool = byImporteBanco.get(k) ?? [];
      for (const m of pool) {
        if (usedMovIds.has(m._id.toString())) continue;
        foundMov  = m;
        confianza = 'review';
        break;
      }
    }

    // 4c. Clasificar resultado ─────────────────────────────────────────────
    if (!foundMov) {
      sinMovBancario++;
      const razonDetalle = tokens.length
        ? `Token(s) [${tokens.slice(0, 3).join(', ')}] no encontrados en movimientos bancarios`
        : `Sin tokens numéricos en el concepto; ningún movimiento con importe $${row.importe.toLocaleString('es-MX', { minimumFractionDigits: 2 })} en banco ${row.banco ?? 'desconocido'}`;
      detalleNoMatcheados.push({
        fila:      row.fila,
        concepto:  row.concepto,
        importe:   row.importe,
        banco:     row.banco,
        folios:    row.folios.map(f => `${f.serie}-${f.folio}`),
        razon:     'sin_movimiento_bancario',
        detalle:   razonDetalle,
        candidato: null,
      });
      continue;
    }

    // Tier 2 → no escribir en DB; retornar como candidato para revisión
    if (confianza === 'review') {
      reviewCount++;
      usedMovIds.add(foundMov._id.toString()); // reservar para no usar en otra fila
      detalleNoMatcheados.push({
        fila:     row.fila,
        concepto: row.concepto,
        importe:  row.importe,
        banco:    row.banco,
        folios:   row.folios.map(f => `${f.serie}-${f.folio}`),
        razon:    'requiere_revision',
        detalle:  `Candidato encontrado solo por importe — sin auth en concepto. Verifica manualmente.`,
        candidato: {
          movId:    foundMov._id.toString(),
          concepto: foundMov.concepto  ?? null,
          deposito: foundMov.deposito  ?? null,
          banco:    foundMov.banco     ?? null,
          status:   foundMov.status    ?? null,
        },
      });
      continue;
    }

    // Tier 1 → vincular en DB
    autoCount++;
    usedMovIds.add(foundMov._id.toString());

    // Construir erpLinks fusionando con los existentes (evita duplicados)
    const existingIds = new Set(foundMov.erpIds ?? []);
    const newLinks    = [...(foundMov.erpLinks ?? [])];
    const newIds      = [...(foundMov.erpIds   ?? [])];

    for (const cxc of cxcsResueltas) {
      if (existingIds.has(cxc.erpId)) continue;
      newLinks.push({
        erpId:          cxc.erpId,
        // calcularMontoPago usa el total del movimiento ABO/CBT/CPF/CFC,
        // que es el monto realmente depositado — no el saldo pendiente ni
        // el total de factura (ambos pueden ser incorrectos en CxC ya liquidadas).
        saldoActual:    calcularMontoPago(cxc),
        folioFiscal:    cxc.folioFiscal  ?? null,
        total:          cxc.total        ?? null,
        serie:          cxc.serie        ?? null,
        folioExterno:   cxc.folioExterno ?? null,
        tieneRetencion: false,
      });
      newIds.push(cxc.erpId);
    }

    const saldoErp = newLinks.reduce((s, l) => s + (l.saldoActual ?? 0), 0);
    // Marcar como identificado cuando la suma de CxC cubre O supera el depósito
    // (saldoErp >= deposito - $1). Cubre tres casos:
    //   · Cuadre exacto: saldoErp ≈ deposito (dentro de la tolerancia)
    //   · CxC mayor: saldoErp > deposito (diferencia positiva, pago por encima)
    //   · Descuento / retención: saldoErp < deposito por hasta $1 de tolerancia
    const newStatus = saldoErp >= (foundMov.deposito ?? 0) - TOLERANCIA_MXN
      ? 'identificado'
      : 'no_identificado';

    // Conservar identificaciones humanas previas; solo reemplazar las del motor
    const identificadoPorHumanos = (foundMov.identificadoPor ?? [])
      .filter(e => !MOTOR_USER_IDS.has(e.userId));

    ops.push({
      updateOne: {
        // Protección ACID: no sobreescribir si el estado en DB cambió mientras
        // procesábamos el batch (race condition con otros usuarios).
        filter: {
          _id:      foundMov._id,
          isActive: true,
          // Bloqueamos escritura si ya fue identificado manualmente después de
          // cargar los movimientos en el step 3. Solo se aplica si no tenía
          // identificación humana cuando lo cargamos.
          ...(!tieneIdentificacionHumana(foundMov)
            ? { 'identificadoPor.userId': { $nin: [...MOTOR_USER_IDS]
                  .filter(id => id !== MOTOR_ID) } }
            : {}),
        },
        update: {
          $set: {
            erpIds:   newIds,
            erpLinks: newLinks,
            saldoErp,
            status:   newStatus,
            identificadoPor: [
              ...identificadoPorHumanos,
              {
                userId:  usuarioId   ?? MOTOR_ID,
                nombre:  usuarioNombre ?? MOTOR_NOMBRE,
                fechaId: new Date(),
              },
            ],
          },
        },
      },
    });
  }

  // ── 5. Bulk write ───────────────────────────────────────────────────────────
  let escritos = 0;
  if (ops.length > 0) {
    // Detectar topología para decidir si usar transacción (igual que el motor ERP)
    const topologyType = mongoose.connection.client?.topology?.description?.type;
    const esReplicaSet = topologyType === 'ReplicaSetWithPrimary'
      || topologyType === 'ReplicaSetNoPrimary'
      || topologyType === 'Sharded';

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

  // ── 6. Retornar resumen ─────────────────────────────────────────────────────
  return {
    total:   excelRows.length,
    auto:    autoCount,
    review:  reviewCount,
    escritos,
    errors: {
      folioNoEncontrado,
      sinMovBancario,
    },
    detalleNoMatcheados,
  };
}

module.exports = { procesarRefacturacionesCyc };
