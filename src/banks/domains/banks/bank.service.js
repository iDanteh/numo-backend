'use strict';

const BankMovement      = require('./BankMovement.model');
const bankConfigRepo    = require('./repositories/bank-config.repository');
const Counter           = require('../../shared/models/Counter');
const CollectionRequest = require('../collection-requests/CollectionRequest.model');
const { parseBankFile, makeHash, TEMPLATE_SIGNATURE_SHEET, TEMPLATE_SIGNATURE_VALUE } = require('./bank.parser');
const ExcelJS = require('exceljs');
const { NotFoundError, BadRequestError, ConflictError, ForbiddenError } = require('../../shared/errors/AppError');
const { emitToUser, emitToBanco } = require('../../shared/socket');
const { matchRegla }   = require('./bank-rules.service');
const bankRuleRepo     = require('./repositories/bank-rule.repository');
// ── Constantes ────────────────────────────────────────────────────────────────

const BANCOS_VALIDOS = [
  'BBVA', 'Banamex', 'Santander', 'Azteca',
  'Banorte', 'HSBC', 'Inbursa', 'Scotiabank',
  'BanBajío', 'Afirme', 'Intercam', 'Nu',
  'Spin', 'Hey Banco', 'Albo',
];
const STATUS_VALIDOS = ['no_identificado', 'identificado', 'otros'];

const BANK_PREFIX = {
  bbva:       'BBVA',
  banamex:    'BNAM',
  santander:  'SANT',
  azteca:     'AZTC',
  banorte:    'BNRT',
  hsbc:       'HSBC',
  inbursa:    'INBR',
  scotiabank: 'SCOT',
  banbajío:   'BAJIO',
  afirme:     'AFRM',
  intercam:   'INTC',
  nu:         'NU',
  spin:       'SPIN',
  'hey banco':'HEY',
  albo:       'ALBO',
};

// ── Helpers ───────────────────────────────────────────────────────────────────

// BBVA exporta ciertos movimientos SPEI con el primer token tras '/' siendo una
// palabra clave de trazabilidad (BNET, REFBNTC) en lugar de un número real.
// Estos valores NO son números de autorización bancaria válidos y usarlos en
// dedup generaría falsos positivos entre movimientos distintos del mismo banco.
const BBVA_PSEUDO_AUTH_RE = /^(BNET|REFBNTC)$/i;

function isBBVAPseudoAuth(banco, auth) {
  return banco === 'BBVA' && !!auth && BBVA_PSEUDO_AUTH_RE.test(auth.trim());
}

// Compara dos números de autorización ignorando ceros iniciales en los numéricos
// y usando coincidencia exacta para los alfanuméricos.
// Evita el bug de parseInt("ALPHA", 10) === NaN donde NaN !== NaN es siempre true,
// lo que haría que auth alfanuméricos nunca matchearan en Capa 2.
function authMatch(a, b) {
  if (!a || !b) return false;
  const aIsNum = /^\d+$/.test(a);
  const bIsNum = /^\d+$/.test(b);
  if (aIsNum && bIsNum) return parseInt(a, 10) === parseInt(b, 10);
  return a === b; // alfanumérico: comparar como string (ya normalizados por normalizeAuthNum)
}

// Construye el objeto $set de enriquecimiento para un soft-dup:
// solo propaga campos que el existente (cand) no tiene y el entrante (inc) sí.
// No sobreescribe valores ya presentes — evita regresiones accidentales.
function buildSoftEnrich(inc, cand) {
  const enrich = {};
  const existingAuthIsPseudo = isBBVAPseudoAuth(cand.banco, cand.numeroAutorizacion);
  if (
    inc.numeroAutorizacion &&
    !isBBVAPseudoAuth(inc.banco, inc.numeroAutorizacion) &&
    (!cand.numeroAutorizacion || existingAuthIsPseudo)
  ) {
    enrich.numeroAutorizacion = inc.numeroAutorizacion;
  }
  if (inc.referenciaNumerica && !cand.referenciaNumerica) {
    enrich.referenciaNumerica = inc.referenciaNumerica;
  }
  return Object.keys(enrich).length > 0 ? enrich : null;
}

function generarFolio(seq) {
  const longitudBase = 6;
  const longitudSeq = seq.toString().length;
  const longitud = Math.max(longitudBase, longitudSeq);
  return seq.toString().padStart(longitud, '0');
}

// ── Service ───────────────────────────────────────────────────────────────────

async function getCards() {
  // Agregación MongoDB: estadísticas de movimientos por banco.
  // BankConfig ya no está en MongoDB → el $lookup se eliminó.
  // El join con la configuración se hace en la capa de aplicación.
  const [agg, configMap] = await Promise.all([
    BankMovement.aggregate([
      { $match: { isActive: true } },
      { $sort:  { banco: 1, fecha: 1, _id: 1 } },
      {
        $group: {
          _id:            '$banco',
          movimientos:    { $sum: 1 },
          movimientoNoIdentificado: {
            $sum: {
              $cond: [
                { $and: [
                  { $in: ['$status', ['no_identificado', null]] },
                  { $gt: [{ $ifNull: ['$deposito', 0] }, 0] },
                ]},
                1,
                0,
              ],
            },
          },
          totalDepositos: { $sum: { $ifNull: ['$deposito', 0] } },
          totalRetiros:   { $sum: { $ifNull: ['$retiro',   0] } },
          ultimaFecha:    { $max: '$fecha' },
          ultimaImport:   { $max: '$createdAt' },
          saldoFinal:     { $last: '$saldo' },
          no_identificado: { $sum: { $cond: [{ $in: ['$status', ['no_identificado', null]] }, 1, 0] } },
          identificado:    { $sum: { $cond: [{ $eq:  ['$status', 'identificado'] }, 1, 0] } },
          otros:           { $sum: { $cond: [{ $eq:  ['$status', 'otros'] }, 1, 0] } },
          saldoPendiente:  {
            $sum: {
              $cond: [
                { $in: ['$status', ['no_identificado', null]] },
                { $ifNull: ['$deposito', 0] },
                0,
              ],
            },
          },
          saldoIdentificado: {
            $sum: {
              $cond: [
                { $eq: ['$status', 'identificado'] },
                { $subtract: [{ $ifNull: ['$deposito', 0] }, { $ifNull: ['$retiro', 0] }] },
                0,
              ],
            },
          },
          saldoOtros: {
            $sum: {
              $cond: [
                { $eq: ['$status', 'otros'] },
                { $subtract: [{ $ifNull: ['$deposito', 0] }, { $ifNull: ['$retiro', 0] }] },
                0,
              ],
            },
          },
        },
      },
      { $sort: { _id: 1 } },
    ]),
    // Join en aplicación: config proviene de PostgreSQL
    bankConfigRepo.findAllAsMap(),
  ]);

  // ── Saldo actualizado por banco ────────────────────────────────────────────
  // Para cada banco que tiene saldo inicial definido, acumulamos el delta de
  // movimientos importados DESPUÉS del corte (createdAt > saldoInicialFechaCorte).
  // Se ejecutan en paralelo — el número de bancos con saldo inicial es pequeño.
  const banksWithSaldo = [...configMap.entries()].filter(
    ([, cfg]) => cfg.saldoInicial != null && cfg.saldoInicialFechaCorte != null,
  );

  const saldoActualizadoMap = {};
  if (banksWithSaldo.length > 0) {
    const deltaResults = await Promise.all(
      banksWithSaldo.map(async ([banco, cfg]) => {
        const [res] = await BankMovement.aggregate([
          { $match: { banco, isActive: true, createdAt: { $gt: cfg.saldoInicialFechaCorte } } },
          {
            $group: {
              _id:   null,
              delta: {
                $sum: {
                  $subtract: [{ $ifNull: ['$deposito', 0] }, { $ifNull: ['$retiro', 0] }],
                },
              },
            },
          },
        ]);
        return [banco, Number(cfg.saldoInicial) + (res?.delta ?? 0)];
      }),
    );
    for (const [banco, saldo] of deltaResults) {
      saldoActualizadoMap[banco] = saldo;
    }
  }

  return agg.map((b) => {
    const cfg = configMap.get(b._id);
    return {
      banco:          b._id,
      movimientos:    b.movimientos,
      movimientoNoIdentificado: b.movimientoNoIdentificado,
      totalDepositos: b.totalDepositos,
      totalRetiros:   b.totalRetiros,
      saldoFinal:     b.saldoFinal ?? null,
      ultimaFecha:    b.ultimaFecha,
      ultimaImport:   b.ultimaImport,
      cuentaContable: cfg?.cuentaContable ?? null,
      numeroCuenta:   cfg?.numeroCuenta   ?? null,
      saldoInicial:            cfg?.saldoInicial            != null ? Number(cfg.saldoInicial) : null,
      saldoInicialFechaCorte:  cfg?.saldoInicialFechaCorte  ?? null,
      saldoPendiente:    b.saldoPendiente    ?? 0,
      saldoIdentificado: b.saldoIdentificado ?? 0,
      saldoOtros:        b.saldoOtros        ?? 0,
      saldoActualizado:  saldoActualizadoMap[b._id] ?? null,
      lastImportBy:      cfg?.lastImportBy  ?? null,
      lastImportAt:      cfg?.lastImportAt  ?? null,
      porStatus: {
        no_identificado: b.no_identificado,
        identificado:    b.identificado,
        otros:           b.otros,
      },
    };
  });
}

async function listMovements(filters) {
  const {
    page = 1, limit = 50,
    banco, fechaInicio, fechaFin,
    tipo, search, concepto,
    sortBy = 'fecha', sortDir = 'desc',
    status, categorias, identificadoPor,
    identificadoPorUsuario,
    movId,
  } = filters;

  const filter = { isActive: true, oculto: { $ne: true } };
  if (banco)  filter.banco  = banco;
  if (status) filter.status = status;
  // Filtro por ID exacto (usado desde OCR para saltar a un movimiento específico)
  if (movId)  filter._id   = movId;

  if (categorias) {
    // Comma-separated list; __null__ represents null (sin categoría)
    const vals = categorias.split(',').map(v => v === '__null__' ? null : v);
    filter.categoria = { $in: vals };
  }
  if (tipo === 'deposito') filter.deposito = { $gt: 0 };
  if (tipo === 'retiro')   filter.retiro   = { $gt: 0 };

  if (concepto) {
    const esc = concepto.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    filter.concepto = new RegExp(esc, 'i');
  }

  if (identificadoPor) {
    const ids = identificadoPor.split(',').map(s => s.trim()).filter(Boolean);
    filter.$and = filter.$and ?? [];
    // Un movimiento puede haber sido identificado via CxC (identificadoPor[].userId)
    // o via ficha bancaria (fichaBy). Ambos caminos se incluyen en el filtro.
    filter.$and.push({ $or: [
      { 'identificadoPor.userId': { $in: ids } },
      { fichaBy: { $in: ids } },
    ]});
  }

  // Restricción de cobranza: solo sus propios movimientos identificados
  if (identificadoPorUsuario) {
    filter.$and = filter.$and ?? [];
    filter.$and.push({ 'identificadoPor.userId': identificadoPorUsuario });
  }

  if (fechaInicio || fechaFin) {
    filter.fecha = {};
    if (fechaInicio) filter.fecha.$gte = new Date(fechaInicio);
    if (fechaFin)    filter.fecha.$lte = new Date(`${fechaFin}T23:59:59.999Z`);
  }

  if (search) {
    const escaped = search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re       = new RegExp(escaped, 'i');
    const orClauses = [
      { concepto: re }, { numeroAutorizacion: re },
      { referenciaNumerica: re }, { folio: re }, { uuidXML: re },
    ];

    // Búsqueda por monto — tolerancia basada en los decimales ingresados:
    // sin decimales → rango de 1 peso completo; 1 decimal → ±0.05; 2 decimales → ±0.005
    const cleanNum = search.replace(/[$,\s]/g, '');
    const num = parseFloat(cleanNum);
    if (!isNaN(num) && num > 0) {
      const decimalPlaces = (cleanNum.split('.')[1] || '').length;
      const tolerance = decimalPlaces === 0 ? 1 : decimalPlaces === 1 ? 0.05 : 0.005;
      const lo = decimalPlaces === 0 ? num : num - tolerance;
      const hi = decimalPlaces === 0 ? num + tolerance : num + tolerance;
      orClauses.push({ deposito: { $gte: lo, $lt: hi } });
      orClauses.push({ retiro:   { $gte: lo, $lt: hi } });
    }

    filter.$or = orClauses;
  }

  const SORTABLE   = ['fecha', 'banco', 'deposito', 'retiro', 'saldo', 'saldo-erp', 'diferencia'];
  const rawSortBy  = SORTABLE.includes(sortBy) ? sortBy : 'fecha';
  const FIELD_MAP  = { 'saldo-erp': 'saldoErp' };
  const sortField  = FIELD_MAP[rawSortBy] ?? rawSortBy;
  const sortOrder  = sortDir === 'asc' ? 1 : -1;
  const skip       = (parseInt(page) - 1) * parseInt(limit);

  let movementsQuery;
  if (rawSortBy === 'diferencia') {
    movementsQuery = BankMovement.aggregate([
      { $match: filter },
      { $addFields: { _diferencia: { $subtract: [
        { $add: [{ $ifNull: ['$deposito', 0] }, { $ifNull: ['$retiro', 0] }] },
        { $ifNull: ['$saldoErp', 0] },
      ] } } },
      { $sort: { _diferencia: sortOrder, _id: 1 } },
      { $skip: skip },
      { $limit: parseInt(limit) },
    ]);
  } else {
    movementsQuery = BankMovement.find(filter)
      .sort({ [sortField]: sortOrder, _id: 1 })
      .skip(skip)
      .limit(parseInt(limit))
      .lean();
  }

  const [movements, total] = await Promise.all([
    movementsQuery,
    BankMovement.countDocuments(filter),
  ]);

  // Enriquecer con solicitudes de cobro confirmadas vinculadas a cada movimiento
  const movIds = movements.map(m => m._id);
  const solicitudes = await CollectionRequest.find(
    { bankMovementId: { $in: movIds }, status: 'confirmado' },
    'bankMovementId monto clienteNombre clienteRFC confirmadoAt',
  ).lean();

  const solicitudesPorMov = {};
  for (const s of solicitudes) {
    const key = s.bankMovementId.toString();
    if (!solicitudesPorMov[key]) solicitudesPorMov[key] = [];
    solicitudesPorMov[key].push({
      _id:           s._id,
      monto:         s.monto,
      clienteNombre: s.clienteNombre,
      clienteRFC:    s.clienteRFC,
      confirmadoAt:  s.confirmadoAt,
    });
  }

  // ── Saldo calculado ────────────────────────────────────────────────────────
  // Solo aplica cuando el banco está filtrado y tiene saldo inicial registrado.
  const saldoMap = {};
  if (banco) {
    const cfg = await bankConfigRepo.findByBanco(banco);
    if (cfg?.saldoInicial != null && cfg?.saldoInicialFechaCorte) {
      // Traer todos los movimientos posteriores al corte, en orden cronológico.
      // Solo se usan deposito/retiro para el cálculo acumulado.
      const allMovs = await BankMovement.find(
        { banco, isActive: true, createdAt: { $gt: cfg.saldoInicialFechaCorte } },
        { deposito: 1, retiro: 1 },
      ).sort({ fecha: 1, _id: 1 }).lean();

      let saldo = Number(cfg.saldoInicial);
      for (const m of allMovs) {
        saldo += (m.deposito ?? 0) - (m.retiro ?? 0);
        saldoMap[m._id.toString()] = saldo;
      }
    }
  }

  const data = movements.map(m => ({
    ...m,
    saldoCalculado: saldoMap[m._id.toString()] ?? null,
    solicitudesConfirmadas: solicitudesPorMov[m._id.toString()] ?? [],
  }));

  return {
    data,
    pagination: {
      total,
      page:  parseInt(page),
      limit: parseInt(limit),
      pages: Math.ceil(total / parseInt(limit)),
    },
  };
}

async function getSummary(fechaInicio, fechaFin) {
  const match = { isActive: true };
  if (fechaInicio || fechaFin) {
    match.fecha = {};
    if (fechaInicio) match.fecha.$gte = new Date(fechaInicio);
    if (fechaFin)    match.fecha.$lte = new Date(fechaFin);
  }
  return BankMovement.aggregate([
    { $match: match },
    {
      $group: {
        _id:            '$banco',
        totalDepositos: { $sum: { $ifNull: ['$deposito', 0] } },
        totalRetiros:   { $sum: { $ifNull: ['$retiro',   0] } },
        movimientos:    { $sum: 1 },
        saldoFinal:     { $last: '$saldo' },
      },
    },
    { $sort: { _id: 1 } },
  ]);
}

async function importFile(buffer, banco, userId, { auth0Sub, nombre } = {}) {
  const bancoValidado = BANCOS_VALIDOS.includes(banco) ? banco : null;
  const { movements, sinFecha, summary, errors } = await parseBankFile(buffer, bancoValidado);
  const sinFechaMovs = sinFecha || [];

  if (!movements.length && errors.length) {
    const err = new Error('No se pudo procesar ninguna hoja del archivo');
    err.statusCode = 422;
    err.errors = errors;
    throw err;
  }

  // ── 1. Detectar duplicados ANTES de reservar secuenciales ─────────────────
  const hashes = movements.map(m => m.hash);
  const existentes = await BankMovement.find(
    { hash: { $in: hashes } },
    '_id hash banco numeroAutorizacion referenciaNumerica',
  ).lean();

  const hashesExistentes = new Set();
  // enrichmentUpdates: movimientos ya en DB que se pueden enriquecer con datos
  // que trae el reimport (ej. numeroAutorizacion que antes era null).
  // Se aplican después de la inserción de nuevos, con $set selectivo.
  const enrichmentUpdates = []; // [{ _id, $set: {...} }]

  const incomingByHash = new Map(movements.map(m => [m.hash, m]));
  for (const ex of existentes) {
    hashesExistentes.add(ex.hash);
    const inc = incomingByHash.get(ex.hash);
    if (!inc) continue;

    const enrich = {};
    // Enriquecer numeroAutorizacion si el existente no la tiene (o tiene pseudo-auth)
    // y el reimport trae un valor real (no pseudo-auth BBVA).
    const existingAuthIsPseudo = isBBVAPseudoAuth(ex.banco, ex.numeroAutorizacion);
    if (
      inc.numeroAutorizacion &&
      !isBBVAPseudoAuth(inc.banco, inc.numeroAutorizacion) &&
      (!ex.numeroAutorizacion || existingAuthIsPseudo)
    ) {
      enrich.numeroAutorizacion = inc.numeroAutorizacion;
    }
    if (inc.referenciaNumerica && !ex.referenciaNumerica) {
      enrich.referenciaNumerica = inc.referenciaNumerica;
    }
    if (Object.keys(enrich).length > 0) {
      enrichmentUpdates.push({ _id: ex._id, $set: enrich });
    }
  }

  // ── 1b. Deduplicar por numeroAutorizacion (todos los bancos) ─────────────
  // Si un movimiento con el mismo número de autorización ya existe en el mismo
  // banco, se considera duplicado aunque el hash difiera (ej. fecha cambiada).
  // Aplica a Banamex (sub-fila "No. de Autorización"), BBVA (token numérico tras '/'),
  // Santander (col 8) y Azteca (col 7).
  //
  // BBVA excepción: si numeroAutorizacion es un pseudo-valor (BNET, REFBNTC),
  // se excluye de esta capa — no es un identificador estable y generaría falsos
  // positivos entre movimientos distintos con el mismo token.
  //
  // Manejo de ceros iniciales: Banamex puede exportar "199480" (fin de semana)
  // y "00199480" (estado del martes) para el mismo movimiento. Se normalizan
  // al parsear, pero registros históricos en DB pueden tener la forma sin
  // normalizar. La query incluye variantes con padding de ceros y la
  // comparación final es numérica (parseInt) + coincidencia de importe.
  const fechaUpdates = [];   // { _id, fecha }
  const authMovs = movements.filter(
    m => m.numeroAutorizacion && !isBBVAPseudoAuth(m.banco, m.numeroAutorizacion),
  );

  if (authMovs.length > 0) {
    // Para auth numbers puramente numéricos usar regex ^0*{n}$ que detecta
    // variantes con cualquier número de ceros iniciales ("67446012" ↔ "0067446012").
    // Para auth numbers alfanuméricos usar match exacto (la regex no aplica
    // y podría generar expresiones inválidas si contienen chars especiales).
    const uniqueAuthNums = [...new Set(authMovs.map(m => m.numeroAutorizacion))];
    const authConditions  = uniqueAuthNums.map(n =>
      /^\d+$/.test(n)
        ? { numeroAutorizacion: { $regex: `^0*${n}$` } }
        : { numeroAutorizacion: n },
    );
    const bancosAuth = [...new Set(authMovs.map(m => m.banco))];

    const existByAuth = await BankMovement.find(
      { banco: { $in: bancosAuth }, $or: authConditions },
      '_id banco numeroAutorizacion referenciaNumerica fecha deposito retiro',
    ).lean();

    for (const existing of existByAuth) {
      // authMatch compara numérico (entero, ignora ceros iniciales) o alfanumérico
      // (string exacto). parseInt fallback generaba NaN !== NaN → match imposible.
      const incoming = authMovs.find((m) => {
        if (m.banco !== existing.banco) return false;
        if (!authMatch(m.numeroAutorizacion, existing.numeroAutorizacion)) return false;
        const montoOk =
          (m.deposito != null && existing.deposito != null && Math.abs(m.deposito - existing.deposito) < 0.01) ||
          (m.retiro   != null && existing.retiro   != null && Math.abs(m.retiro   - existing.retiro  ) < 0.01);
        return montoOk;
      });
      if (!incoming) continue;
      // Programar actualización de fecha si cambió
      const existingFecha = new Date(existing.fecha).getTime();
      const incomingFecha = new Date(incoming.fecha).getTime();
      if (existingFecha !== incomingFecha) {
        fechaUpdates.push({ _id: existing._id, fecha: incoming.fecha });
      }
      // Enriquecer referenciaNumerica si el reimport la trae y el existente no la tiene
      if (incoming.referenciaNumerica && !existing.referenciaNumerica) {
        enrichmentUpdates.push({ _id: existing._id, $set: { referenciaNumerica: incoming.referenciaNumerica } });
      }
      // Marcar como ya existente para que no se re-inserte
      hashesExistentes.add(incoming.hash);
    }
  }

  // ── 1c. Deduplicar por referenciaNumerica (Banamex principalmente) ───────────
  // La referencia numérica es el identificador de operación que Banamex asigna
  // a cada transacción. Dos exports distintos del mismo movimiento pueden traer
  // distinto saldo corriente (o autorización vacía vs. con valor), generando
  // hashes y auth-numbers diferentes, pero la referencia numérica es siempre
  // la misma. Si coinciden banco + referenciaNumerica + importe → duplicado.
  const refNumMovs = movements.filter(m => m.referenciaNumerica && !hashesExistentes.has(m.hash));

  if (refNumMovs.length > 0) {
    const uniqueRefNums   = [...new Set(refNumMovs.map(m => m.referenciaNumerica))];
    const refConditions   = uniqueRefNums.map(n =>
      /^\d+$/.test(n)
        ? { referenciaNumerica: { $regex: `^0*${n}$` } }
        : { referenciaNumerica: n },
    );
    const bancosRef = [...new Set(refNumMovs.map(m => m.banco))];

    const existByRef = await BankMovement.find(
      { banco: { $in: bancosRef }, $or: refConditions },
      '_id banco referenciaNumerica numeroAutorizacion deposito retiro',
    ).lean();

    for (const existing of existByRef) {
      if (!existing.referenciaNumerica) continue;
      const incoming = refNumMovs.find((m) => {
        if (m.banco !== existing.banco) return false;
        if (!authMatch(m.referenciaNumerica, existing.referenciaNumerica)) return false;
        const montoOk =
          (m.deposito != null && existing.deposito != null && Math.abs(m.deposito - existing.deposito) < 0.01) ||
          (m.retiro   != null && existing.retiro   != null && Math.abs(m.retiro   - existing.retiro  ) < 0.01);
        return montoOk;
      });
      if (!incoming) continue;
      // Enriquecer numeroAutorizacion si el reimport la trae y el existente no la tiene
      const existingAuthIsPseudo = isBBVAPseudoAuth(existing.banco, existing.numeroAutorizacion);
      if (
        incoming.numeroAutorizacion &&
        !isBBVAPseudoAuth(incoming.banco, incoming.numeroAutorizacion) &&
        (!existing.numeroAutorizacion || existingAuthIsPseudo)
      ) {
        enrichmentUpdates.push({ _id: existing._id, $set: { numeroAutorizacion: incoming.numeroAutorizacion } });
      }
      hashesExistentes.add(incoming.hash);
    }
  }

  // ── 1d. Soft dedup: same banco+fecha+importe+saldo + concept prefix match ─
  // Catches cases where the same movement was imported previously with a slightly
  // different concept (e.g. Banamex with/without authorization sub-rows).
  const aun_sin_dedup = movements.filter(m => !hashesExistentes.has(m.hash));
  let softDuplicados = 0;

  if (aun_sin_dedup.length > 0) {
    // Collect unique banco+fecha combos for a single batch query
    const fechaBancoMap = new Map();
    for (const m of aun_sin_dedup) {
      if (!m.fecha) continue;
      const key = `${m.banco}|${new Date(m.fecha).toISOString().slice(0, 10)}`;
      if (!fechaBancoMap.has(key)) {
        const fechaStart = new Date(m.fecha);
        fechaStart.setUTCHours(0, 0, 0, 0);
        const fechaEnd = new Date(fechaStart);
        fechaEnd.setUTCHours(23, 59, 59, 999);
        fechaBancoMap.set(key, { banco: m.banco, fechaStart, fechaEnd });
      }
    }

    if (fechaBancoMap.size > 0) {
      const orConds = [...fechaBancoMap.values()].map(({ banco, fechaStart, fechaEnd }) => ({
        banco, fecha: { $gte: fechaStart, $lte: fechaEnd },
      }));
      const dbCands = await BankMovement.find(
        { $or: orConds },
        '_id banco fecha deposito retiro saldo concepto numeroAutorizacion referenciaNumerica',
      ).lean();

      // Group DB candidates by banco+fecha key
      const candsByKey = new Map();
      for (const c of dbCands) {
        const key = `${c.banco}|${new Date(c.fecha).toISOString().slice(0, 10)}`;
        if (!candsByKey.has(key)) candsByKey.set(key, []);
        candsByKey.get(key).push(c);
      }

      for (const m of aun_sin_dedup) {
        if (!m.fecha) continue;
        const key = `${m.banco}|${new Date(m.fecha).toISOString().slice(0, 10)}`;
        const cands = candsByKey.get(key) || [];
        for (const cand of cands) {
          // 1. El importe debe coincidir exactamente (±0.01)
          const amountOk =
            (m.deposito != null && cand.deposito != null && Math.abs(m.deposito - cand.deposito) < 0.01) ||
            (m.retiro   != null && cand.retiro   != null && Math.abs(m.retiro   - cand.retiro  ) < 0.01);
          if (!amountOk) continue;

          // 2a. BBVA: comparar número BNET incrustado en el concepto.
          // BBVA exporta transferencias SPEI con el número de trazabilidad BNET
          // dentro del concepto (ej. "PAGO / BNET 0476156782 ...").  El mismo
          // movimiento puede aparecer con o sin el número de autorización antes
          // del token BNET, y con saldos distintos si proviene de extractos de
          // distintos períodos.  El número BNET es único por transferencia y es
          // el identificador estable para este caso.
          if (m.banco === 'BBVA') {
            const BNET_RE = /\bBNET\s+0*(\d+)/i;
            const bnetInc = ((m.concepto    || '').match(BNET_RE) || [])[1];
            const bnetCnd = ((cand.concepto || '').match(BNET_RE) || [])[1];
            if (bnetInc && bnetCnd && bnetInc === bnetCnd) {
              hashesExistentes.add(m.hash);
              softDuplicados++;
              // Enriquecer si el reimport trae datos que el existente no tiene
              const enrichBnet = buildSoftEnrich(m, cand);
              if (enrichBnet) enrichmentUpdates.push({ _id: cand._id, $set: enrichBnet });
              break;
            }
            // Si ninguno tiene número BNET, seguir con el check de saldo+concepto.
          }

          // 2b. El saldo debe coincidir exactamente (±0.01).
          // El saldo es el balance acumulado de la cuenta: dos movimientos distintos
          // en la misma cuenta nunca comparten el mismo saldo, por lo que este
          // criterio descarta falsos positivos de forma prácticamente infalible.
          const saldoOk = m.saldo != null && cand.saldo != null && Math.abs(m.saldo - cand.saldo) < 0.01;
          if (!saldoOk) continue;

          // 3. El concepto debe compartir un prefijo común de al menos 20 chars.
          // Cubre el caso donde un import trae el número de autorización incrustado
          // en el concepto y el otro no (ej. Banamex con/sin sub-filas).
          const cA = (m.concepto   || '').replace(/\s+/g, ' ').trim().toLowerCase();
          const cB = (cand.concepto || '').replace(/\s+/g, ' ').trim().toLowerCase();
          const minL = Math.min(cA.length, cB.length);
          if (minL >= 20 && cA.substring(0, minL) === cB.substring(0, minL)) {
            hashesExistentes.add(m.hash);
            softDuplicados++;
            // Enriquecer si el reimport trae datos que el existente no tiene
            const enrichSoft = buildSoftEnrich(m, cand);
            if (enrichSoft) enrichmentUpdates.push({ _id: cand._id, $set: enrichSoft });
            break;
          }
        }
      }
    }
  }

  const nuevos     = movements.filter(m => !hashesExistentes.has(m.hash));
  const duplicados = movements.length - nuevos.length;

  // ── 2. Reservar secuenciales solo para los movimientos nuevos ─────────────
  if (nuevos.length > 0) {
    const startSeq = await Counter.nextBatchSeq('bankMovement', nuevos.length);
    nuevos.forEach((m, i) => {
      m.folio = generarFolio(startSeq + i);
    });
  }

  // ── 3. Insertar solo los nuevos ───────────────────────────────────────────
  const BATCH = 500;
  let insertados = 0;
  const total = nuevos.length;

  for (let i = 0; i < nuevos.length; i += BATCH) {
    const batch = nuevos.slice(i, i + BATCH);
    const ops = batch.map((m) => ({
      updateOne: {
        filter: { hash: m.hash },
        update: { $setOnInsert: { ...m, categoria: null, uploadedBy: userId, isActive: true } },
        upsert: true,
      },
    }));
    try {
      const result = await BankMovement.bulkWrite(ops, { ordered: false });
      insertados += result.upsertedCount;
    } catch (err) {
      // BulkWriteError con ordered:false — algunos upserts pudieron completarse
      if (err.result) {
        insertados += err.result.nUpserted ?? 0;
      } else {
        // Error inesperado (ej. cast error en todos los docs) — relanzar para visibilidad
        throw err;
      }
    }

    // Emitir progreso al usuario que hizo la importación
    emitToUser(auth0Sub, 'bank:import:progress', {
      banco:      bancoValidado || banco,
      done:       Math.min(i + BATCH, total),
      total,
      importados: insertados,
      duplicados,
    });
  }

  // ── 3b. Actualizar fecha de movimientos Banamex deduplicados por auth ──────
  if (fechaUpdates.length > 0) {
    const fechaOps = fechaUpdates.map(({ _id, fecha }) => ({
      updateOne: { filter: { _id }, update: { $set: { fecha } } },
    }));
    await BankMovement.bulkWrite(fechaOps, { ordered: false });
  }

  // ── 3c. Enriquecer movimientos existentes con datos del reimport ──────────
  // Aplica $set selectivo (solo campos que el existente no tenía) sobre los
  // movimientos detectados como duplicados pero que ahora traen información
  // adicional: principalmente numeroAutorizacion y referenciaNumerica.
  // La salvaguarda de no-sobreescritura está en buildSoftEnrich / Capa 1:
  // solo se enriquece si el campo destino está vacío o era un pseudo-valor.
  let enriquecidos = 0;
  if (enrichmentUpdates.length > 0) {
    // Consolidar por _id: un mismo documento puede haber sido detectado en varias
    // capas (ej. Capa 1 por hash y Capa 2 por auth) generando entradas redundantes.
    // Fusionar los $set evita operaciones duplicadas y mantiene el conteo preciso.
    const enrichById = new Map();
    for (const { _id, $set } of enrichmentUpdates) {
      const key = String(_id);
      if (!enrichById.has(key)) {
        enrichById.set(key, { _id, $set: { ...$set } });
      } else {
        Object.assign(enrichById.get(key).$set, $set);
      }
    }
    const enrichOps = [...enrichById.values()].map(({ _id, $set }) => ({
      updateOne: { filter: { _id }, update: { $set } },
    }));
    try {
      const result = await BankMovement.bulkWrite(enrichOps, { ordered: false });
      enriquecidos = result.modifiedCount;
    } catch (err) {
      if (err.result) {
        enriquecidos = err.result.nModified ?? 0;
      } else {
        throw err;
      }
    }
  }

    // ── 4. Aplicar reglas a los movimientos recién insertados ─────────────────
    let categorizados  = 0;
    let sinReglasAviso = false;

    if (insertados > 0 && bancoValidado) {
      const [catRules, ocultarRules] = await Promise.all([
        bankRuleRepo.listByBanco(bancoValidado, { accion: 'categorizar' }),
        bankRuleRepo.listByBanco(bancoValidado, { accion: 'ocultar' }),
      ]);

      if (catRules.length === 0 && ocultarRules.length === 0) {
        sinReglasAviso = true;
      } else {
        const foliosNuevos   = nuevos.map(m => m.folio);
        const docsInsertados = await BankMovement.find(
          { folio: { $in: foliosNuevos }, isActive: true },
        ).lean();

        const ops = [];
        for (const mov of docsInsertados) {
          const $set = {};
          for (const rule of catRules) {
            if (matchRegla(mov, rule)) { $set.categoria = rule.nombre; break; }
          }
          if ($set.categoria) categorizados++;
          for (const rule of ocultarRules) {
            if (matchRegla(mov, rule)) { $set.oculto = true; break; }
          }
          if (Object.keys($set).length > 0) {
            ops.push({ updateOne: { filter: { _id: mov._id }, update: { $set } } });
          }
        }

        if (ops.length) {
          await BankMovement.bulkWrite(ops, { ordered: false });
        }
      }
    }

  // ── 5. Registrar última carga por banco ─────────────────────────────────
  if (insertados > 0 && nombre) {
    const bancosAfectados = [...new Set(nuevos.map(m => m.banco).filter(Boolean))];
    const ahora = new Date();
    await Promise.all(
      bancosAfectados.map(b =>
        bankConfigRepo.upsert(b, { lastImportBy: nombre, lastImportAt: ahora }),
      ),
    );
  }

    return {
      message:      `${insertados} movimientos importados, ${duplicados} ya existían, ${enriquecidos} enriquecidos`,
      importados:   insertados,
      duplicados,
      enriquecidos,
      softDuplicados,
      categorizados,
      sinReglas:    sinReglasAviso,
      resumen:      summary,
      erroresHojas: errors,
      sinFecha:     sinFechaMovs.map(m => ({
        banco:    m.banco,
        concepto: (m.concepto || '').substring(0, 100),
        deposito: m.deposito,
        retiro:   m.retiro,
      })),
    };
}

async function importIndividual(mov, banco, userId, { auth0Sub } = {}) {
  // ── 1. Validar banco ───────────────────────────────────────────────
  const bancoValidado = BANCOS_VALIDOS.includes(banco) ? banco : null;

  // ── 2. Validación básica del movimiento ────────────────────────────
  if (!mov.hash) {
    const err = new Error('El movimiento debe contener un hash');
    err.statusCode = 400;
    throw err;
  }

  // ── 3. Crear folio (secuencial) ────────────────────────────────────
  const seq   = await Counter.nextSeq('bankMovement');
  const folio = generarFolio(seq.seq);

  // ── 4. Construir documento ─────────────────────────────────────────
  const nuevo = new BankMovement({
    ...mov,
    banco: bancoValidado || banco,
    folio,
    categoria: null,
    uploadedBy: userId,
    isActive: true,
  });

  // ── 5. Guardar (controlando duplicados por índice único) ───────────
  try {
    await nuevo.save();
  } catch (err) {
    if (err.code === 11000) {
      const e = new Error('Movimiento ya existe');
      e.statusCode = 409;
      throw e;
    }
    throw err;
  }

  // ── 6. Aplicar reglas automáticas ──────────────────────────────────
  let categorizado = false;

  if (bancoValidado) {
    const [catRules, ocultarRules] = await Promise.all([
      bankRuleRepo.listByBanco(bancoValidado, { accion: 'categorizar' }),
      bankRuleRepo.listByBanco(bancoValidado, { accion: 'ocultar' }),
    ]);

    for (const rule of catRules) {
      if (matchRegla(nuevo, rule)) {
        nuevo.categoria = rule.nombre;
        categorizado = true;
        break;
      }
    }
    for (const rule of ocultarRules) {
      if (matchRegla(nuevo, rule)) { nuevo.oculto = true; break; }
    }
    if (categorizado || nuevo.oculto) await nuevo.save();
  }

  // ── 7. Emitir evento ───────────────────────────────────────────────
  if (auth0Sub) {
    emitToUser(auth0Sub, 'bank:import:individual', {
      banco: bancoValidado || banco,
      folio,
      categorizado,
    });
  }

  // ── 8. Respuesta ───────────────────────────────────────────────────
  return {
    message: 'Movimiento importado correctamente',
    movimiento: nuevo,
    categorizado,
  };
}

const ERP_TOLERANCE = 1.00; // $1 MXN de tolerancia para cuadre

// Calcula saldoErp, uuidXML y status a partir de erpLinks del movimiento.
// Para cada link:
//   - Si saldoActual > 0  → usar saldoActual (pago parcial pendiente en ERP)
//   - Si saldoActual es null o 0 → usar total del comprobante
//     (ERP marcó la CxC como cobrada o no devolvió saldo; se compara contra
//      el importe original para permitir la identificación manual o automática)
//
// Regla de identificación automática:
//   saldoErp >= bankAmount - ERP_TOLERANCE
//   Es decir: la CxC cubre o excede el depósito → identificado.
//   Si la CxC es MENOR que el depósito → no_identificado (pago insuficiente).
function aplicarLogicaErp(mov) {
  const links = mov.erpLinks || [];
  const saldoErp = links.length > 0
    ? links.reduce((sum, l) => {
        const ref = (l.saldoActual != null && l.saldoActual > 0)
          ? l.saldoActual
          : (l.total ?? 0);
        return sum + ref;
      }, 0)
    : null;
  const uuidXML    = links.find(l => l.folioFiscal)?.folioFiscal?.toUpperCase() ?? null;
  const bankAmount = Math.abs(mov.deposito ?? mov.retiro ?? 0);
  let status       = (saldoErp !== null && saldoErp >= bankAmount - ERP_TOLERANCE)
    ? 'identificado'
    : 'no_identificado';
  // Si el movimiento ya tiene ficha registrada, siempre queda identificado
  if (mov.ficha && status === 'no_identificado') {
    status = 'identificado';
  }
  return { saldoErp, uuidXML, status };
}

async function updateStatus(id, status, user) {
  if (!STATUS_VALIDOS.includes(status)) {
    throw new BadRequestError(`Status inválido. Debe ser: ${STATUS_VALIDOS.join(', ')}`);
  }
  const mov = await BankMovement.findById(id);
  if (!mov) throw new NotFoundError('Movimiento');
  const isAdmin = user?.role === 'admin';
  // Bloquear si el cuadre ERP determinó automáticamente el status (admin puede forzar).
  // La CxC cubre el depósito (saldoErp >= bankAmount - tolerancia) → bloqueado para no-admin.
  const bankAmount = Math.abs(mov.deposito ?? mov.retiro ?? 0);
  if (!isAdmin && mov.saldoErp !== null && mov.saldoErp >= bankAmount - ERP_TOLERANCE) {
    throw new ConflictError('Movimiento bloqueado: el saldo ERP cuadra con el monto bancario');
  }
  // Bloquear si el movimiento fue identificado por otro usuario (admin puede forzar)
  const idPorEntries = mov.identificadoPor ?? [];
  if (
    !isAdmin &&
    mov.status === 'identificado' &&
    idPorEntries.length > 0 &&
    !idPorEntries.some(e => e.userId === user?._id)
  ) {
    throw new ConflictError('Movimiento bloqueado: fue identificado por otro usuario');
  }
  // Solo admin puede transicionar de 'no_identificado' a 'otros'
  if (status === 'otros' && mov.status === 'no_identificado' && !isAdmin) {
    throw new ForbiddenError('Solo un administrador puede marcar este movimiento como "otros"');
  }
  // Para marcar como identificado siempre se requiere al menos un ID ERP asociado
  if (status === 'identificado' && (!mov.erpIds || mov.erpIds.length === 0)) {
    throw new BadRequestError('Para identificar un movimiento debe tener al menos un ID ERP asociado');
  }
  // Verificar reglas de bloqueo de identificación (los admins pueden forzar)
  if (status === 'identificado' && !isAdmin) {
    const blockRules = await bankRuleRepo.findBlockingRules(mov.banco);
    for (const rule of blockRules) {
      if (matchRegla(mov, rule)) {
        const msg = rule.mensajeBloqueo
          || `La regla "${rule.nombre}" impide identificar este movimiento`;
        throw new ForbiddenError(msg);
      }
    }
  }
  mov.status = status;
  // identificadoPor es gestionado exclusivamente al vincular/desvincular CxCs — no se toca aquí
  await mov.save();

  const updated = { _id: mov._id, banco: mov.banco, status: mov.status, identificadoPor: mov.identificadoPor };
  emitToBanco(mov.banco, 'bank:movement:updated', updated);

  return updated;
}

async function updateErpIds(id, action, erpId, user) {
  if (action !== 'remove') throw new BadRequestError('Solo se acepta action "remove"');
  if (!erpId || typeof erpId !== 'string' || !erpId.trim()) {
    throw new BadRequestError('erpId inválido o vacío');
  }
  const cleanId = erpId.trim();
  const mov = await BankMovement.findById(id);
  if (!mov) throw new NotFoundError('Movimiento');
  const idPorEntries = mov.identificadoPor ?? [];
  if (
    user?.role !== 'admin' &&
    mov.status === 'identificado' &&
    idPorEntries.length > 0 &&
    !idPorEntries.some(e => e.userId === user?._id)
  ) {
    throw new ConflictError('Movimiento bloqueado: fue identificado por otro usuario');
  }

  mov.erpIds          = (mov.erpIds          || []).filter(x => x !== cleanId);
  mov.erpLinks        = (mov.erpLinks        || []).filter(l => l.erpId !== cleanId);
  // Eliminar las entradas de identificadoPor correspondientes a la CxC desvinculada.
  // Si ya no quedan CxCs vinculadas, limpiar por completo: cubre entradas sin erpId
  // (erpId: null) almacenadas por el motor automático, que el filtro exacto no elimina.
  if (mov.erpIds.length === 0) {
    mov.identificadoPor = [];
  } else {
    mov.identificadoPor = (mov.identificadoPor || []).filter(e => e.erpId !== cleanId);
  }

  const { saldoErp, uuidXML, status } = aplicarLogicaErp(mov);
  mov.saldoErp = saldoErp;
  mov.uuidXML  = uuidXML;
  mov.status   = status;
  await mov.save();

  const updated = {
    _id: mov._id, banco: mov.banco, erpIds: mov.erpIds, erpLinks: mov.erpLinks,
    saldoErp: mov.saldoErp, uuidXML: mov.uuidXML, status: mov.status, identificadoPor: mov.identificadoPor,
  };
  emitToBanco(mov.banco, 'bank:movement:updated', updated);

  return updated;
}

async function setErpIds(id, erpLinks, user) {
  if (!Array.isArray(erpLinks)) throw new BadRequestError('erpLinks debe ser un arreglo');

  const cleanLinks = erpLinks
    .map(l => ({
      erpId:        String(l.erpId || '').trim(),
      saldoActual:  l.saldoActual != null ? Number(l.saldoActual) : null,
      folioFiscal:  l.folioFiscal ? String(l.folioFiscal).trim().toUpperCase() : null,
      total:        l.total != null ? Number(l.total) : null,
      serie:        l.serie ? String(l.serie).trim() : null,
      folioExterno: l.folioExterno ? String(l.folioExterno).trim() : null,
    }))
    .filter(l => l.erpId);

  const mov = await BankMovement.findById(id);
  if (!mov) throw new NotFoundError('Movimiento');
  const idPorSet = mov.identificadoPor ?? [];
  if (
    user?.role !== 'admin' &&
    mov.status === 'identificado' &&
    idPorSet.length > 0 &&
    !idPorSet.some(e => e.userId === user?._id)
  ) {
    throw new ConflictError('Movimiento bloqueado: fue identificado por otro usuario');
  }

  mov.erpLinks = cleanLinks;
  mov.erpIds   = cleanLinks.map(l => l.erpId);

  // Actualizar identificadoPor: añadir entradas para CxCs nuevas, quitar las eliminadas.
  // También eliminar entradas de 'erp-auto' (sin erpId): cuando un humano toma posesión
  // manual de los links, el motor ya no es dueño del registro — si quedara la entrada de
  // erp-auto, el Revertir ERP podría borrar los links del humano.
  const prevIds       = new Set((mov.identificadoPor || []).map(e => e.erpId));
  const newIds        = new Set(cleanLinks.map(l => l.erpId));
  const displayName   = user?.nombre || user?.email || null;
  const addedErpIds   = cleanLinks.filter(l => !prevIds.has(l.erpId)).map(l => l.erpId);
  const removedErpIds = [...prevIds].filter(id => !newIds.has(id));

  let updatedIdPor = (mov.identificadoPor || [])
    .filter(e => e.userId !== 'erp-auto')          // ← ceder ownership al humano
    .filter(e => !removedErpIds.includes(e.erpId));
  for (const erpId of addedErpIds) {
    updatedIdPor.push({ userId: user?._id ?? null, nombre: displayName, fechaId: new Date(), erpId });
  }
  mov.identificadoPor = updatedIdPor;

  const { saldoErp, uuidXML, status } = aplicarLogicaErp(mov);
  mov.saldoErp = saldoErp;
  mov.uuidXML  = uuidXML;
  mov.status   = status;
  await mov.save();

  const updated = {
    _id: mov._id, banco: mov.banco, erpIds: mov.erpIds, erpLinks: mov.erpLinks,
    saldoErp: mov.saldoErp, uuidXML: mov.uuidXML, status: mov.status,
    identificadoPor: mov.identificadoPor,
  };
  emitToBanco(mov.banco, 'bank:movement:updated', updated);

  return updated;
}

async function getConfig(banco) {
  const cfg = await bankConfigRepo.findByBanco(banco);
  return cfg ?? { banco, cuentaContable: null, numeroCuenta: null };
}

async function saveConfig(banco, data) {
  if (!BANCOS_VALIDOS.includes(banco)) throw new BadRequestError('Banco inválido');
  const fields = {};
  if (data.cuentaContable !== undefined) fields.cuentaContable = data.cuentaContable || null;
  if (data.numeroCuenta   !== undefined) fields.numeroCuenta   = data.numeroCuenta   || null;
  return bankConfigRepo.upsert(banco, fields);
}

async function setSaldoInicial(banco, monto) {
  if (!BANCOS_VALIDOS.includes(banco)) throw new BadRequestError('Banco inválido');
  if (isNaN(monto) || monto < 0) throw new BadRequestError('Monto inválido');
  return bankConfigRepo.setSaldoInicial(banco, monto);
}

async function listIdentificadores(banco) {
  // banco: string opcional con uno o varios bancos separados por coma.
  // Sin banco → consulta en todos los bancos activos.
  const baseMatch = { isActive: true };
  if (banco) {
    const vals = banco.split(',').map(v => v.trim()).filter(Boolean);
    baseMatch.banco = vals.length === 1 ? vals[0] : { $in: vals };
  }

  // Dos fuentes de identificación:
  //   1. Vía CxC/ERP  → array identificadoPor[].userId / .nombre
  //   2. Vía ficha bancaria → campos fichaBy / fichaNombre
  // Ambas se consolidan y deduplicadas por userId antes de devolver.
  const [porErp, porFicha] = await Promise.all([
    BankMovement.aggregate([
      { $match: { ...baseMatch, 'identificadoPor.0': { $exists: true } } },
      { $unwind: '$identificadoPor' },
      { $match: { 'identificadoPor.userId': { $ne: null } } },
      { $group: { _id: '$identificadoPor.userId', nombre: { $first: '$identificadoPor.nombre' } } },
    ]),
    BankMovement.aggregate([
      { $match: { ...baseMatch, fichaBy: { $ne: null } } },
      { $group: { _id: '$fichaBy', nombre: { $first: '$fichaNombre' } } },
    ]),
  ]);

  // Fusionar deduplicando por userId (la primera fuente encontrada gana el nombre)
  const map = new Map();
  for (const d of [...porErp, ...porFicha]) {
    if (d._id && !map.has(d._id)) map.set(d._id, d.nombre);
  }

  return [...map.entries()]
    .map(([userId, nombre]) => ({ userId, nombre: nombre || userId }))
    .sort((a, b) => (a.nombre || '').localeCompare(b.nombre || ''));
}

async function listCategories(banco) {
  // banco: string opcional con uno o varios bancos separados por coma.
  const q = { isActive: true };
  if (banco) {
    const vals = banco.split(',').map(v => v.trim()).filter(Boolean);
    q.banco = vals.length === 1 ? vals[0] : { $in: vals };
  }
  const values = await BankMovement.distinct('categoria', q);
  // Sort: non-null first alphabetically, then null last
  return values.sort((a, b) => {
    if (a === null) return 1;
    if (b === null) return -1;
    return a.localeCompare(b);
  });
}

async function exportMovements(filters) {
  const {
    banco, fechaInicio, fechaFin,
    fechaAplicacionInicio, fechaAplicacionFin,
    tipo, search, concepto,
    sortBy = 'fecha', sortDir = 'desc',
    status, categorias,
    identificadoPor,        // comma-separated userIds (nombre correcto enviado por el frontend)
  } = filters;

  const filter = { isActive: true, oculto: { $ne: true } };
  if (banco) {
    const bancoVals = banco.split(',').map(v => v.trim()).filter(Boolean);
    filter.banco = bancoVals.length === 1 ? bancoVals[0] : { $in: bancoVals };
  }

  if (status) {
    const statusVals = status.split(',').map(v => v.trim()).filter(Boolean);
    if (statusVals.length === 1) filter.status = statusVals[0];
    else if (statusVals.length > 1) filter.status = { $in: statusVals };
  }

  // Filtro por identificador: cubre identificadoPor[].userId Y fichaBy
  // (ambas fuentes son las que usa listIdentificadores para poblar las opciones).
  if (identificadoPor) {
    const userIds = identificadoPor.split(',').map(v => v.trim()).filter(Boolean);
    if (userIds.length > 0) {
      const match = userIds.length === 1
        ? { $or: [{ 'identificadoPor.userId': userIds[0] }, { fichaBy: userIds[0] }] }
        : { $or: [{ 'identificadoPor.userId': { $in: userIds } }, { fichaBy: { $in: userIds } }] };
      filter.$and = filter.$and ?? [];
      filter.$and.push(match);
    }
  }

  if (categorias) {
    const vals = categorias.split(',').map(v => v === '__null__' ? null : v);
    filter.categoria = { $in: vals };
  }

  if (tipo) {
    const tipoVals = tipo.split(',').map(v => v.trim()).filter(Boolean);
    if (tipoVals.length === 1) {
      if (tipoVals[0] === 'deposito') filter.deposito = { $gt: 0 };
      if (tipoVals[0] === 'retiro')   filter.retiro   = { $gt: 0 };
    }
    // Si vienen ambos o ninguno → sin filtro de tipo
  }

  if (concepto) {
    const esc = concepto.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    filter.concepto = new RegExp(esc, 'i');
  }

  if (fechaInicio || fechaFin) {
    filter.fecha = {};
    if (fechaInicio) filter.fecha.$gte = new Date(fechaInicio);
    if (fechaFin)    filter.fecha.$lte = new Date(`${fechaFin}T23:59:59.999Z`);
  }

  // Filtro por fecha de aplicación: max(identificadoPor[].fechaId, fichaAt) en el rango.
  // Como es un campo calculado, se filtra buscando documentos donde ALGUNO de sus
  // campos de fecha de identificación caiga dentro del rango solicitado.
  if (fechaAplicacionInicio || fechaAplicacionFin) {
    const df = {};
    if (fechaAplicacionInicio) df.$gte = new Date(fechaAplicacionInicio);
    if (fechaAplicacionFin)    df.$lte = new Date(`${fechaAplicacionFin}T23:59:59.999Z`);
    filter.$and = filter.$and ?? [];
    filter.$and.push({ $or: [
      { 'identificadoPor.fechaId': df },
      { fichaAt: df },
    ]});
  }

  if (search) {
    const escaped = search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re       = new RegExp(escaped, 'i');
    const orClauses = [
      { concepto: re }, { numeroAutorizacion: re },
      { referenciaNumerica: re }, { folio: re }, { uuidXML: re },
    ];
    // Búsqueda por monto — tolerancia basada en los decimales ingresados:
    // sin decimales → rango de 1 peso completo; 1 decimal → ±0.05; 2 decimales → ±0.005
    const cleanNum = search.replace(/[$,\s]/g, '');
    const num = parseFloat(cleanNum);
    if (!isNaN(num) && num > 0) {
      const decimalPlaces = (cleanNum.split('.')[1] || '').length;
      const tolerance = decimalPlaces === 0 ? 1 : decimalPlaces === 1 ? 0.05 : 0.005;
      const lo = decimalPlaces === 0 ? num : num - tolerance;
      const hi = decimalPlaces === 0 ? num + tolerance : num + tolerance;
      orClauses.push({ deposito: { $gte: lo, $lt: hi } });
      orClauses.push({ retiro:   { $gte: lo, $lt: hi } });
    }
    filter.$or = orClauses;
  }

  const SORTABLE  = ['fecha', 'banco', 'deposito', 'retiro', 'saldo', 'saldo-erp', 'diferencia'];
  const rawSortBy = SORTABLE.includes(sortBy) ? sortBy : 'fecha';
  const FIELD_MAP = { 'saldo-erp': 'saldoErp' };
  const sortField = FIELD_MAP[rawSortBy] ?? rawSortBy;
  const sortOrder = sortDir === 'asc' ? 1 : -1;

  let movements;
  if (rawSortBy === 'diferencia') {
    movements = await BankMovement.aggregate([
      { $match: filter },
      { $addFields: { _diferencia: { $subtract: [
        { $add: [{ $ifNull: ['$deposito', 0] }, { $ifNull: ['$retiro', 0] }] },
        { $ifNull: ['$saldoErp', 0] },
      ] } } },
      { $sort: { _diferencia: sortOrder, _id: 1 } },
    ]);
  } else {
    movements = await BankMovement.find(filter)
      .sort({ [sortField]: sortOrder, _id: 1 })
      .lean();
  }

  const ExcelJS = require('exceljs');
  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet('Movimientos');

  sheet.columns = [
    { header: 'Fecha',            key: 'fecha',              width: 14 },
    { header: 'Folio',            key: 'folio',              width: 10 },
    { header: 'Banco',            key: 'banco',              width: 14 },
    { header: 'Concepto',         key: 'concepto',           width: 50 },
    { header: 'Fecha aplicación', key: 'fechaAplicacion',    width: 18 },
    { header: 'Depósito',         key: 'deposito',           width: 16 },
    { header: 'Retiro',           key: 'retiro',             width: 16 },
    { header: 'Serie-Folio / Ficha', key: 'erpIds',           width: 32 },
    { header: 'Saldo ERP',        key: 'saldoErp',           width: 16 },
    { header: 'Diferencia',       key: 'diferencia',         width: 16 },
    { header: 'Categoría',        key: 'categoria',          width: 20 },
    { header: 'Estado',           key: 'status',             width: 16 },
    { header: 'N° Autorización',  key: 'numeroAutorizacion', width: 20 },
    { header: 'Identificado por', key: 'identificadoPor',    width: 24 },
  ];

  const STATUS_LABELS = {
    no_identificado: 'No identificado',
    identificado:    'Identificado',
    otros:           'Otros',
  };

  const formatUTCDate = (raw) => {
    if (!raw) return null;
    const d = new Date(raw);
    return `${String(d.getUTCDate()).padStart(2, '0')}/${String(d.getUTCMonth() + 1).padStart(2, '0')}/${d.getUTCFullYear()}`;
  };

  for (const m of movements) {
    const bankAmount  = (m.deposito ?? 0) + (m.retiro ?? 0);
    const diferencia  = m.saldoErp != null ? Math.abs(bankAmount - m.saldoErp) : null;

    const fechasAplicacion = [
      ...(m.identificadoPor || []).map(e => e.fechaId ? new Date(e.fechaId).getTime() : null),
      m.fichaAt ? new Date(m.fichaAt).getTime() : null,
    ].filter(Boolean);
    const fechaAplicacion = fechasAplicacion.length
      ? formatUTCDate(new Date(Math.max(...fechasAplicacion)))
      : null;

    sheet.addRow({
      folio:              m.status === 'identificado' ? (m.folio ?? null) : null,
      banco:              m.banco ?? null,
      fecha:              formatUTCDate(m.fecha),
      concepto:           m.concepto ?? null,
      deposito:           m.deposito ?? null,
      retiro:             m.retiro   ?? null,
      categoria:          m.categoria ?? null,
      status:             STATUS_LABELS[m.status] ?? m.status,
      erpIds:             (() => {
                            const erp   = (m.erpLinks || [])
                              .map(l => (l.serie && l.folioExterno) ? `${l.serie}-${l.folioExterno}` : (l.folioExterno || l.erpId))
                              .join(', ');
                            const parts = [erp, m.ficha ?? null].filter(Boolean);
                            return parts.join(' · ') || null;
                          })(),
      saldoErp:           m.saldoErp ?? null,
      diferencia,
      numeroAutorizacion: m.numeroAutorizacion ?? null,
      identificadoPor:    [...new Set([
                            ...(m.identificadoPor || []).map(e => e.nombre || e.userId || '?'),
                            ...(m.fichaNombre ? [m.fichaNombre] : m.fichaBy ? [m.fichaBy] : []),
                          ])].join(', ') || null,
      fechaAplicacion,
    });
  }

  // Estilo del encabezado
  const headerRow = sheet.getRow(1);
  headerRow.font = { bold: true };
  headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE2E8F0' } };
  headerRow.border = {
    bottom: { style: 'thin', color: { argb: 'FFB0BAC4' } },
  };

  return workbook.xlsx.writeBuffer();
}

async function deleteMovements(ids) {
  if (!Array.isArray(ids) || ids.length === 0) throw new BadRequestError('Se requiere al menos un ID');
  const result = await BankMovement.deleteMany({ _id: { $in: ids } });
  return { deleted: result.deletedCount };
}

async function setFicha(id, ficha, user) {
  const mov = await BankMovement.findById(id);
  if (!mov) throw new NotFoundError('Movimiento');

  // El permiso banks:ficha ya fue validado en la ruta — solo accede quien corresponde.
  // Solo una ficha por movimiento
  if (mov.ficha != null) {
    throw new ConflictError('Este movimiento ya tiene una ficha registrada');
  }

  const fichaLimpia = (ficha ?? '').toString().trim();
  if (!fichaLimpia) throw new BadRequestError('El número de ficha no puede estar vacío');

  mov.ficha       = fichaLimpia;
  mov.fichaBy     = user._id ?? user.auth0Sub ?? null;
  mov.fichaNombre = user.nombre ?? null;
  mov.fichaAt     = new Date();
  mov.status      = 'identificado';

  const updated = await mov.save();

  emitToBanco(mov.banco, 'bank:movement:updated', {
    _id:        updated._id,
    status:     updated.status,
    ficha:      updated.ficha,
    fichaBy:    updated.fichaBy,
    fichaNombre: updated.fichaNombre,
    fichaAt:    updated.fichaAt,
  });

  return {
    _id:        updated._id,
    status:     updated.status,
    ficha:      updated.ficha,
    fichaBy:    updated.fichaBy,
    fichaNombre: updated.fichaNombre,
    fichaAt:    updated.fichaAt,
  };
}

async function deleteFicha(id, user) {
  const mov = await BankMovement.findById(id);
  if (!mov) throw new NotFoundError('Movimiento');

  if (!mov.ficha) throw new BadRequestError('Este movimiento no tiene ficha registrada');

  // Admin puede borrar cualquier ficha; el autor puede borrar la suya; el resto no
  const userId = user._id ?? user.auth0Sub ?? null;
  const esAdmin = user?.role === 'admin';
  const esAutor = mov.fichaBy && userId && mov.fichaBy === userId;

  if (!esAdmin && !esAutor) {
    throw new ForbiddenError('Solo el usuario que registró la ficha o un administrador puede eliminarla');
  }

  mov.ficha       = null;
  mov.fichaBy     = null;
  mov.fichaNombre = null;
  mov.fichaAt     = null;

  // Recalcular status sin la ficha
  const { saldoErp, uuidXML, status } = aplicarLogicaErp(mov);
  mov.saldoErp = saldoErp;
  mov.uuidXML  = uuidXML;
  mov.status   = status;

  const updated = await mov.save();

  emitToBanco(mov.banco, 'bank:movement:updated', {
    _id:         updated._id,
    status:      updated.status,
    saldoErp:    updated.saldoErp,
    uuidXML:     updated.uuidXML,
    ficha:       null,
    fichaBy:     null,
    fichaNombre: null,
    fichaAt:     null,
  });

  return {
    _id:         updated._id,
    status:      updated.status,
    ficha:       null,
    fichaBy:     null,
    fichaNombre: null,
    fichaAt:     null,
  };
}

// ── Campos que el usuario puede editar manualmente ───────────────────────────
const CAMPOS_EDITABLES = [
  'concepto', 'fecha', 'deposito', 'retiro', 'saldo',
  'numeroAutorizacion', 'referenciaNumerica', 'categoria',
];

// Campos incluidos en el hash de deduplicación (banco no cambia en edición)
const CAMPOS_QUE_AFECTAN_HASH = new Set(['fecha', 'saldo', 'deposito', 'retiro', 'concepto']);

async function updateMovement(id, data, user) {
  const mov = await BankMovement.findById(id);
  if (!mov) throw new NotFoundError('Movimiento');

  // Bloquear si fue identificado por otro usuario (admin puede forzar)
  const idPorEntries = mov.identificadoPor ?? [];
  if (
    user?.role !== 'admin' &&
    mov.status === 'identificado' &&
    idPorEntries.length > 0 &&
    !idPorEntries.some(e => e.userId === user?._id)
  ) {
    throw new ConflictError('Movimiento bloqueado: fue identificado por otro usuario');
  }

  // Los montos no son editables si hay CxC vinculadas (protege la conciliación)
  if ((mov.erpLinks ?? []).length > 0 && ('deposito' in data || 'retiro' in data)) {
    throw new ConflictError('No se pueden editar los montos de un movimiento con CxC vinculadas');
  }

  // Aplicar solo los campos permitidos que vengan en el payload
  let recalcularHash = false;
  for (const campo of CAMPOS_EDITABLES) {
    if (campo in data) {
      mov[campo] = data[campo] ?? null;
      if (CAMPOS_QUE_AFECTAN_HASH.has(campo)) recalcularHash = true;
    }
  }

  // Actualizar hash para mantener la integridad de deduplicación futura
  if (recalcularHash) {
    const nuevoHash = makeHash(mov);
    const colision = await BankMovement.findOne({ hash: nuevoHash, _id: { $ne: mov._id } });
    if (colision) {
      throw new ConflictError('Ya existe un movimiento idéntico con esos datos');
    }
    mov.hash = nuevoHash;
  }

  await mov.save();

  const payload = CAMPOS_EDITABLES.reduce((acc, campo) => {
    acc[campo] = mov[campo] ?? null;
    return acc;
  }, { _id: mov._id, banco: mov.banco });

  emitToBanco(mov.banco, 'bank:movement:updated', payload);

  return payload;
}

async function generateTemplate() {
  const wb = new ExcelJS.Workbook();
  wb.creator       = 'NUMO';
  wb.lastModifiedBy = 'NUMO';

  // ── Hidden signature sheet ─────────────────────────────────────────────────
  const sigWs = wb.addWorksheet(TEMPLATE_SIGNATURE_SHEET);
  sigWs.state = 'veryHidden';
  sigWs.getCell('A1').value = TEMPLATE_SIGNATURE_VALUE;

  // Shared header style
  const headerFill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1D4ED8' } };
  const headerFont = { bold: true, color: { argb: 'FFFFFFFF' }, size: 10 };
  const border     = { style: 'thin', color: { argb: 'FFD1D5DB' } };
  const allBorders = { top: border, left: border, bottom: border, right: border };

  function applyHeader(ws, headers, colWidths) {
    const row = ws.addRow(headers);
    row.eachCell(cell => {
      cell.fill   = headerFill;
      cell.font   = headerFont;
      cell.border = allBorders;
      cell.alignment = { vertical: 'middle', horizontal: 'center' };
    });
    row.height = 20;
    ws.views = [{ state: 'frozen', ySplit: 1 }];
    colWidths.forEach((w, i) => { ws.getColumn(i + 1).width = w; });
  }

  // ── BBVA ──────────────────────────────────────────────────────────────────
  const bbva = wb.addWorksheet('BBVA');
  applyHeader(bbva,
    ['FECHA', 'DESCRIPCION', 'RETIROS', 'DEPOSITOS', 'SALDO'],
    [14, 60, 14, 14, 14],
  );
  bbva.getColumn(1).numFmt = 'dd/mm/yyyy';

  // ── BANAMEX ───────────────────────────────────────────────────────────────
  const banamex = wb.addWorksheet('BANAMEX');
  applyHeader(banamex,
    ['FECHA', 'DESCRIPCION', 'DEPOSITOS', 'RETIROS', 'SALDO'],
    [14, 60, 14, 14, 14],
  );
  banamex.getColumn(1).numFmt = 'dd/mm/yyyy';

  // ── SANTANDER ─────────────────────────────────────────────────────────────
  const santander = wb.addWorksheet('SANTANDER');
  applyHeader(santander,
    ['Cuenta', 'Fecha', 'Hora', 'Sucursal', 'Descripcion',
     'Cargo/Abono', 'Importe', 'Saldo', 'Referencia', 'Concepto',
     'Banco Participante', 'Clabe Beneficiario', 'Nombre Beneficiario',
     'Cta Ordenante', 'Nombre Ordenante', 'Codigo Devolucion',
     'Causa Devolucion', 'RFC Beneficiario', 'RFC Ordenante',
     'Clave de Rastreo', 'Descripcion Larga'],
    [16, 12, 10, 8, 36, 12, 12, 14, 12, 36,
     20, 22, 28, 16, 28, 18, 22, 16, 16, 16, 36],
  );
  santander.getColumn(2).numFmt = 'dd/mm/yyyy';

  // ── AZTECA ────────────────────────────────────────────────────────────────
  const azteca = wb.addWorksheet('AZTECA');
  applyHeader(azteca,
    ['NUMERO DE CUENTA', 'FECHA DE OPERACION', 'FECHA DE APLICACION',
     'CONCEPTO', 'IMPORTE', 'SALDO', 'MOVIMIENTO'],
    [20, 18, 18, 50, 14, 14, 16],
  );
  azteca.getColumn(2).numFmt = 'dd/mm/yyyy';
  azteca.getColumn(3).numFmt = 'dd/mm/yyyy';

  return wb.xlsx.writeBuffer();
}

module.exports = {
  getCards, listMovements, getSummary,
  importFile, updateStatus, updateErpIds, setErpIds, setFicha, deleteFicha,
  getConfig, saveConfig, setSaldoInicial, listCategories, listIdentificadores, importIndividual,
  exportMovements, deleteMovements, updateMovement, generateTemplate,
};
