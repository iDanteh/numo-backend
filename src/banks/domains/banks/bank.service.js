'use strict';

const { randomUUID }    = require('crypto');
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
const STATUS_VALIDOS = ['no_identificado', 'identificado', 'otros', 'reclasificado'];

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
// palabra clave genérica (BNET, REFBNTC, COMPENSACION) en lugar de un número
// real de autorización.  Estos valores NO son identificadores únicos y usarlos
// en dedup generaría falsos positivos entre movimientos distintos del mismo banco.
// COMPENSACION: aparece en MORA SPEI NORMABANXICO — múltiples transacciones
// distintas comparten este label; la dedup real la hace Layer 1d (saldo+concepto).
const BBVA_PSEUDO_AUTH_RE = /^(BNET|REFBNTC|COMPENSACION)$/i;

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
      { $match: { isActive: true, oculto: { $ne: true } } },
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
          reclasificado:   { $sum: { $cond: [{ $eq:  ['$status', 'reclasificado'] }, 1, 0] } },
          saldoPendiente: {
            // Σ de no_identificados ponderada por CxC vinculadas:
            // · Con saldoErp: se suma solo la diferencia (deposito - saldoErp), mínimo 0.
            // · Sin saldoErp: se suma el depósito completo.
            $sum: {
              $cond: [
                { $in: ['$status', ['no_identificado', null]] },
                {
                  $cond: [
                    { $ne: ['$saldoErp', null] },
                    { $max: [0, { $subtract: [{ $ifNull: ['$deposito', 0] }, '$saldoErp'] }] },
                    { $ifNull: ['$deposito', 0] },
                  ],
                },
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
                { $in: ['$status', ['otros', 'reclasificado']] },
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
        reclasificado:   b.reclasificado,
      },
    };
  });
}

async function getStatusStats(year, month) {
  const match = { isActive: true, oculto: { $ne: true } };

  if (year) {
    const y = parseInt(year, 10);
    const m = month ? parseInt(month, 10) : null;
    if (m && m >= 1 && m <= 12) {
      match.fecha = { $gte: new Date(y, m - 1, 1), $lt: new Date(y, m, 1) };
    } else {
      match.fecha = { $gte: new Date(y, 0, 1), $lt: new Date(y + 1, 0, 1) };
    }
  }

  const [statsAgg, yearsAgg] = await Promise.all([
    BankMovement.aggregate([
      { $match: match },
      {
        $group: {
          _id:             null,
          no_identificado: { $sum: { $cond: [{ $and: [{ $in: ['$status', ['no_identificado', null]] }, { $gt: [{ $ifNull: ['$deposito', 0] }, 0] }] }, 1, 0] } },
          identificado:    { $sum: { $cond: [{ $and: [{ $eq:  ['$status', 'identificado'] },           { $gt: [{ $ifNull: ['$deposito', 0] }, 0] }] }, 1, 0] } },
          otros:           { $sum: { $cond: [{ $and: [{ $eq:  ['$status', 'otros'] },                  { $gt: [{ $ifNull: ['$deposito', 0] }, 0] }] }, 1, 0] } },
          reclasificado:   { $sum: { $cond: [{ $and: [{ $eq:  ['$status', 'reclasificado'] },          { $gt: [{ $ifNull: ['$deposito', 0] }, 0] }] }, 1, 0] } },
          dep_no_identificado: { $sum: { $cond: [{ $in: ['$status', ['no_identificado', null]] }, { $cond: [{ $ne: ['$saldoErp', null] }, { $max: [0, { $subtract: [{ $ifNull: ['$deposito', 0] }, '$saldoErp'] }] }, { $ifNull: ['$deposito', 0] }] }, 0] } },
          dep_identificado:    { $sum: { $cond: [{ $and: [{ $eq:  ['$status', 'identificado'] },           { $gt: [{ $ifNull: ['$deposito', 0] }, 0] }] }, { $ifNull: ['$deposito', 0] }, 0] } },
          dep_otros:           { $sum: { $cond: [{ $and: [{ $eq:  ['$status', 'otros'] },                  { $gt: [{ $ifNull: ['$deposito', 0] }, 0] }] }, { $ifNull: ['$deposito', 0] }, 0] } },
          dep_reclasificado:   { $sum: { $cond: [{ $and: [{ $eq:  ['$status', 'reclasificado'] },          { $gt: [{ $ifNull: ['$deposito', 0] }, 0] }] }, { $ifNull: ['$deposito', 0] }, 0] } },
        },
      },
    ]),
    BankMovement.aggregate([
      { $match: { isActive: true } },
      { $group: { _id: { $year: '$fecha' } } },
      { $sort:  { _id: -1 } },
    ]),
  ]);

  return {
    no_identificado:     statsAgg[0]?.no_identificado     ?? 0,
    identificado:        statsAgg[0]?.identificado        ?? 0,
    otros:               statsAgg[0]?.otros               ?? 0,
    reclasificado:       statsAgg[0]?.reclasificado       ?? 0,
    dep_no_identificado: statsAgg[0]?.dep_no_identificado ?? 0,
    dep_identificado:    statsAgg[0]?.dep_identificado    ?? 0,
    dep_otros:           statsAgg[0]?.dep_otros           ?? 0,
    dep_reclasificado:   statsAgg[0]?.dep_reclasificado   ?? 0,
    years: yearsAgg.map(r => r._id).filter(y => y != null && y > 1990),
  };
}

async function listMovements(filters) {
  const {
    page = 1, limit = 50,
    banco, fechaInicio, fechaFin,
    fechaAplicacionInicio, fechaAplicacionFin,
    tipo, search, concepto,
    sortBy = 'fecha', sortDir = 'desc',
    status, categorias, identificadoPor,
    identificadoPorUsuario,
    movId,
  } = filters;

  const filter = { isActive: true, oculto: { $ne: true } };
  if (banco)  filter.banco  = banco;
  if (status) {
    const statusVals = status.split(',').map(v => v.trim()).filter(Boolean);
    filter.status = statusVals.length === 1 ? statusVals[0] : { $in: statusVals };
  }
  // Filtro por ID: acepta un ID exacto (OCR) o varios separados por coma (duplicados).
  // Cuando se pasan múltiples IDs se omiten los demás filtros de fecha/concepto/etc.
  if (movId) {
    const ids = movId.split(',').map(s => s.trim()).filter(Boolean);
    filter._id = ids.length === 1 ? ids[0] : { $in: ids };
  }

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

  if (fechaAplicacionInicio || fechaAplicacionFin) {
    const df = {};
    if (fechaAplicacionInicio) df.$gte = new Date(fechaAplicacionInicio);
    if (fechaAplicacionFin)    df.$lte = new Date(`${fechaAplicacionFin}T23:59:59.999Z`);
    filter.$and = filter.$and ?? [];
    filter.$and.push({ $or: [
      { fichaAt: df },
      { identificadoPor: { $elemMatch: { fechaId: df } } },
    ]});
  }

  // Variables de búsqueda — se populan si search está activo y se reutilizan
  // tanto en el $match (filter.$or) como en el $addFields de scoring.
  let _searchEscaped = null;
  let _amountLo      = null;
  let _amountHi      = null;

  if (search) {
    _searchEscaped = search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re        = new RegExp(_searchEscaped, 'i');
    const orClauses = [
      { concepto: re }, { numeroAutorizacion: re },
      { referenciaNumerica: re }, { folio: re }, { uuidXML: re },
    ];

    // Búsqueda por monto — tolerancia basada en los decimales ingresados:
    // sin decimales → rango de 1 peso completo; 1 decimal → ±0.05; 2 decimales → ±0.005
    const cleanNum = search.replace(/[$,\s]/g, '');
    const num      = parseFloat(cleanNum);
    if (!isNaN(num) && num > 0) {
      const decimalPlaces = (cleanNum.split('.')[1] || '').length;
      const tolerance = decimalPlaces === 0 ? 1 : decimalPlaces === 1 ? 0.05 : 0.005;
      _amountLo = decimalPlaces === 0 ? num             : num - tolerance;
      _amountHi = decimalPlaces === 0 ? num + tolerance : num + tolerance;
      orClauses.push({ deposito: { $gte: _amountLo, $lte: _amountHi } });
      orClauses.push({ retiro:   { $gte: _amountLo, $lte: _amountHi } });
    }

    filter.$or = orClauses;
  }

  const SORTABLE  = ['fecha', 'banco', 'deposito', 'retiro', 'saldo', 'saldo-erp', 'diferencia'];
  const rawSortBy = SORTABLE.includes(sortBy) ? sortBy : 'fecha';
  const FIELD_MAP = { 'saldo-erp': 'saldoErp' };
  const sortField = FIELD_MAP[rawSortBy] ?? rawSortBy;
  const sortOrder = sortDir === 'asc' ? 1 : -1;
  const skip      = (parseInt(page) - 1) * parseInt(limit);

  // ── Construcción del query ──────────────────────────────────────────────────
  // Usamos aggregation cuando:
  //   a) El usuario ordenó por "diferencia" (requiere campo calculado), O
  //   b) Hay búsqueda activa → añadimos _score para priorizar montos sobre concepto.
  //
  // Orden de prioridad de _score:
  //   3 → deposito / retiro  (monto exacto — mayor relevancia)
  //   2 → numeroAutorizacion / referenciaNumerica
  //   1 → folio / uuidXML / auxNombre
  //   0 → concepto (texto libre — menor relevancia)
  const useAggregation = rawSortBy === 'diferencia' || !!search;

  let movementsQuery;
  if (useAggregation) {
    const pipeline = [{ $match: filter }];

    // ── Scoring de relevancia (solo cuando hay búsqueda activa) ─────────────
    if (search) {
      const scoreBranches = [];

      // Score 3: match por monto (solo si el término es numérico)
      if (_amountLo !== null) {
        scoreBranches.push({
          case: { $or: [
            { $and: [{ $gte: ['$deposito', _amountLo] }, { $lt: ['$deposito', _amountHi] }] },
            { $and: [{ $gte: ['$retiro',   _amountLo] }, { $lt: ['$retiro',   _amountHi] }] },
          ]},
          then: 3,
        });
      }

      // Score 2: número de autorización o referencia numérica
      scoreBranches.push({
        case: { $or: [
          { $regexMatch: { input: { $ifNull: ['$numeroAutorizacion', ''] }, regex: _searchEscaped, options: 'i' } },
          { $regexMatch: { input: { $ifNull: ['$referenciaNumerica',  ''] }, regex: _searchEscaped, options: 'i' } },
        ]},
        then: 2,
      });

      // Score 1: folio interno, UUID CFDI, nombre auxiliar
      scoreBranches.push({
        case: { $or: [
          { $regexMatch: { input: { $ifNull: ['$folio',     ''] }, regex: _searchEscaped, options: 'i' } },
          { $regexMatch: { input: { $ifNull: ['$uuidXML',   ''] }, regex: _searchEscaped, options: 'i' } },
          { $regexMatch: { input: { $ifNull: ['$auxNombre', ''] }, regex: _searchEscaped, options: 'i' } },
        ]},
        then: 1,
      });

      // Default 0: solo matcheó el concepto (texto libre, menor prioridad)
      pipeline.push({
        $addFields: { _score: { $switch: { branches: scoreBranches, default: 0 } } },
      });
    }

    // ── Campo calculado para ordenar por diferencia ──────────────────────────
    if (rawSortBy === 'diferencia') {
      pipeline.push({ $addFields: { _diferencia: { $subtract: [
        { $add: [{ $ifNull: ['$deposito', 0] }, { $ifNull: ['$retiro', 0] }] },
        { $ifNull: ['$saldoErp', 0] },
      ] } } });
    }

    // ── Sort: primero por score (si hay búsqueda), luego por campo del usuario ─
    const sortStage = {};
    if (search)                        sortStage._score      = -1;
    if (rawSortBy === 'diferencia')    sortStage._diferencia = sortOrder;
    else                               sortStage[sortField]  = sortOrder;
    sortStage._id = 1;

    pipeline.push({ $sort: sortStage });
    pipeline.push({ $skip: skip });
    pipeline.push({ $limit: parseInt(limit) });

    movementsQuery = BankMovement.aggregate(pipeline);
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

async function importFile(buffer, banco, userId, { auth0Sub, nombre, filename } = {}) {
  const bancoValidado = BANCOS_VALIDOS.includes(banco) ? banco : null;
  const { movements, sinFecha, sinImporte, summary, errors } = await parseBankFile(buffer, bancoValidado);
  const sinFechaMovs   = sinFecha   || [];
  const sinImporteMovs = sinImporte || [];

  if (!movements.length && errors.length) {
    const err = new Error('No se pudo procesar ninguna hoja del archivo');
    err.statusCode = 422;
    err.errors = errors;
    throw err;
  }

  // ── 1. Detectar duplicados ANTES de reservar secuenciales ─────────────────
  const hashes = movements.map(m => m.hash);
  const existentes = await BankMovement.find(
    { isActive: true, hash: { $in: hashes } },
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
      enrichmentUpdates.push({ _id: ex._id, $set: enrich, via: 'capa1a' });
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
      { isActive: true, banco: { $in: bancosAuth }, $or: authConditions },
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
          (m.retiro   != null && existing.retiro   != null && Math.abs(m.retiro   - existing.retiro  ) < 0.01) ||
          // El registro existente no tiene importe (bug de parseo de DEP EN EFECTIVO de
          // Banamex: el monto estaba en sub-fila "Referencia alfanumérica" y no se extraía).
          // Con el auth coincidente y el existing sin monto, se acepta el match y se
          // enriquece el registro con el importe que trae el reimport.
          (existing.deposito == null && existing.retiro == null && (m.deposito != null || m.retiro != null));
        return montoOk;
      });
      if (!incoming) continue;
      // Layer 1b NO actualiza fecha: su propósito es enriquecer campos faltantes
      // (referenciaNumerica, deposito/retiro).  Los cambios de fecha por distinto
      // extracto bancario son responsabilidad de Layer 1e (cross-date dedup) que
      // aplica la lógica de "fecha de valor más reciente".
      // Enriquecer referenciaNumerica si el reimport la trae y el existente no la tiene
      if (incoming.referenciaNumerica && !existing.referenciaNumerica) {
        enrichmentUpdates.push({ _id: existing._id, $set: { referenciaNumerica: incoming.referenciaNumerica }, via: 'capa1b' });
      }
      // Enriquecer deposito/retiro si el existente no tiene importe (legacy del bug
      // de "Referencia alfanumérica") y el reimport ya lo trae correctamente parseado.
      if (existing.deposito == null && existing.retiro == null) {
        const enrich = {};
        if (incoming.deposito != null) enrich.deposito = incoming.deposito;
        if (incoming.retiro   != null) enrich.retiro   = incoming.retiro;
        if (Object.keys(enrich).length > 0) {
          enrichmentUpdates.push({ _id: existing._id, $set: enrich, via: 'capa1b' });
        }
      }
      // Marcar como ya existente para que no se re-inserte
      hashesExistentes.add(incoming.hash);
    }
  }

  // ── 1c. Deduplicar por referenciaNumerica (Banamex y BBVA MORA SPEI) ─────────
  // Banamex: identificador de operación estable entre exportaciones.
  // BBVA MORA SPEI: clave de rastreo extraída del concepto tras "COMPENSACION DE"
  //   (ej. "8846APR1202605085280762645").  El mismo movimiento puede aparecer con
  //   distinto whitespace en el concepto → hash diferente; la clave de rastreo es
  //   siempre idéntica y permite deduplicar con banco + referenciaNumerica + importe.
  //
  // Se excluye referenciaNumerica = "0": es el placeholder de Banamex para depósitos
  // en efectivo sin referencia real (Banamex exporta "0000000000" → normalizado a "0").
  // Usarlo como clave generaría falsos positivos entre transacciones distintas con
  // el mismo monto (el parser ya lo normaliza a null, pero se filtra aquí también
  // por si existen registros históricos con este valor).
  const refNumMovs = movements.filter(m =>
    m.referenciaNumerica &&
    m.referenciaNumerica !== '0' &&
    !hashesExistentes.has(m.hash)
  );

  if (refNumMovs.length > 0) {
    const uniqueRefNums   = [...new Set(refNumMovs.map(m => m.referenciaNumerica))];
    const refConditions   = uniqueRefNums.map(n =>
      /^\d+$/.test(n)
        ? { referenciaNumerica: { $regex: `^0*${n}$` } }
        : { referenciaNumerica: n },
    );
    const bancosRef = [...new Set(refNumMovs.map(m => m.banco))];

    const existByRef = await BankMovement.find(
      { isActive: true, banco: { $in: bancosRef }, $or: refConditions },
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
        if (!montoOk) return false;
        // referenciaNumerica puede ser código de sucursal/ruta (no ID de transacción).
        // Si ambos lados tienen numeroAutorizacion real, debe coincidir; de lo contrario
        // dos transacciones distintas con misma ref+monto serían tratadas como duplicado.
        const incomingHasAuth = m.numeroAutorizacion && !isBBVAPseudoAuth(m.banco, m.numeroAutorizacion);
        const existingHasAuth = existing.numeroAutorizacion && !isBBVAPseudoAuth(existing.banco, existing.numeroAutorizacion);
        if (incomingHasAuth && existingHasAuth) {
          return authMatch(m.numeroAutorizacion, existing.numeroAutorizacion);
        }
        return true;
      });
      if (!incoming) continue;
      // Enriquecer numeroAutorizacion si el reimport la trae y el existente no la tiene
      const existingAuthIsPseudo = isBBVAPseudoAuth(existing.banco, existing.numeroAutorizacion);
      if (
        incoming.numeroAutorizacion &&
        !isBBVAPseudoAuth(incoming.banco, incoming.numeroAutorizacion) &&
        (!existing.numeroAutorizacion || existingAuthIsPseudo)
      ) {
        enrichmentUpdates.push({ _id: existing._id, $set: { numeroAutorizacion: incoming.numeroAutorizacion }, via: 'capa1c' });
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
        { isActive: true, $or: orConds },
        '_id banco fecha deposito retiro saldo concepto numeroAutorizacion referenciaNumerica',
      ).limit(5000).lean();

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
              // Guard: en formato "PAGO CUENTA DE TERCERO / {authNum} BNET {accountNum}",
              // el token tras BNET es el número de cuenta destino del beneficiario —
              // compartido entre múltiples pagos al mismo destinatario, NO un ID único
              // de transacción.  Si ambos lados tienen auth real y distinto, el BNET
              // es cuenta destino; dejar pasar al chequeo de saldo que los distinguirá.
              const incRealAuth = m.numeroAutorizacion && !isBBVAPseudoAuth(m.banco, m.numeroAutorizacion);
              const cndRealAuth = cand.numeroAutorizacion && !isBBVAPseudoAuth(cand.banco, cand.numeroAutorizacion);
              const authsDiffer = incRealAuth && cndRealAuth && !authMatch(m.numeroAutorizacion, cand.numeroAutorizacion);
              if (!authsDiffer) {
                hashesExistentes.add(m.hash);
                softDuplicados++;
                // Enriquecer si el reimport trae datos que el existente no tiene
                const enrichBnet = buildSoftEnrich(m, cand);
                if (enrichBnet) enrichmentUpdates.push({ _id: cand._id, $set: enrichBnet, via: 'capa1d' });
                break;
              }
              // authsDiffer=true → BNET es cuenta destino, no ID de transacción.
              // Continuar al chequeo de saldo.
            }
            // Si ninguno tiene número BNET, seguir con el check de saldo+concepto.
          }

          // 2b-bis. Banamex + saldo=null: deduplicar por auth+monto cuando el entrante
          // no trae saldo (archivos de terceros o re-exportaciones sin columna de saldo).
          // Ocurre cuando la tercera variante de un DEP MIXTO se importa con auth real
          // pero sin balance de cuenta → Capa 1b la atrapa si el existente ya tiene auth;
          // si el existente aún tiene auth=null, cae aquí para deduplicar por auth del
          // entrante contra el mismo candidato con auth no nulo.
          if (m.banco === 'Banamex' && m.saldo == null) {
            if (
              m.numeroAutorizacion &&
              !isBBVAPseudoAuth(m.banco, m.numeroAutorizacion) &&
              cand.numeroAutorizacion &&
              authMatch(m.numeroAutorizacion, cand.numeroAutorizacion)
            ) {
              hashesExistentes.add(m.hash);
              softDuplicados++;
              const enrichBnmx = buildSoftEnrich(m, cand);
              if (enrichBnmx) enrichmentUpdates.push({ _id: cand._id, $set: enrichBnmx, via: 'capa1d' });
              break;
            }
            // saldo=null sin auth coincidente → no es posible determinar duplicado
            // por saldo; saltar al siguiente candidato sin ejecutar la comprobación
            // de saldo que siempre fallará (evita conceptMatch falso con null-saldo).
            continue;
          }

          // 2b. El saldo debe coincidir exactamente (±0.01).
          // El saldo es el balance acumulado de la cuenta: dos movimientos distintos
          // en la misma cuenta nunca comparten el mismo saldo, por lo que este
          // criterio descarta falsos positivos de forma prácticamente infalible.
          const saldoOk = m.saldo != null && cand.saldo != null && Math.abs(m.saldo - cand.saldo) < 0.01;
          if (!saldoOk) continue;

          // BBVA: banco+fecha+monto+saldo iguales → mismo movimiento, sin verificar
          // concepto.  BBVA exporta la misma transacción con textos completamente
          // distintos según el tipo de estado de cuenta descargado:
          //   "DEPOSITO CHEQUE BBVA"       ↔  "BBV0002829120109031014"
          //   "MORA SPEI NORMABANXICO / …" ↔  "COMP SPEI / …"   (red de seguridad;
          //                                      Capa 1c es la vía primaria)
          // El saldo acumulado es único dentro de una misma cuenta → el triplete
          // banco+fecha+saldo no puede coincidir por azar en transacciones distintas
          // de la misma cuenta.  Riesgo teórico: dos cuentas BBVA distintas con el
          // mismo saldo y el mismo monto en la misma fecha (prácticamente imposible).
          if (m.banco === 'BBVA') {
            hashesExistentes.add(m.hash);
            softDuplicados++;
            const enrichSoft = buildSoftEnrich(m, cand);
            if (enrichSoft) enrichmentUpdates.push({ _id: cand._id, $set: enrichSoft, via: 'capa1d' });
            break;
          }

          // 3. El concepto debe compartir texto significativo (mín. 20 chars).
          // Dos variantes cubiertas:
          //   a) Prefijo común: un import trae el número de autorización incrustado
          //      y el otro no (ej. Banamex con/sin sub-filas).
          //   b) Sufijo común: BBVA exporta el mismo movimiento en formato largo
          //      "DEPOSITO EFECTIVO PRACTIC / ******1014 …" y formato corto
          //      "******1014 …". El corto es exactamente el sufijo del largo tras
          //      el separador " / ", por lo que no comparten prefijo pero sí sufijo.
          const cA = (m.concepto   || '').replace(/\s+/g, ' ').trim().toLowerCase();
          const cB = (cand.concepto || '').replace(/\s+/g, ' ').trim().toLowerCase();
          const minL = Math.min(cA.length, cB.length);
          const conceptMatch = minL >= 20 && (
            cA.substring(0, minL) === cB.substring(0, minL) ||  // (a) prefijo
            cA.endsWith(cB) || cB.endsWith(cA)                   // (b) sufijo
          );
          if (conceptMatch) {
            hashesExistentes.add(m.hash);
            softDuplicados++;
            // Enriquecer si el reimport trae datos que el existente no tiene
            const enrichSoft = buildSoftEnrich(m, cand);
            if (enrichSoft) enrichmentUpdates.push({ _id: cand._id, $set: enrichSoft, via: 'capa1d' });
            break;
          }
        }
      }
    }
  }

  // ── 1e. Cross-date dedup: banco + saldo + importe (sin restricción de fecha) ─
  // BBVA asigna fechas distintas al mismo depósito según el extracto descargado:
  //   · Fecha de operación (cuando se depositó en sucursal)
  //   · Fecha de valor     (cuando acreditó en cuenta)
  // El hash difiere porque la fecha forma parte de la clave, y Capa 1d no los
  // detecta porque su ventana de búsqueda es el mismo día exacto.
  // El saldo resultante en cuenta es idéntico en ambas versiones y, combinado
  // con el importe y el concepto, identifica el movimiento de forma unívoca.
  //
  // Solo aplica a movimientos sin numeroAutorizacion ni referenciaNumerica
  // (los que tienen identificador ya quedan cubiertos por Capas 1b/1c).
  const sinIdentificador = movements.filter(m =>
    !hashesExistentes.has(m.hash) &&
    !m.numeroAutorizacion        &&
    !m.referenciaNumerica        &&
    m.saldo != null              &&
    (m.deposito != null || m.retiro != null),
  );

  if (sinIdentificador.length > 0) {
    // Ventana máxima entre fecha de operación y fecha de valor en extractos bancarios.
    // BBVA puede diferir 1-3 días hábiles; 7 días cubre cualquier caso real y evita
    // que un movimiento del mismo saldo+monto en un período distinto se tome como dup.
    const CROSS_DATE_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;

    // Una condición por movimiento: banco + fecha ±7d + saldo ±0.01 + importe ±0.01
    const saldoConds = sinIdentificador.map(m => {
      const fechaBase = new Date(m.fecha).getTime();
      return {
        banco: m.banco,
        fecha: {
          $gte: new Date(fechaBase - CROSS_DATE_WINDOW_MS),
          $lte: new Date(fechaBase + CROSS_DATE_WINDOW_MS),
        },
        saldo: { $gte: m.saldo - 0.01, $lte: m.saldo + 0.01 },
        ...(m.deposito != null
          ? { deposito: { $gte: m.deposito - 0.01, $lte: m.deposito + 0.01 } }
          : { retiro:   { $gte: m.retiro   - 0.01, $lte: m.retiro   + 0.01 } }),
      };
    });

    const existBySaldo = await BankMovement.find(
      { isActive: true, $or: saldoConds },
      '_id banco saldo deposito retiro concepto numeroAutorizacion referenciaNumerica fecha',
    ).lean();

    for (const existing of existBySaldo) {
      const incoming = sinIdentificador.find(m => {
        if (m.banco !== existing.banco) return false;
        if (hashesExistentes.has(m.hash)) return false; // ya marcado en iteración previa
        const saldoMatch =
          Math.abs(m.saldo - existing.saldo) < 0.01;
        const montoMatch =
          (m.deposito != null && existing.deposito != null && Math.abs(m.deposito - existing.deposito) < 0.01) ||
          (m.retiro   != null && existing.retiro   != null && Math.abs(m.retiro   - existing.retiro  ) < 0.01);
        if (!saldoMatch || !montoMatch) return false;
        // Concepto: prefijo o sufijo común (mín. 10 chars).
        // Umbral menor que Capa 1d (20) para cubrir conceptos cortos pero
        // descriptivos como "DEPOSITO EN EFECTIVO" (20 chars exactos).
        const cA = (m.concepto        || '').replace(/\s+/g, ' ').trim().toLowerCase();
        const cB = (existing.concepto || '').replace(/\s+/g, ' ').trim().toLowerCase();
        const minL = Math.min(cA.length, cB.length);
        // BBVA cross-date: saldo+monto suficiente sin verificar concepto
        // (misma lógica que Capa 1d — BBVA usa formatos de texto incompatibles).
        if (m.banco === 'BBVA') return true;
        return minL >= 10 && (
          cA.substring(0, minL) === cB.substring(0, minL) ||
          cA.endsWith(cB) || cB.endsWith(cA)
        );
      });
      if (!incoming) continue;

      // Conservar la fecha de valor (la más reciente) como fecha definitiva
      const fechaExisting = new Date(existing.fecha).getTime();
      const fechaIncoming = new Date(incoming.fecha).getTime();
      if (fechaExisting !== fechaIncoming) {
        const fechaCorrecta = fechaIncoming > fechaExisting ? incoming.fecha : existing.fecha;
        fechaUpdates.push({ _id: existing._id, fecha: fechaCorrecta, de: existing.fecha, via: 'capa1e', importFile: filename });
      }

      const enrichCross = buildSoftEnrich(incoming, existing);
      if (enrichCross) enrichmentUpdates.push({ _id: existing._id, $set: enrichCross, via: 'capa1e' });
      hashesExistentes.add(incoming.hash);
      softDuplicados++;
    }
  }

  let nuevos = movements.filter(m => !hashesExistentes.has(m.hash));

  // ── 1f. Intra-lote: deduplicar nuevos contra otros nuevos del mismo batch ─
  // Capas 1a-1e solo comparan contra BD. Si un archivo contiene dos filas del
  // mismo movimiento (concepto distinto, saldo distinto, ninguna en BD todavía),
  // ambas pasan todas las capas y se insertan como documentos separados.
  // Esta capa aplica los mismos criterios del parser intra-lote (auth+monto y
  // monto+saldo) al subconjunto que sería insertado como nuevo.
  // En caso de colisión, se conserva el primer movimiento del lote y se descarta
  // el segundo — igual que el comportamiento del parser de Banamex.
  if (nuevos.length > 1) {
    const intraAuthSeen  = new Map(); // `banco|normAuth|dep|ret` → true
    const intraSaldoSeen = new Map(); // `banco|dep|ret|saldo`    → true
    const intraDupHashes = new Set();
    // Normaliza a centavos enteros para evitar falsos negativos por variación de
    // punto flotante entre filas del mismo archivo (0.01 threshold = 1 centavo).
    const toCents = v => v != null ? Math.round(v * 100) : '';

    for (const m of nuevos) {
      const auth = m.numeroAutorizacion;
      let isDup  = false;

      // Auth + monto (idéntico a Capa A del parser, bank-agnostic)
      if (auth && auth !== '0' && !isBBVAPseudoAuth(m.banco, auth)) {
        const normAuth = /^\d+$/.test(auth) ? String(parseInt(auth, 10)) : auth;
        const k = `${m.banco}|${normAuth}|${toCents(m.deposito)}|${toCents(m.retiro)}`;
        if (intraAuthSeen.has(k)) {
          isDup = true;
          softDuplicados++;
        } else {
          intraAuthSeen.set(k, true);
        }
      }

      // Monto + saldo (idéntico a Capa B del parser, bank-agnostic)
      if (!isDup && m.saldo != null && (m.deposito != null || m.retiro != null)) {
        const k = `${m.banco}|${toCents(m.deposito)}|${toCents(m.retiro)}|${toCents(m.saldo)}`;
        if (intraSaldoSeen.has(k)) {
          isDup = true;
          softDuplicados++;
        } else {
          intraSaldoSeen.set(k, true);
        }
      }

      if (isDup) intraDupHashes.add(m.hash);
    }

    if (intraDupHashes.size > 0) {
      nuevos = nuevos.filter(m => !intraDupHashes.has(m.hash));
    }
  }

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

  // ── 3b. Actualizar fecha de movimientos deduplicados por cross-date (capa1e) ─
  if (fechaUpdates.length > 0) {
    const now = new Date();
    const fechaOps = fechaUpdates.map(({ _id, fecha, de, via, importFile: file }) => ({
      updateOne: {
        filter: { _id },
        update: {
          $set:  { fecha },
          $push: { _changelog: { at: now, via, campo: 'fecha', de, a: fecha, importFile: file } },
        },
      },
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
    for (const { _id, $set, via } of enrichmentUpdates) {
      const key = String(_id);
      if (!enrichById.has(key)) {
        enrichById.set(key, { _id, $set: { ...$set }, vias: [via] });
      } else {
        const entry = enrichById.get(key);
        Object.assign(entry.$set, $set);
        if (!entry.vias.includes(via)) entry.vias.push(via);
      }
    }
    const enrichNow = new Date();
    const enrichOps = [...enrichById.values()].map(({ _id, $set, vias }) => ({
      updateOne: {
        filter: { _id },
        update: {
          $set,
          $push: {
            _changelog: {
              at:         enrichNow,
              via:        vias.join('+'),
              campos:     Object.keys($set),
              importFile: filename,
            },
          },
        },
      },
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
      const [catRules, ocultarRules, cambiarEstadoRules] = await Promise.all([
        bankRuleRepo.listByBanco(bancoValidado, { accion: 'categorizar' }),
        bankRuleRepo.listByBanco(bancoValidado, { accion: 'ocultar' }),
        bankRuleRepo.listByBanco(bancoValidado, { accion: 'cambiar_estado' }),
      ]);

      if (catRules.length === 0 && ocultarRules.length === 0 && cambiarEstadoRules.length === 0) {
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
            if (matchRegla(mov, rule)) {
              $set.categoria = rule.nombre;
              if (rule.estadoDestino) $set.status = rule.estadoDestino;
              break;
            }
          }
          if ($set.categoria) categorizados++;
          for (const rule of ocultarRules) {
            if (matchRegla(mov, rule)) { $set.oculto = true; break; }
          }
          if (!$set.status) {
            for (const rule of cambiarEstadoRules) {
              if (matchRegla(mov, rule)) { $set.status = rule.estadoDestino; break; }
            }
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
      sinFecha:    sinFechaMovs.map(m => ({
        banco:    m.banco,
        concepto: (m.concepto || '').substring(0, 100),
        deposito: m.deposito,
        retiro:   m.retiro,
      })),
      sinImporte:  sinImporteMovs.map(m => ({
        banco:    m.banco,
        concepto: (m.concepto || '').substring(0, 100),
        fecha:    m.fecha,
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
    const [catRules, ocultarRules, cambiarEstadoRules] = await Promise.all([
      bankRuleRepo.listByBanco(bancoValidado, { accion: 'categorizar' }),
      bankRuleRepo.listByBanco(bancoValidado, { accion: 'ocultar' }),
      bankRuleRepo.listByBanco(bancoValidado, { accion: 'cambiar_estado' }),
    ]);

    let catEstadoAplicado = false;
    for (const rule of catRules) {
      if (matchRegla(nuevo, rule)) {
        nuevo.categoria = rule.nombre;
        if (rule.estadoDestino) { nuevo.status = rule.estadoDestino; catEstadoAplicado = true; }
        categorizado = true;
        break;
      }
    }
    for (const rule of ocultarRules) {
      if (matchRegla(nuevo, rule)) { nuevo.oculto = true; break; }
    }
    let estadoCambiado = catEstadoAplicado;
    if (!catEstadoAplicado) {
      for (const rule of cambiarEstadoRules) {
        if (matchRegla(nuevo, rule)) { nuevo.status = rule.estadoDestino; estadoCambiado = true; break; }
      }
    }
    if (categorizado || nuevo.oculto || estadoCambiado) await nuevo.save();
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
        // saldoPagado: monto acumulado cobrado por transferencia/depósito en efectivo para
        // este link (ver cobro-panel _buildCobroSaldosErp). Si el cobro-panel ya lo calculó
        // explícitamente (no es null, aunque sea 0 — ej. se cobró todo en efectivo de caja),
        // ES la fuente de verdad y NO se cae a saldoActual/total: si cayera, un pago hecho
        // por una forma no bancaria terminaría contando como si el banco lo hubiera cubierto.
        // Solo cuando saldoPagado nunca se ha determinado (null — CxC vinculada sin pasar por
        // el cobro-panel, ej. ya estaba pagada en Kore por otro canal) cae al comportamiento
        // legado: saldoActual > 0 o total.
        let ref;
        if (l.saldoPagado != null) {
          ref = l.saldoPagado;
        } else {
          ref = (l.saldoActual != null && l.saldoActual > 0)
            ? l.saldoActual
            : (l.total ?? 0);
        }
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

async function updateCategoria(id, categoria, user) {
  if (categoria !== undefined && categoria !== null && typeof categoria !== 'string') {
    throw new BadRequestError('categoria debe ser string o null');
  }
  const categoriaLimpia = typeof categoria === 'string' ? (categoria.trim() || null) : null;

  const mov = await BankMovement.findById(id);
  if (!mov) throw new NotFoundError('Movimiento');

  const anterior = mov.categoria ?? null;
  if (anterior === categoriaLimpia) {
    return { _id: mov._id, banco: mov.banco, categoria: anterior, status: mov.status };
  }

  // Al asignar categoría manualmente:
  //   - Si el movimiento ya está 'identificado' (tiene CxC conciliada), conservamos ese status.
  //     Asignar una categoría es una anotación organizacional, no revierte la conciliación.
  //   - En cualquier otro status pasamos a 'reclasificado' para indicar intervención manual.
  // Al limpiarla → vuelve a 'identificado' si tiene ERP, sino 'no_identificado'.
  let newStatus = mov.status;
  if (categoriaLimpia) {
    if (mov.status !== 'identificado') {
      newStatus = 'reclasificado';
    }
  } else if (mov.status === 'reclasificado') {
    newStatus = (mov.erpIds?.length ?? 0) > 0 ? 'identificado' : 'no_identificado';
  }

  await BankMovement.updateOne(
    { _id: id },
    {
      $set:  { categoria: categoriaLimpia, status: newStatus },
      $push: {
        _changelog: {
          at:         new Date(),
          via:        user ? `manual:${user._id}` : 'manual',
          campo:      'categoria',
          de:         anterior,
          a:          categoriaLimpia,
          importFile: null,
        },
      },
    }
  );

  const result = { _id: mov._id, banco: mov.banco, categoria: categoriaLimpia, status: newStatus };
  emitToBanco(mov.banco, 'bank:movement:updated', result);
  return result;
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
      saldoPagado:  l.saldoPagado != null ? Number(l.saldoPagado) : null,
      folioFiscal:  l.folioFiscal ? String(l.folioFiscal).trim().toUpperCase() : null,
      total:        l.total != null ? Number(l.total) : null,
      serie:        l.serie ? String(l.serie).trim() : null,
      folioExterno: l.folioExterno ? String(l.folioExterno).trim() : null,
      tipoPago:     l.tipoPago ? String(l.tipoPago).trim().toUpperCase() : null,
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
    status, categorias, identificadoPor,
    formaPago,
    importeMin, importeMax,
    folioFiscal: folioFiscalFilter,
    ficha:       fichaParamFilter,
    columnas,
  } = filters;

  // Columnas opcionales activas (default: las 3 más útiles)
  const colSet = columnas
    ? new Set(columnas.split(',').map(v => v.trim()).filter(Boolean))
    : new Set(['saldoErp', 'folioFiscal', 'formaPago']);

  // ── Query ────────────────────────────────────────────────────────────────
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

  if (fechaAplicacionInicio || fechaAplicacionFin) {
    const df = {};
    if (fechaAplicacionInicio) df.$gte = new Date(fechaAplicacionInicio);
    if (fechaAplicacionFin)    df.$lte = new Date(`${fechaAplicacionFin}T23:59:59.999Z`);
    filter.$and = filter.$and ?? [];
    filter.$and.push({ $or: [
      { fichaAt: df },
      { identificadoPor: { $elemMatch: { fechaId: df } } },
    ]});
  }

  if (search) {
    const escaped = search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re       = new RegExp(escaped, 'i');
    const orClauses = [
      { concepto: re }, { numeroAutorizacion: re },
      { referenciaNumerica: re }, { folio: re }, { uuidXML: re },
    ];
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

  // ── Nuevos filtros ───────────────────────────────────────────────────────

  // Forma de pago ERP: al menos un erpLink con tipoPago en los valores seleccionados
  if (formaPago) {
    const fps = formaPago.split(',').map(v => v.trim()).filter(Boolean);
    if (fps.length > 0) {
      filter.$and = filter.$and ?? [];
      filter.$and.push({
        erpLinks: { $elemMatch: { tipoPago: fps.length === 1 ? fps[0] : { $in: fps } } },
      });
    }
  }

  // Rango de importe (aplica sobre deposito o retiro)
  if (importeMin != null || importeMax != null) {
    const min = importeMin != null ? Number(importeMin) : null;
    const max = importeMax != null ? Number(importeMax) : null;
    const rangeFilter = {};
    if (min != null) rangeFilter.$gte = min;
    if (max != null) rangeFilter.$lte = max;
    filter.$and = filter.$and ?? [];
    filter.$and.push({ $or: [{ deposito: rangeFilter }, { retiro: rangeFilter }] });
  }

  // Folio fiscal (con / sin)
  if (folioFiscalFilter === 'con') {
    filter.$and = filter.$and ?? [];
    filter.$and.push({ erpLinks: { $elemMatch: { folioFiscal: { $nin: [null, ''] } } } });
  } else if (folioFiscalFilter === 'sin') {
    filter.$and = filter.$and ?? [];
    filter.$and.push({ $nor: [{ erpLinks: { $elemMatch: { folioFiscal: { $nin: [null, ''] } } } }] });
  }

  // Ficha de conciliación (con / sin)
  if (fichaParamFilter === 'con') {
    filter.ficha = { $nin: [null, ''] };
  } else if (fichaParamFilter === 'sin') {
    filter.$and = filter.$and ?? [];
    filter.$and.push({ $or: [{ ficha: null }, { ficha: { $exists: false } }] });
  }

  // ── Sort + query ─────────────────────────────────────────────────────────
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

  // ── Excel ────────────────────────────────────────────────────────────────
  const ExcelJS = require('exceljs');
  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet('Movimientos');

  // Columnas base (siempre presentes)
  const baseCols = [
    { header: 'Fecha',             key: 'fecha',              width: 13 },
    { header: 'Banco',             key: 'banco',              width: 13 },
    { header: 'Aut. NUMO',        key: 'folio',              width: 12 },
    { header: 'Concepto',          key: 'concepto',           width: 48 },
    { header: 'Fecha aplicación',  key: 'fechaAplicacion',    width: 17 },
    { header: 'Depósito',          key: 'deposito',           width: 16 },
    { header: 'Retiro',            key: 'retiro',             width: 16 },
    { header: 'Serie-Folio ERP',   key: 'erpIds',             width: 30 },
    { header: 'Saldo ERP',         key: 'saldoErp',           width: 16 },
    { header: 'Diferencia',        key: 'diferencia',         width: 14 },
    { header: 'Estado',            key: 'status',             width: 17 },
    { header: 'Aut. Bancaria',   key: 'numeroAutorizacion', width: 20 },
    { header: 'Categoría',         key: 'categoria',          width: 18 },
    { header: 'Identificado por',  key: 'identificadoPor',    width: 22 },
  ];

  // Columnas opcionales
  const addlColDefs = {
    folioFiscal: { header: 'UUID Venta',  key: 'folioFiscalCol', width: 20 },
    formaPago:   { header: 'Forma de pago', key: 'formaPagoCol',   width: 16 },
    retencion:   { header: 'Retención',     key: 'retencionCol',   width: 12 },
    ficha:       { header: 'N° Ficha',      key: 'fichaCol',       width: 14 },
  };

  const activeCols = [...baseCols];
  for (const key of ['folioFiscal', 'formaPago', 'retencion', 'ficha']) {
    if (colSet.has(key)) activeCols.push(addlColDefs[key]);
  }
  sheet.columns = activeCols;

  // ── Helpers ──────────────────────────────────────────────────────────────
  const STATUS_LABELS = {
    no_identificado: 'No identificado',
    identificado:    'Identificado',
    otros:           'Otros',
    reclasificado:   'Reclasificado',
  };

  const formatUTCDate = (raw) => {
    if (!raw) return null;
    const d = new Date(raw);
    return `${String(d.getUTCDate()).padStart(2, '0')}/${String(d.getUTCMonth() + 1).padStart(2, '0')}/${d.getUTCFullYear()}`;
  };

  // ── Filas ────────────────────────────────────────────────────────────────
  for (const m of movements) {
    const bankAmount = (m.deposito ?? 0) + (m.retiro ?? 0);
    const diferencia = m.saldoErp != null ? Math.abs(bankAmount - m.saldoErp) : null;

    const fechasAplicacion = [
      ...(m.identificadoPor || []).map(e => e.fechaId ? new Date(e.fechaId).getTime() : null),
      m.fichaAt ? new Date(m.fichaAt).getTime() : null,
    ].filter(Boolean);
    const fechaAplicacion = fechasAplicacion.length
      ? formatUTCDate(new Date(Math.max(...fechasAplicacion)))
      : null;

    const links   = m.erpLinks || [];
    const erpBase = links
      .map(l => (l.serie && l.folioExterno) ? `${l.serie}-${l.folioExterno}` : (l.folioExterno || l.erpId))
      .join(', ');
    const erpIdsFicha = [erpBase, m.ficha ?? null].filter(Boolean).join(' · ') || null;

    const rowData = {
      fecha:              formatUTCDate(m.fecha),
      banco:              m.banco ?? null,
      folio:              m.status === 'identificado' ? (m.folio ?? null) : null,
      concepto:           m.concepto ?? null,
      deposito:           m.deposito ?? null,
      retiro:             m.retiro   ?? null,
      status:             STATUS_LABELS[m.status] ?? m.status,
      categoria:          m.categoria ?? null,
      erpIds:             erpIdsFicha,
      saldoErp:           m.saldoErp ?? null,
      diferencia,
      fechaAplicacion,
      identificadoPor:    [...new Set([
                            ...(m.identificadoPor || []).map(e => e.nombre || e.userId || '?'),
                            ...(m.fichaNombre ? [m.fichaNombre] : m.fichaBy ? [m.fichaBy] : []),
                          ])].join(', ') || null,
      numeroAutorizacion: m.numeroAutorizacion ?? null,
    };

    if (colSet.has('folioFiscal'))
      rowData.folioFiscalCol = links.map(l => l.folioFiscal).filter(Boolean).join(', ') || null;
    if (colSet.has('formaPago'))
      rowData.formaPagoCol   = [...new Set(links.map(l => l.tipoPago).filter(Boolean))].join(', ') || null;
    if (colSet.has('retencion'))
      rowData.retencionCol   = links.length > 0 ? (links.some(l => l.tieneRetencion) ? 'Sí' : 'No') : null;
    if (colSet.has('ficha'))
      rowData.fichaCol       = m.ficha ?? null;

    sheet.addRow(rowData);
  }

  // ── Estilos ──────────────────────────────────────────────────────────────
  const numColKeys = new Set(['deposito', 'retiro', 'saldoErp', 'diferencia']);
  const numFmt     = '#,##0.00';

  const STATUS_COLORS = {
    'Identificado':    { fgColor: { argb: 'FFD1FAE5' }, fontColor: { argb: 'FF065F46' } },
    'No identificado': { fgColor: { argb: 'FFFEF9C3' }, fontColor: { argb: 'FF713F12' } },
    'Otros':           { fgColor: { argb: 'FFF1F5F9' }, fontColor: { argb: 'FF475569' } },
    'Reclasificado':   { fgColor: { argb: 'FFDBEAFE' }, fontColor: { argb: 'FF1E40AF' } },
  };

  const statusColIdx  = activeCols.findIndex(c => c.key === 'status') + 1;
  const depositColIdx = activeCols.findIndex(c => c.key === 'deposito') + 1;
  const retiroColIdx  = activeCols.findIndex(c => c.key === 'retiro') + 1;

  // Header
  const headerRow = sheet.getRow(1);
  headerRow.height = 22;
  headerRow.font   = { bold: true, color: { argb: 'FFE0E7FF' }, size: 10 };
  headerRow.fill   = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E1B4B' } };
  headerRow.alignment = { vertical: 'middle', horizontal: 'center' };
  headerRow.eachCell(cell => {
    cell.border = { bottom: { style: 'medium', color: { argb: 'FF4F46E5' } } };
  });

  // Data rows
  const evenFill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF8FAFF' } };
  sheet.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return;
    const isEven = rowNumber % 2 === 0;

    for (let col = 1; col <= activeCols.length; col++) {
      const cell   = row.getCell(col);
      const colKey = activeCols[col - 1].key;
      const isNum  = numColKeys.has(colKey);

      if (isEven) cell.fill = evenFill;
      cell.alignment = { vertical: 'middle', horizontal: isNum ? 'right' : 'left' };
      if (isNum && cell.value != null) cell.numFmt = numFmt;
    }

    // Estado: coloreado por valor
    if (statusColIdx > 0) {
      const sc    = row.getCell(statusColIdx);
      const style = STATUS_COLORS[sc.value];
      if (style) {
        sc.fill = { type: 'pattern', pattern: 'solid', fgColor: style.fgColor };
        sc.font = { bold: true, color: style.fontColor, size: 9.5 };
      }
    }

    // Depósito → verde, Retiro → rojo
    if (depositColIdx > 0) {
      const c = row.getCell(depositColIdx);
      if (c.value != null) c.font = { bold: true, color: { argb: 'FF15803D' } };
    }
    if (retiroColIdx > 0) {
      const c = row.getCell(retiroColIdx);
      if (c.value != null) c.font = { bold: true, color: { argb: 'FFB91C1C' } };
    }
  });

  // Encabezado congelado
  sheet.views = [{ state: 'frozen', ySplit: 1 }];

  return workbook.xlsx.writeBuffer();
}

async function deleteMovements(ids) {
  if (!Array.isArray(ids) || ids.length === 0) throw new BadRequestError('Se requiere al menos un ID');
  const result = await BankMovement.deleteMany({ _id: { $in: ids } });
  return { deleted: result.deletedCount };
}

async function reclasifyMovements(ids) {
  if (!Array.isArray(ids) || ids.length === 0) throw new BadRequestError('Se requiere al menos un ID');
  const result = await BankMovement.updateMany(
    { _id: { $in: ids } },
    { $set: { status: 'reclasificado' } }
  );
  return { reclasified: result.modifiedCount };
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

  // Reflejar status cuando la categoría cambia manualmente
  if ('categoria' in data) {
    if (mov.categoria) {
      mov.status = 'reclasificado';
    } else if (mov.status === 'reclasificado') {
      mov.status = (mov.erpIds?.length ?? 0) > 0 ? 'identificado' : 'no_identificado';
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

// ── findPotentialDuplicates ───────────────────────────────────────────────────
// Detecta movimientos que, aunque tienen hashes distintos (pasaron la dedup por
// hash), comparten campos clave que los hacen sospechosamente iguales.
//
// Cuatro criterios en orden de prioridad:
//  1. importe_saldo_fecha  — banco + día + monto + saldo (concepto diferente)
//  2. importe_saldo_auth   — banco + monto + saldo + auth (sin restricción de fecha)
//  3. importe_fecha_auth   — banco + día + monto + auth (saldo diferente)
//  4. auth_monto_sin_saldo — banco + día + monto con relación de auth (DEP MIXTO)
async function findPotentialDuplicates() {
  // Campo calculado reutilizado en varias estrategias: normaliza auth numérico
  // para equiparar "00199480" con "199480" (datos históricos vs. nuevos).
  const authNormField = {
    $cond: {
      if:   { $regexMatch: { input: '$numeroAutorizacion', regex: /^\d+$/ } },
      then: { $toString: { $toLong: '$numeroAutorizacion' } },
      else: '$numeroAutorizacion',
    },
  };

  const [
    byImporteSaldoFecha,
    byImporteSaldoAuth,
    byImporteFechaAuth,
    byMontoSinSaldo,
  ] = await Promise.all([

    // 1. importe+saldo+fecha — mismo banco + día + monto + saldo, concepto distinto
    BankMovement.aggregate([
      { $match: { isActive: true } },
      {
        $group: {
          _id: {
            banco:    '$banco',
            dia:      { $dateToString: { format: '%Y-%m-%d', date: '$fecha' } },
            deposito: '$deposito',
            retiro:   '$retiro',
            saldo:    '$saldo',
          },
          count: { $sum: 1 },
          ids:   { $push: '$_id' },
        },
      },
      { $match: { count: { $gte: 2 } } },
      { $sort: { '_id.dia': -1, '_id.banco': 1 } },
    ]),

    // 2. importe+saldo+auth — mismo banco + monto + saldo + auth (sin fecha)
    // Detecta el mismo movimiento importado desde archivos de periodos distintos.
    BankMovement.aggregate([
      { $match: { isActive: true, numeroAutorizacion: { $nin: [null, ''] } } },
      { $addFields: { _authNorm: authNormField } },
      {
        $group: {
          _id: {
            banco:    '$banco',
            deposito: '$deposito',
            retiro:   '$retiro',
            saldo:    '$saldo',
            authKey:  '$_authNorm',
          },
          count: { $sum: 1 },
          ids:   { $push: '$_id' },
        },
      },
      { $match: { count: { $gte: 2 } } },
      { $sort: { '_id.banco': 1 } },
    ]),

    // 3. importe+fecha+auth — mismo banco + día + monto + auth, saldo diferente
    // Captura duplicados donde el balance difiere (registro en distinto orden)
    // pero la transacción es inequívoca: mismo auth + monto + fecha.
    BankMovement.aggregate([
      { $match: { isActive: true, numeroAutorizacion: { $nin: [null, ''] } } },
      { $addFields: { _authNorm: authNormField } },
      {
        $group: {
          _id: {
            banco:    '$banco',
            dia:      { $dateToString: { format: '%Y-%m-%d', date: '$fecha' } },
            deposito: '$deposito',
            retiro:   '$retiro',
            authKey:  '$_authNorm',
          },
          count: { $sum: 1 },
          ids:   { $push: '$_id' },
        },
      },
      { $match: { count: { $gte: 2 } } },
      { $sort: { '_id.dia': -1, '_id.banco': 1 } },
    ]),

    // 4. auth+monto (sin saldo) — mismo banco + día + monto, con relación de auth
    // Patrón DEP MIXTO Banamex: fila A tiene auth, fila B no; mismo monto y día.
    // La validación post-aggregation filtra coincidencias accidentales de monto.
    BankMovement.aggregate([
      { $match: { isActive: true } },
      {
        $group: {
          _id: {
            banco:    '$banco',
            dia:      { $dateToString: { format: '%Y-%m-%d', date: '$fecha' } },
            deposito: '$deposito',
            retiro:   '$retiro',
          },
          count: { $sum: 1 },
          ids:   { $push: '$_id' },
          auths: { $push: '$numeroAutorizacion' },
        },
      },
      { $match: { count: { $gte: 2 } } },
      { $sort: { '_id.dia': -1, '_id.banco': 1 } },
    ]),
  ]);

  // ── Construir mapa de grupos (deduplicando por conjunto de IDs) ───────────
  // El orden de los loops define la prioridad: el primer criterio en ver un
  // conjunto de IDs lo "gana". Los más específicos van primero.
  const seen = new Map();

  for (const g of byImporteSaldoFecha) {
    const key = g.ids.map(id => id.toString()).sort().join('|');
    if (!seen.has(key)) {
      seen.set(key, { ids: g.ids, criterio: 'importe_saldo_fecha', meta: g._id, count: g.count });
    }
  }

  for (const g of byImporteSaldoAuth) {
    const key = g.ids.map(id => id.toString()).sort().join('|');
    if (!seen.has(key)) {
      seen.set(key, { ids: g.ids, criterio: 'importe_saldo_auth', meta: g._id, count: g.count });
    }
  }

  for (const g of byImporteFechaAuth) {
    const key = g.ids.map(id => id.toString()).sort().join('|');
    if (!seen.has(key)) {
      seen.set(key, { ids: g.ids, criterio: 'importe_fecha_auth', meta: g._id, count: g.count });
    }
  }

  for (const g of byMontoSinSaldo) {
    const key = g.ids.map(id => id.toString()).sort().join('|');
    if (seen.has(key)) continue;
    // Dos patrones válidos para evitar falsos positivos por coincidencia de monto:
    //   a) authCompartido — el mismo auth en 2+ docs: mismo mov importado dos veces.
    //   b) authMixto      — al menos un doc con auth y uno sin: patrón DEP MIXTO.
    const authCounts = new Map();
    let docsConAuth = 0;
    for (const a of g.auths) {
      if (!a || a === '' || a === '0') continue;
      docsConAuth++;
      const norm = /^\d+$/.test(a) ? String(parseInt(a, 10)) : a;
      authCounts.set(norm, (authCounts.get(norm) || 0) + 1);
    }
    const authCompartido = [...authCounts.values()].some(c => c >= 2);
    const authMixto      = docsConAuth >= 1 && docsConAuth < g.count;
    if (!authCompartido && !authMixto) continue;
    seen.set(key, { ids: g.ids, criterio: 'auth_monto_sin_saldo', meta: g._id, count: g.count });
  }

  if (seen.size === 0) return { total: 0, grupos: [] };

  // ── Recuperar documentos completos en una sola consulta ───────────────────
  const allIds = [...seen.values()].flatMap(g => g.ids);
  const docs   = await BankMovement.find(
    { _id: { $in: allIds } },
    { hash: 0 }, // nunca exponer el hash al cliente
  ).lean();

  const byId = new Map(docs.map(d => [d._id.toString(), d]));

  // ── Construir y ordenar la respuesta ──────────────────────────────────────
  const grupos = [];
  for (const [, group] of seen) {
    const movimientos = group.ids
      .map(id => byId.get(id.toString()))
      .filter(Boolean);

    if (movimientos.length >= 2) {
      grupos.push({
        criterio:    group.criterio,
        meta:        group.meta,
        count:       movimientos.length,
        movimientos,
      });
    }
  }

  // Fecha descendente; grupos sin día (importe_saldo_auth, cruza fechas) van al final.
  grupos.sort((a, b) => {
    const da = a.meta.dia ?? '';
    const db = b.meta.dia ?? '';
    if (da > db) return -1;
    if (da < db) return  1;
    return (a.meta.banco ?? '').localeCompare(b.meta.banco ?? '');
  });

  // Límite DESPUÉS del merge — un grupo detectado por N estrategias consume 1 slot.
  const gruposFinal = grupos.slice(0, 500);
  return { total: gruposFinal.length, grupos: gruposFinal };
}

// ── identificarAnterioresAMayo ────────────────────────────────────────────────
// Marca como 'identificado' TODOS los movimientos activos con status
// 'no_identificado' y fecha anterior al 1 de mayo.
// • No toca movimientos con status 'identificado' u 'otros'.
// • Registra la autoría en identificadoPor con userId = MOTOR_ID_HISTORICO
//   para que el revert sea selectivo y preciso.
// • La operación es atómica por documento (MongoDB updateMany con pipeline).
const MOTOR_ID_HISTORICO     = 'admin-bulk-anterior';
const MOTOR_NOMBRE_HISTORICO = 'Identificación masiva pre-mayo';
const CORTE_MAYO             = new Date('2026-05-01T00:00:00.000Z');

// Todos los userIds de motores conocidos — para distinguir identificaciones
// automáticas de las hechas manualmente por un usuario humano en el revert.
const TODOS_MOTORES_HISTORICO = [
  'erp-auto', 'aut-match', 'refact-cyc', 'mostrador-cyc', 'pagos-cyc',
  MOTOR_ID_HISTORICO,
];

async function identificarAnterioresAMayo() {
  const resultado = await BankMovement.updateMany(
    {
      isActive: true,
      status:   'no_identificado',
      fecha:    { $lt: CORTE_MAYO },
      deposito: { $gt: 0 },
    },
    [
      {
        $set: {
          status: 'identificado',
          identificadoPor: {
            $concatArrays: [
              { $ifNull: ['$identificadoPor', []] },
              [{
                userId:  MOTOR_ID_HISTORICO,
                nombre:  MOTOR_NOMBRE_HISTORICO,
                fechaId: '$$NOW',
                erpId:   null,
              }],
            ],
          },
        },
      },
    ],
  );
  return {
    marcados: resultado.modifiedCount,
    message:  `${resultado.modifiedCount} movimientos anteriores al 1 de mayo marcados como identificados`,
  };
}

// ── revertirAnterioresAMayo ───────────────────────────────────────────────────
// Deshace exclusivamente lo que hizo identificarAnterioresAMayo:
// • Solo actúa sobre movimientos que aún están en status 'identificado'
//   y tienen la entrada admin-bulk-anterior en identificadoPor.
//   Si un humano cambió el status manualmente, no se toca.
// • Elimina la entrada admin-bulk-anterior de identificadoPor.
// • Resetea status a 'no_identificado' ÚNICAMENTE si no quedan entradas de
//   usuarios humanos (userId que no sea de ningún motor).
//   Si un humano también identificó el mismo movimiento, se conserva 'identificado'.
async function revertirAnterioresAMayo() {
  const resultado = await BankMovement.updateMany(
    {
      isActive: true,                              // espejo del filtro de identificarAnterioresAMayo
      'identificadoPor.userId': MOTOR_ID_HISTORICO,
      status: 'identificado',
    },
    [
      {
        $set: {
          // Quitar solo la entrada del motor de identificación masiva
          identificadoPor: {
            $filter: {
              input: { $ifNull: ['$identificadoPor', []] },
              as:    'entry',
              cond:  { $ne: ['$$entry.userId', MOTOR_ID_HISTORICO] },
            },
          },
          // Resetear status solo si en el array ORIGINAL no hay entradas humanas.
          // (Las expresiones en un mismo $set usan el documento de entrada, no el
          //  estado parcialmente mutado — evaluar sobre '$identificadoPor' original
          //  da el mismo resultado que sobre el array ya filtrado porque estamos
          //  preguntando "¿hay humanos?" antes o después de quitar el motor.)
          status: {
            $cond: {
              if: {
                $eq: [
                  {
                    $size: {
                      $filter: {
                        input: { $ifNull: ['$identificadoPor', []] },
                        as:    'e',
                        cond:  { $not: { $in: ['$$e.userId', TODOS_MOTORES_HISTORICO] } },
                      },
                    },
                  },
                  0,
                ],
              },
              then: 'no_identificado',
              else: '$status',
            },
          },
        },
      },
    ],
  );
  return {
    revertidos: resultado.modifiedCount,
    message:    `${resultado.modifiedCount} movimientos revertidos a "no identificado"`,
  };
}

// ── importarConciliacion ──────────────────────────────────────────────────────
// Lee un Excel con columnas fecha_deposito, banco, monto_deposito y marca como
// 'identificado' cada movimiento que coincida con status 'no_identificado'.
// • No toca movimientos ya identificados, en estado 'otros' ni manipulados.
// • Registra la autoría con source: SOURCE_CONCILIACION y un runId único por
//   operación para que el revert sea exactamente selectivo.
const SOURCE_CONCILIACION = 'conciliacion-import';

async function _parseConciliacionExcel(buffer) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(buffer);
  const sheet = workbook.worksheets[0];
  if (!sheet) throw new Error('El archivo no contiene hojas de cálculo');

  const headerMap = {};
  sheet.getRow(1).eachCell({ includeEmpty: false }, (cell, col) => {
    const key = String(cell.value ?? '').trim().toLowerCase().replace(/\s+/g, '_');
    headerMap[col] = key;
  });

  const rows = [];
  sheet.eachRow({ includeEmpty: false }, (_row, rowNum) => {
    if (rowNum === 1) return;
    const obj = {};
    _row.eachCell({ includeEmpty: true }, (cell, col) => {
      const key = headerMap[col];
      if (!key) return;
      const v = cell.value;
      if (v === null || v === undefined) { obj[key] = null; return; }
      if (typeof v === 'object') {
        if ('result'   in v) { obj[key] = v.result; return; }                             // fórmula
        if ('richText' in v) { obj[key] = v.richText.map(t => t.text ?? '').join(''); return; } // texto enriquecido
      }
      obj[key] = v;
    });
    rows.push(obj);
  });

  return rows.map(row => {
    const rawFecha = row['fecha_deposito'];
    const rawBanco = row['banco'];
    const rawMonto = row['monto_deposito'];

    let fecha = null;
    if (rawFecha instanceof Date && !isNaN(rawFecha.getTime())) {
      fecha = rawFecha;
    } else if (typeof rawFecha === 'number' && rawFecha > 25000) {
      // Serial numérico de Excel cuando la celda no está formateada como fecha
      // 25569 = días entre 1900-01-00 (Excel epoch) y 1970-01-01 (Unix epoch)
      const d = new Date((rawFecha - 25569) * 86400000);
      if (!isNaN(d.getTime())) fecha = d;
    } else if (typeof rawFecha === 'string' && rawFecha.trim()) {
      const s = rawFecha.trim();
      let parsed = new Date(s);
      if (isNaN(parsed.getTime())) {
        // Formato DD/MM/YYYY o DD-MM-YYYY (común en México)
        const parts = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
        if (parts) parsed = new Date(Date.UTC(+parts[3], +parts[2] - 1, +parts[1]));
      }
      if (!isNaN(parsed.getTime())) fecha = parsed;
    }

    const banco = String(rawBanco ?? '').trim();
    const rawNum = typeof rawMonto === 'number'
      ? rawMonto
      : parseFloat(String(rawMonto ?? '0').replace(/,/g, ''));
    const monto = Math.round(rawNum * 100) / 100;

    if (!fecha || !banco || !monto || monto <= 0) return null;
    return { fecha, banco, monto };
  }).filter(Boolean);
}

async function importarConciliacion(buffer, user) {
  const runId = randomUUID();

  let rows;
  try {
    rows = await _parseConciliacionExcel(buffer);
  } catch (err) {
    throw new BadRequestError(`Error al leer el archivo: ${err.message}`);
  }

  if (rows.length === 0) {
    throw new BadRequestError(
      'El archivo no contiene filas válidas (verifica las columnas fecha_deposito, banco, monto_deposito)',
    );
  }

  let identificados = 0;
  const fallidosDetalle = [];

  for (const { fecha, banco, monto } of rows) {
    // Rango del día completo en UTC para cubrir la fecha sin importar la hora guardada
    const dayStart = new Date(Date.UTC(fecha.getUTCFullYear(), fecha.getUTCMonth(), fecha.getUTCDate()));
    const dayEnd   = new Date(Date.UTC(fecha.getUTCFullYear(), fecha.getUTCMonth(), fecha.getUTCDate() + 1));

    const updated = await BankMovement.findOneAndUpdate(
      {
        isActive: true,
        status:   'no_identificado',
        fecha:    { $gte: dayStart, $lt: dayEnd },
        banco:    { $regex: new RegExp(`^${banco.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') },
        deposito: monto,
      },
      {
        $set:  { status: 'identificado' },
        $push: {
          identificadoPor: {
            userId:  user._id,
            nombre:  user.nombre ?? user._id,
            fechaId: new Date(),
            erpId:   null,
            source:  SOURCE_CONCILIACION,
            runId,
          },
        },
      },
    );

    if (updated) {
      identificados++;
    } else {
      fallidosDetalle.push({ fecha: fecha.toISOString().slice(0, 10), banco, monto });
    }
  }

  return { runId, total: rows.length, identificados, fallidos: fallidosDetalle.length, fallidosDetalle };
}

// ── revertirConciliacion ──────────────────────────────────────────────────────
// Deshace exactamente lo que aplicó importarConciliacion para un runId concreto:
// • Solo actúa sobre movimientos con esa combinación userId + SOURCE + runId.
// • Elimina la entrada del array identificadoPor.
// • Resetea status a 'no_identificado' solo si no quedan entradas de usuarios
//   humanos (userId fuera de TODOS_MOTORES_HISTORICO, excluyendo la entrada
//   que estamos quitando).
// • No toca identificaciones manuales del mismo usuario en otro contexto.
async function revertirConciliacion(runId, userId) {
  const resultado = await BankMovement.updateMany(
    {
      isActive: true,
      status:   'identificado',
      identificadoPor: {
        $elemMatch: { userId, source: SOURCE_CONCILIACION, runId },
      },
    },
    [
      {
        $set: {
          identificadoPor: {
            $filter: {
              input: { $ifNull: ['$identificadoPor', []] },
              as:    'entry',
              cond: {
                $not: {
                  $and: [
                    { $eq: ['$$entry.userId',  userId] },
                    { $eq: ['$$entry.source', SOURCE_CONCILIACION] },
                    { $eq: ['$$entry.runId',  runId] },
                  ],
                },
              },
            },
          },
          // Resetear a no_identificado solo si, quitando esa entrada, no quedan
          // entradas de usuarios humanos. Las expresiones $set del pipeline operan
          // sobre el doc original, así que excluimos explícitamente la entrada
          // que vamos a quitar al contar entradas humanas restantes.
          status: {
            $cond: {
              if: {
                $eq: [
                  {
                    $size: {
                      $filter: {
                        input: { $ifNull: ['$identificadoPor', []] },
                        as:    'e',
                        cond: {
                          $and: [
                            { $not: { $in: ['$$e.userId', TODOS_MOTORES_HISTORICO] } },
                            {
                              $not: {
                                $and: [
                                  { $eq: ['$$e.userId',  userId] },
                                  { $eq: ['$$e.source', SOURCE_CONCILIACION] },
                                  { $eq: ['$$e.runId',  runId] },
                                ],
                              },
                            },
                          ],
                        },
                      },
                    },
                  },
                  0,
                ],
              },
              then: 'no_identificado',
              else: '$status',
            },
          },
        },
      },
    ],
  );

  return {
    revertidos: resultado.modifiedCount,
    message:    `${resultado.modifiedCount} movimiento(s) revertido(s) a "no identificado"`,
  };
}

module.exports = {
  getCards, listMovements, getSummary, getStatusStats,
  importFile, updateStatus, updateErpIds, setErpIds, setFicha, deleteFicha,
  getConfig, saveConfig, setSaldoInicial, listCategories, listIdentificadores, importIndividual,
  exportMovements, deleteMovements, reclasifyMovements, updateMovement, updateCategoria, generateTemplate,
  findPotentialDuplicates,
  identificarAnterioresAMayo, revertirAnterioresAMayo,
  importarConciliacion, revertirConciliacion,
};
