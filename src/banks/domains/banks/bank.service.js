'use strict';

const BankMovement      = require('./BankMovement.model');
const bankConfigRepo    = require('./repositories/bank-config.repository');
const Counter           = require('../../shared/models/Counter');
const CollectionRequest = require('../collection-requests/CollectionRequest.model');
const { parseBankFile } = require('./bank.parser');
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
                { $subtract: [{ $ifNull: ['$deposito', 0] }, { $ifNull: ['$retiro', 0] }] },
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

  return agg.map((b) => {
    const cfg = configMap.get(b._id);
    return {
      banco:          b._id,
      movimientos:    b.movimientos,
      totalDepositos: b.totalDepositos,
      totalRetiros:   b.totalRetiros,
      saldoFinal:     b.saldoFinal ?? null,
      ultimaFecha:    b.ultimaFecha,
      ultimaImport:   b.ultimaImport,
      cuentaContable: cfg?.cuentaContable ?? null,
      numeroCuenta:   cfg?.numeroCuenta   ?? null,
      saldoPendiente:    b.saldoPendiente    ?? 0,
      saldoIdentificado: b.saldoIdentificado ?? 0,
      saldoOtros:        b.saldoOtros        ?? 0,
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
    movId,
  } = filters;

  const filter = { isActive: true };
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
    filter.$and.push({ 'identificadoPor.userId': { $in: ids } });
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

    // Búsqueda por monto
    const cleanNum = search.replace(/[$,\s]/g, '');
    const num = parseFloat(cleanNum);
    if (!isNaN(num) && num > 0) {
      orClauses.push({ deposito: { $gte: num - 0.005, $lte: num + 0.005 } });
      orClauses.push({ retiro:   { $gte: num - 0.005, $lte: num + 0.005 } });
    }

    // Búsqueda por fecha
    const dmyMatch = search.match(/^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$/);
    const ymdMatch = search.match(/^(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})$/);
    let searchDate = null;
    if (dmyMatch) {
      searchDate = new Date(parseInt(dmyMatch[3]), parseInt(dmyMatch[2]) - 1, parseInt(dmyMatch[1]));
    } else if (ymdMatch) {
      searchDate = new Date(parseInt(ymdMatch[1]), parseInt(ymdMatch[2]) - 1, parseInt(ymdMatch[3]));
    }
    if (searchDate && !isNaN(searchDate.getTime())) {
      const nextDay = new Date(searchDate);
      nextDay.setDate(nextDay.getDate() + 1);
      orClauses.push({ fecha: { $gte: searchDate, $lt: nextDay } });
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

  const data = movements.map(m => ({
    ...m,
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

async function importFile(buffer, banco, userId, { auth0Sub } = {}) {
  const bancoValidado = BANCOS_VALIDOS.includes(banco) ? banco : null;
  const { movements, summary, errors } = await parseBankFile(buffer, bancoValidado);

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
    'hash',
  ).lean();
  const hashesExistentes = new Set(existentes.map(e => e.hash));

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

    // ── 4. Aplicar reglas a los movimientos recién insertados ─────────────────
    let categorizados  = 0;
    let sinReglasAviso = false;

    if (insertados > 0 && bancoValidado) {
      const rules = await bankRuleRepo.listByBanco(bancoValidado, { accion: 'categorizar' });

      if (rules.length === 0) {
        sinReglasAviso = true;
      } else {
        const foliosNuevos   = nuevos.map(m => m.folio);
        const docsInsertados = await BankMovement.find(
          { folio: { $in: foliosNuevos }, isActive: true },
        ).lean();

        const ops = [];
        for (const mov of docsInsertados) {
          for (const rule of rules) {
            if (matchRegla(mov, rule)) {
              ops.push({
                updateOne: {
                  filter: { _id: mov._id },
                  update: { $set: { categoria: rule.nombre } },
                },
              });
              categorizados++;
              break; // Primera regla que aplica gana
            }
          }
        }

        if (ops.length) {
          await BankMovement.bulkWrite(ops, { ordered: false });
        }
      }
    }

    return {
      message:      `${insertados} movimientos importados, ${duplicados} ya existían`,
      importados:   insertados,
      duplicados,
      categorizados,
      sinReglas:    sinReglasAviso,
      resumen:      summary,
      erroresHojas: errors,
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
    const rules = await bankRuleRepo.listByBanco(bancoValidado, { accion: 'categorizar' });

    for (const rule of rules) {
      if (matchRegla(nuevo, rule)) {
        nuevo.categoria = rule.nombre;
        await nuevo.save();
        categorizado = true;
        break;
      }
    }
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
  const status     = (saldoErp !== null && Math.abs(bankAmount - saldoErp) <= ERP_TOLERANCE)
    ? 'identificado'
    : 'no_identificado';
  return { saldoErp, uuidXML, status };
}

async function updateStatus(id, status, user) {
  if (!STATUS_VALIDOS.includes(status)) {
    throw new BadRequestError(`Status inválido. Debe ser: ${STATUS_VALIDOS.join(', ')}`);
  }
  const mov = await BankMovement.findById(id);
  if (!mov) throw new NotFoundError('Movimiento');
  const isAdmin = user?.role === 'admin';
  // Bloquear si el cuadre ERP determinó automáticamente el status (admin puede forzar)
  const bankAmount = (mov.deposito ?? mov.retiro ?? 0);
  if (!isAdmin && mov.saldoErp !== null && Math.abs(bankAmount - mov.saldoErp) <= ERP_TOLERANCE) {
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
      erpId:       String(l.erpId || '').trim(),
      saldoActual: l.saldoActual != null ? Number(l.saldoActual) : null,
      folioFiscal: l.folioFiscal ? String(l.folioFiscal).trim().toUpperCase() : null,
      total:       l.total != null ? Number(l.total) : null,
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

  // Actualizar identificadoPor: añadir entradas para CxCs nuevas, quitar las eliminadas
  const prevIds       = new Set((mov.identificadoPor || []).map(e => e.erpId));
  const newIds        = new Set(cleanLinks.map(l => l.erpId));
  const displayName   = user?.nombre || user?.email || null;
  const addedErpIds   = cleanLinks.filter(l => !prevIds.has(l.erpId)).map(l => l.erpId);
  const removedErpIds = [...prevIds].filter(id => !newIds.has(id));

  let updatedIdPor = (mov.identificadoPor || []).filter(e => !removedErpIds.includes(e.erpId));
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

async function listIdentificadores(banco) {
  const docs = await BankMovement.aggregate([
    { $match: { banco, isActive: true, 'identificadoPor.userId': { $ne: null } } },
    { $group: { _id: '$identificadoPor.userId', nombre: { $first: '$identificadoPor.nombre' } } },
    { $sort: { nombre: 1 } },
  ]);
  return docs.map(d => ({ userId: d._id, nombre: d.nombre || d._id }));
}

async function listCategories(banco) {
  const values = await BankMovement.distinct('categoria', { banco, isActive: true });
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
    tipo, search, concepto,
    sortBy = 'fecha', sortDir = 'desc',
    status, categorias,
  } = filters;

  const filter = { isActive: true };
  if (banco)  filter.banco  = banco;
  if (status) filter.status = status;

  if (categorias) {
    const vals = categorias.split(',').map(v => v === '__null__' ? null : v);
    filter.categoria = { $in: vals };
  }
  if (tipo === 'deposito') filter.deposito = { $gt: 0 };
  if (tipo === 'retiro')   filter.retiro   = { $gt: 0 };

  if (concepto) {
    const esc = concepto.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    filter.concepto = new RegExp(esc, 'i');
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
    const cleanNum = search.replace(/[$,\s]/g, '');
    const num = parseFloat(cleanNum);
    if (!isNaN(num) && num > 0) {
      orClauses.push({ deposito: { $gte: num - 0.005, $lte: num + 0.005 } });
      orClauses.push({ retiro:   { $gte: num - 0.005, $lte: num + 0.005 } });
    }
    const dmyMatch = search.match(/^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$/);
    const ymdMatch = search.match(/^(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})$/);
    let searchDate = null;
    if (dmyMatch) {
      searchDate = new Date(parseInt(dmyMatch[3]), parseInt(dmyMatch[2]) - 1, parseInt(dmyMatch[1]));
    } else if (ymdMatch) {
      searchDate = new Date(parseInt(ymdMatch[1]), parseInt(ymdMatch[2]) - 1, parseInt(ymdMatch[3]));
    }
    if (searchDate && !isNaN(searchDate.getTime())) {
      const nextDay = new Date(searchDate);
      nextDay.setDate(nextDay.getDate() + 1);
      orClauses.push({ fecha: { $gte: searchDate, $lt: nextDay } });
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
    { header: 'Folio',            key: 'folio',              width: 10 },
    { header: 'Fecha',            key: 'fecha',              width: 14 },
    { header: 'Concepto',         key: 'concepto',           width: 50 },
    { header: 'Depósito',         key: 'deposito',           width: 16 },
    { header: 'Retiro',           key: 'retiro',             width: 16 },
    { header: 'Saldo',            key: 'saldo',              width: 16 },
    { header: 'Categoría',        key: 'categoria',          width: 20 },
    { header: 'Estado',           key: 'status',             width: 16 },
    { header: 'IDs ERP',          key: 'erpIds',             width: 30 },
    { header: 'Saldo ERP',        key: 'saldoErp',           width: 16 },
    { header: 'N° Autorización',  key: 'numeroAutorizacion', width: 20 },
    { header: 'Identificado por', key: 'identificadoPor',    width: 24 },
  ];

  const STATUS_LABELS = {
    no_identificado: 'No identificado',
    identificado:    'Identificado',
    otros:           'Otros',
  };

  for (const m of movements) {
    const d = m.fecha ? new Date(m.fecha) : null;
    const fechaStr = d
      ? `${String(d.getUTCDate()).padStart(2, '0')}/${String(d.getUTCMonth() + 1).padStart(2, '0')}/${d.getUTCFullYear()}`
      : null;
    sheet.addRow({
      folio:              m.status === 'identificado' ? (m.folio ?? null) : null,
      fecha:              fechaStr,
      concepto:           m.concepto ?? null,
      deposito:           m.deposito ?? null,
      retiro:             m.retiro   ?? null,
      saldo:              m.saldo    ?? null,
      categoria:          m.categoria ?? null,
      status:             STATUS_LABELS[m.status] ?? m.status,
      erpIds:             (m.erpIds || []).join(', ') || null,
      saldoErp:           m.saldoErp ?? null,
      numeroAutorizacion: m.numeroAutorizacion ?? null,
      identificadoPor:    m.identificadoPor?.nombre ?? null,
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

module.exports = {
  getCards, listMovements, getSummary,
  importFile, updateStatus, updateErpIds, setErpIds,
  getConfig, saveConfig, listCategories, listIdentificadores, importIndividual,
  exportMovements,
};
