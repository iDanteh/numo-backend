const { validationResult } = require('express-validator');
const Comparison = require('../models/Comparison');
const ComparisonSession = require('../models/ComparisonSession');
const CFDI = require('../models/CFDI');
const { batchCompareCFDIs, formatSessionName } = require('../services/comparisonEngine');
const { asyncHandler } = require('../middleware/errorHandler');
const { paginate, skip } = require('../utils/pagination');

/**
 * GET /api/comparisons/ejercicios/resumen
 */
const ejerciciosResumen = asyncHandler(async (req, res) => {
  const rows = await Comparison.aggregate([
    { $match: { ejercicio: { $exists: true, $ne: null } } },
    {
      $group: {
        _id: { ejercicio: '$ejercicio', periodo: '$periodo' },
        total:            { $sum: 1 },
        match:            { $sum: { $cond: [{ $eq: ['$status', 'match'] },       1, 0] } },
        discrepancy:      { $sum: { $cond: [{ $eq: ['$status', 'discrepancy'] }, 1, 0] } },
        not_in_sat:       { $sum: { $cond: [{ $eq: ['$status', 'not_in_sat'] },  1, 0] } },
        cancelled:        { $sum: { $cond: [{ $eq: ['$status', 'cancelled'] },   1, 0] } },
        error:            { $sum: { $cond: [{ $eq: ['$status', 'error'] },       1, 0] } },
        totalDifferences: { $sum: '$totalDifferences' },
      },
    },
    { $sort: { '_id.ejercicio': -1, '_id.periodo': 1 } },
    {
      $project: {
        _id: 0,
        ejercicio: '$_id.ejercicio', periodo: '$_id.periodo',
        total: 1, match: 1, discrepancy: 1, not_in_sat: 1,
        cancelled: 1, error: 1, totalDifferences: 1,
      },
    },
  ]);

  const map = {};
  for (const r of rows) {
    if (!map[r.ejercicio]) {
      map[r.ejercicio] = {
        ejercicio: r.ejercicio, total: 0, match: 0, discrepancy: 0,
        not_in_sat: 0, cancelled: 0, error: 0, totalDifferences: 0, periodos: [],
      };
    }
    const ej = map[r.ejercicio];
    ej.total           += r.total;
    ej.match           += r.match;
    ej.discrepancy     += r.discrepancy;
    ej.not_in_sat      += r.not_in_sat;
    ej.cancelled       += r.cancelled;
    ej.error           += r.error;
    ej.totalDifferences += r.totalDifferences;
    ej.periodos.push(r);
  }

  res.json({ data: Object.values(map).sort((a, b) => b.ejercicio - a.ejercicio) });
});

/**
 * GET /api/comparisons/periodos
 */
const periodos = asyncHandler(async (req, res) => {
  const raw = await Comparison.aggregate([
    { $match: { ejercicio: { $exists: true, $ne: null } } },
    { $group: { _id: { ejercicio: '$ejercicio', periodo: '$periodo' } } },
    { $sort: { '_id.ejercicio': -1, '_id.periodo': 1 } },
    { $project: { _id: 0, ejercicio: '$_id.ejercicio', periodo: '$_id.periodo' } },
  ]);
  const ejercicios = [...new Set(raw.map(p => p.ejercicio))].sort((a, b) => b - a);
  res.json({ periodos: raw, ejercicios });
});

/**
 * GET /api/comparisons
 */
const list = asyncHandler(async (req, res) => {
  const { page = 1, limit = 20, status, resolved, dateFrom, dateTo, ejercicio, periodo, tipo } = req.query;
  const pg = parseInt(page);
  const lm = parseInt(limit);

  const filter = {};
  if (status)    filter.status    = status;
  if (resolved !== undefined) filter.resolved = resolved === 'true';
  if (ejercicio) filter.ejercicio = parseInt(ejercicio);
  if (periodo)   filter.periodo   = parseInt(periodo);
  if (tipo)      filter.tipoDeComprobante = tipo;
  if (dateFrom || dateTo) {
    filter.comparedAt = {};
    if (dateFrom) filter.comparedAt.$gte = new Date(dateFrom);
    if (dateTo)   filter.comparedAt.$lte = new Date(dateTo);
  }

  const [comparisons, total] = await Promise.all([
    Comparison.find(filter, { satRawResponse: 0 })
      .populate('erpCfdiId', 'uuid serie folio tipoDeComprobante emisor receptor total moneda satStatus fecha')
      .sort({ comparedAt: -1 })
      .skip(skip(pg, lm))
      .limit(lm)
      .lean(),
    Comparison.countDocuments(filter),
  ]);

  res.json(paginate(comparisons, total, pg, lm));
});

/**
 * GET /api/comparisons/stats
 */
const stats = asyncHandler(async (req, res) => {
  const { ejercicio, periodo } = req.query;
  const match = {};
  if (ejercicio) match.ejercicio = parseInt(ejercicio);
  if (periodo)   match.periodo   = parseInt(periodo);

  const result = await Comparison.aggregate([
    ...(Object.keys(match).length ? [{ $match: match }] : []),
    { $group: { _id: '$status', count: { $sum: 1 }, totalDifferences: { $sum: '$totalDifferences' } } },
    { $sort: { count: -1 } },
  ]);

  const total = result.reduce((acc, s) => acc + s.count, 0);
  res.json({ total, byStatus: result });
});

/**
 * GET /api/comparisons/sessions
 */
const listSessions = asyncHandler(async (req, res) => {
  const { page = 1, limit = 20 } = req.query;
  const pg = parseInt(page);
  const lm = parseInt(limit);

  const [sessions, total] = await Promise.all([
    ComparisonSession.find()
      .populate('triggeredBy', 'name email')
      .sort({ startedAt: -1 })
      .skip(skip(pg, lm))
      .limit(lm)
      .lean(),
    ComparisonSession.countDocuments(),
  ]);

  res.json(paginate(sessions, total, pg, lm));
});

/**
 * GET /api/comparisons/sessions/:id
 */
const getSession = asyncHandler(async (req, res) => {
  const { page = 1, limit = 20 } = req.query;
  const pg = parseInt(page);
  const lm = parseInt(limit);

  const session = await ComparisonSession.findById(req.params.id)
    .populate('triggeredBy', 'name email')
    .lean();
  if (!session) return res.status(404).json({ error: 'Sesión no encontrada' });

  const [comparisons, total] = await Promise.all([
    Comparison.find({ sessionId: req.params.id }, { satRawResponse: 0 })
      .populate('erpCfdiId', 'uuid serie folio fecha tipoDeComprobante emisor receptor total subTotal moneda satStatus satLastCheck source erpId')
      .sort({ comparedAt: -1 })
      .skip(skip(pg, lm))
      .limit(lm)
      .lean(),
    Comparison.countDocuments({ sessionId: req.params.id }),
  ]);

  res.json({
    session,
    comparisons: paginate(comparisons, total, pg, lm),
  });
});

/**
 * GET /api/comparisons/:id
 */
const getById = asyncHandler(async (req, res) => {
  const comparison = await Comparison.findById(req.params.id)
    .populate('erpCfdiId')
    .populate('satCfdiId')
    .populate('triggeredBy', 'name email');
  if (!comparison) return res.status(404).json({ error: 'Comparación no encontrada' });
  res.json(comparison);
});

/**
 * POST /api/comparisons/batch
 */
const batch = asyncHandler(async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) return res.status(400).json({ errors: errors.array() });

  const { filters = {}, concurrency = 5, uuids, ejercicio, periodo } = req.body;

  // Traslados (tipo T) no participan en la conciliación SAT vs ERP
  const cfdiFilter = { source: 'ERP', isActive: true, tipoDeComprobante: { $ne: 'T' }, ...filters };

  if (ejercicio) {
    const yr = parseInt(ejercicio);
    const mo = periodo ? parseInt(periodo) : null;
    // Primero intentar por campos explícitos (CFDIs subidos con periodo seleccionado)
    // Si no hay resultados con eso, el fallback al rango de fechas queda como respaldo
    cfdiFilter.ejercicio = yr;
    if (mo && mo >= 1 && mo <= 12) {
      cfdiFilter.periodo = mo;
    }
  }

  if (Array.isArray(uuids) && uuids.length > 0) {
    cfdiFilter.uuid = { $in: uuids.map(u => u.toUpperCase()) };
  }

  // Buscar también CFDIs SAT/MANUAL del mismo periodo para detectar los que no están en ERP
  const satFilter = { source: { $in: ['SAT', 'MANUAL'] }, isActive: true };
  if (cfdiFilter.ejercicio)          satFilter.ejercicio          = cfdiFilter.ejercicio;
  if (cfdiFilter.periodo)            satFilter.periodo            = cfdiFilter.periodo;
  if (cfdiFilter.tipoDeComprobante)  satFilter.tipoDeComprobante  = cfdiFilter.tipoDeComprobante;

  // Para detectar SAT-only necesitamos TODOS los UUIDs ERP del periodo,
  // no solo los no conciliados, para no marcar como "faltante en ERP"
  // a facturas que ya fueron conciliadas.
  const allErpFilter = {
    source: 'ERP', isActive: true, tipoDeComprobante: { $ne: 'T' },
    ...(cfdiFilter.ejercicio ? { ejercicio: cfdiFilter.ejercicio } : {}),
    ...(cfdiFilter.periodo   ? { periodo:   cfdiFilter.periodo   } : {}),
    ...(cfdiFilter.tipoDeComprobante ? { tipoDeComprobante: cfdiFilter.tipoDeComprobante } : {}),
  };

  const [erpCfdis, allErpUuidDocs, satCfdis] = await Promise.all([
    CFDI.find(cfdiFilter, '_id uuid').lean(),
    Array.isArray(uuids) && uuids.length > 0
      ? Promise.resolve([])
      : CFDI.find(allErpFilter, 'uuid').lean(),
    Array.isArray(uuids) && uuids.length > 0
      ? Promise.resolve([])
      : CFDI.find(satFilter, '_id uuid').lean(),
  ]);

  if (erpCfdis.length === 0 && satCfdis.length === 0) {
    return res.status(200).json({ message: 'No hay CFDIs para comparar', processed: 0 });
  }

  // CFDIs SAT que no tienen contraparte en ERP (por UUID) — usar set completo de ERP
  const erpUuids = new Set(allErpUuidDocs.map(c => c.uuid.toUpperCase()));
  const satOnlyCfdis = satCfdis.filter(c => !erpUuids.has(c.uuid.toUpperCase()));

  const totalCFDIs = erpCfdis.length + satOnlyCfdis.length;

  const session = await ComparisonSession.create({
    name: formatSessionName(new Date()),
    triggeredBy: req.user._id,
    totalCFDIs,
    status: 'running',
    filters,
  });

  const ids        = erpCfdis.map(c => c._id.toString());
  const satOnlyIds = satOnlyCfdis.map(c => c._id.toString());

  res.status(202).json({
    message: 'Comparación en lote iniciada',
    total: totalCFDIs,
    sessionId: session._id,
  });

  batchCompareCFDIs(ids, { concurrency, triggeredBy: req.user._id, sessionId: session._id, satOnlyIds })
    .catch(err => {
      ComparisonSession.findByIdAndUpdate(session._id, { status: 'failed', completedAt: new Date() }).exec();
      console.error('Error en batch:', err);
    });
});

/**
 * PATCH /api/comparisons/:id/resolve
 */
const resolve = asyncHandler(async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) return res.status(400).json({ errors: errors.array() });

  const comparison = await Comparison.findByIdAndUpdate(
    req.params.id,
    {
      resolved: true,
      resolvedAt: new Date(),
      resolvedBy: req.user._id,
      resolutionNotes: req.body.resolutionNotes,
    },
    { new: true },
  );
  if (!comparison) return res.status(404).json({ error: 'Comparación no encontrada' });
  res.json(comparison);
});

module.exports = {
  ejerciciosResumen, periodos, list, stats,
  listSessions, getSession, getById, batch, resolve,
};
