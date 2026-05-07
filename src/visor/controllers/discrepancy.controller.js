const Discrepancy = require('../models/Discrepancy');
const Comparison  = require('../models/Comparison');
const CFDI        = require('../models/CFDI');
const { asyncHandler } = require('../../shared/middleware/error-handler');
const { paginate, skip } = require('../utils/pagination');

const ESTADOS_RESUELTOS = ['resolved', 'ignored', 'escalated'];

/**
 * GET /api/discrepancies
 */
const list = asyncHandler(async (req, res) => {
  const { page = 1, limit = 20, type, severity, status, rfcEmisor, ejercicio, periodo, tipoDeComprobante, uuid } = req.query;
  const pg = parseInt(page);
  const lm = parseInt(limit);

  const filter = {};
  if (type)      filter.type      = type;
  if (severity)  filter.severity  = severity;
  if (status)    filter.status    = status;
  if (rfcEmisor) filter.rfcEmisor = rfcEmisor.toUpperCase();
  if (ejercicio)          filter.ejercicio          = parseInt(ejercicio);
  if (periodo)            filter.periodo            = parseInt(periodo);
  if (tipoDeComprobante)  filter.tipoDeComprobante  = tipoDeComprobante;
  if (uuid)               filter.uuid               = uuid.toUpperCase();

  const [discrepancies, total] = await Promise.all([
    Discrepancy.find(filter)
      .populate({
        path: 'comparisonId',
        select: 'uuid status comparedAt differences criticalCount warningCount totalDifferences erpCfdiId satCfdiId',
        populate: [
          {
            path: 'erpCfdiId',
            model: 'CFDI',
            select: 'uuid serie folio fecha total subTotal moneda tipoCambio tipoDeComprobante emisor receptor satStatus',
          },
          {
            path: 'satCfdiId',
            model: 'CFDI',
            select: 'uuid serie folio fecha total subTotal moneda tipoCambio tipoDeComprobante emisor receptor satStatus',
          },
        ],
      })
      .sort({ createdAt: -1 })
      .skip(skip(pg, lm))
      .limit(lm)
      .lean(),
    Discrepancy.countDocuments(filter),
  ]);

  res.json(paginate(discrepancies, total, pg, lm));
});

/**
 * GET /api/discrepancies/summary
 */
const summary = asyncHandler(async (req, res) => {
  const { ejercicio, periodo } = req.query;
  const match = {};
  if (ejercicio) match.ejercicio = parseInt(ejercicio);
  if (periodo)   match.periodo   = parseInt(periodo);
  const $match = Object.keys(match).length ? [{ $match: match }] : [];

  const [byType, bySeverity, byStatus] = await Promise.all([
    Discrepancy.aggregate([...$match, { $group: { _id: '$type',     count: { $sum: 1 } } }, { $sort: { count: -1 } }]),
    Discrepancy.aggregate([...$match, { $group: { _id: '$severity', count: { $sum: 1 } } }]),
    Discrepancy.aggregate([...$match, { $group: { _id: '$status',   count: { $sum: 1 } } }]),
  ]);

  res.json({ byType, bySeverity, byStatus });
});

/**
 * GET /api/discrepancies/:id
 */
const getById = asyncHandler(async (req, res) => {
  const d = await Discrepancy.findById(req.params.id).populate('comparisonId');
  if (!d) return res.status(404).json({ error: 'Discrepancia no encontrada' });
  res.json(d);
});

/**
 * PATCH /api/discrepancies/:id/status
 */
const updateStatus = asyncHandler(async (req, res) => {
  const { status, resolutionType, note } = req.body;
  const update = { status };
  if (resolutionType) update.resolutionType = resolutionType;
  if (status === 'resolved') {
    update.resolvedAt = new Date();
    update.resolvedBy = req.user._id;
  }
  if (note) update.$push = { notes: note };

  const d = await Discrepancy.findByIdAndUpdate(req.params.id, update, { new: true });
  if (!d) return res.status(404).json({ error: 'Discrepancia no encontrada' });

  // Si el nuevo estado es "resuelto/ignorado/escalado", verificar si quedan
  // discrepancias abiertas en la misma comparación.
  // Si todas están resueltas → marcar la comparación y el CFDI como conciliados.
  if (ESTADOS_RESUELTOS.includes(status) && d.comparisonId) {
    const pendientes = await Discrepancy.countDocuments({
      comparisonId: d.comparisonId,
      status: { $nin: ESTADOS_RESUELTOS },
    });

    if (pendientes === 0) {
      await Comparison.findByIdAndUpdate(d.comparisonId, {
        status: 'match',
        resolved: true,
        resolvedAt: new Date(),
      });
      // Actualizar lastComparisonStatus en el CFDI ERP correspondiente
      const comp = await Comparison.findById(d.comparisonId).select('erpCfdiId').lean();
      if (comp?.erpCfdiId) {
        await CFDI.findByIdAndUpdate(comp.erpCfdiId, {
          lastComparisonStatus: 'match',
          lastComparisonAt: new Date(),
        });
      }
    }
  }

  res.json(d);
});

/**
 * POST /api/discrepancies/comentario-por-uuid
 * Añade comentario buscando por uuid (y opcionalmente tipo).
 * Útil cuando solo se tiene el UUID, no el _id de la discrepancia.
 */
const addComentarioPorUUID = asyncHandler(async (req, res) => {
  const { uuid, tipo, motivo, descripcion = '' } = req.body;
  if (!uuid)         return res.status(400).json({ error: 'uuid es requerido.' });
  if (!motivo?.trim()) return res.status(400).json({ error: 'motivo es requerido.' });

  const filter = { uuid: uuid.toUpperCase() };
  if (tipo) filter.type = tipo;

  const d = await Discrepancy.findOneAndUpdate(
    filter,
    { $push: { comentarios: { motivo: motivo.trim(), descripcion: descripcion.trim(), creadoPor: req.user?.nombre || req.user?.email || '' } } },
    { new: true, sort: { createdAt: -1 } },
  );
  if (!d) return res.status(404).json({ error: 'No existe un registro de discrepancia para este UUID. Verifica que se haya ejecutado una comparación para este CFDI.' });
  res.json({ success: true, discrepancyId: d._id, comentarios: d.comentarios });
});

/**
 * POST /api/discrepancies/:id/comentarios
 */
const addComentario = asyncHandler(async (req, res) => {
  const { motivo, descripcion = '' } = req.body;
  if (!motivo?.trim()) return res.status(400).json({ error: 'El campo motivo es requerido.' });

  const d = await Discrepancy.findByIdAndUpdate(
    req.params.id,
    { $push: { comentarios: { motivo: motivo.trim(), descripcion: descripcion.trim(), creadoPor: req.user?.nombre || req.user?.email || '' } } },
    { new: true },
  );
  if (!d) return res.status(404).json({ error: 'Discrepancia no encontrada.' });
  res.json({ success: true, comentarios: d.comentarios });
});

module.exports = { list, summary, getById, updateStatus, addComentario, addComentarioPorUUID };
