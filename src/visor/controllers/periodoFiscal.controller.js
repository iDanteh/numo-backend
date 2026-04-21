'use strict';

/**
 * visor/controllers/periodoFiscal.controller.js
 * ─────────────────────────────────────────────────────────────────────────────
 * CRUD de períodos fiscales.
 *
 * Arquitectura mixta:
 *   • PeriodoFiscal → PostgreSQL (via periodo-fiscal.repository)
 *   • Estadísticas (comparaciones, discrepancias, CFDIs) → MongoDB aggregation
 *
 * El join entre ambas fuentes se realiza en la capa de aplicación
 * usando el par (ejercicio, periodo) como clave común.
 */

const { validationResult } = require('express-validator');
const periodoRepo          = require('../repositories/periodo-fiscal.repository');
const Comparison           = require('../models/Comparison');
const Discrepancy          = require('../models/Discrepancy');
const CFDI                 = require('../models/CFDI');
const { asyncHandler }     = require('../../shared/middleware/error-handler');

/**
 * GET /api/periodos-fiscales
 * Lista todos los períodos enriquecidos con estadísticas de MongoDB.
 */
const list = asyncHandler(async (_req, res) => {
  const periodos = await periodoRepo.findAll();
  if (!periodos.length) return res.json({ data: [] });

  // Estadísticas de comparaciones, discrepancias y CFDIs desde MongoDB
  const [compStats, discStats, cfdiStats] = await Promise.all([
    Comparison.aggregate([
      {
        $group: {
          _id: { ejercicio: '$ejercicio', periodo: '$periodo' },
          total:       { $sum: 1 },
          match:       { $sum: { $cond: [{ $eq: ['$status', 'match'] },       1, 0] } },
          discrepancy: { $sum: { $cond: [{ $eq: ['$status', 'discrepancy'] }, 1, 0] } },
          not_in_sat:  { $sum: { $cond: [{ $eq: ['$status', 'not_in_sat'] },  1, 0] } },
          cancelled:   { $sum: { $cond: [{ $eq: ['$status', 'cancelled'] },   1, 0] } },
          error:       { $sum: { $cond: [{ $eq: ['$status', 'error'] },       1, 0] } },
        },
      },
    ]),
    Discrepancy.aggregate([
      { $match: { status: { $in: ['open', 'in_review', 'escalated'] } } },
      {
        $group: {
          _id: { ejercicio: '$ejercicio', periodo: '$periodo' },
          openDiscrepancies: { $sum: 1 },
        },
      },
    ]),
    CFDI.aggregate([
      { $match: { isActive: true, ejercicio: { $exists: true }, periodo: { $exists: true } } },
      {
        $group: {
          _id:   { ejercicio: '$ejercicio', periodo: '$periodo', source: '$source' },
          count: { $sum: 1 },
        },
      },
    ]),
  ]);

  // Indexar para acceso O(1)
  const compMap = Object.fromEntries(
    compStats.map((s) => [`${s._id.ejercicio}-${s._id.periodo ?? 'null'}`, s]),
  );
  const discMap = Object.fromEntries(
    discStats.map((s) => [`${s._id.ejercicio}-${s._id.periodo ?? 'null'}`, s]),
  );

  const cfdiMap = {};
  for (const s of cfdiStats) {
    const key = `${s._id.ejercicio}-${s._id.periodo}`;
    if (!cfdiMap[key]) cfdiMap[key] = { erp: 0, sat: 0, total: 0 };
    cfdiMap[key].total += s.count;
    if (s._id.source === 'ERP')               cfdiMap[key].erp += s.count;
    if (['SAT', 'MANUAL'].includes(s._id.source)) cfdiMap[key].sat += s.count;
  }

  // Merge: PG + MongoDB en la capa de aplicación
  const data = periodos.map((p) => {
    const pPlain = p.toJSON();
    const key    = `${p.ejercicio}-${p.periodo ?? 'null'}`;
    const cfdiKey = `${p.ejercicio}-${p.periodo}`;
    const cs = compMap[key] ?? {};
    const ds = discMap[key] ?? {};
    const cf = p.periodo != null ? (cfdiMap[cfdiKey] ?? {}) : {};

    return {
      ...pPlain,
      cfdis: {
        erp:   cf.erp   ?? 0,
        sat:   cf.sat   ?? 0,
        total: cf.total ?? 0,
      },
      stats: {
        total:             cs.total       ?? 0,
        match:             cs.match       ?? 0,
        discrepancy:       cs.discrepancy ?? 0,
        not_in_sat:        cs.not_in_sat  ?? 0,
        cancelled:         cs.cancelled   ?? 0,
        error:             cs.error       ?? 0,
        openDiscrepancies: ds.openDiscrepancies ?? 0,
      },
    };
  });

  res.json({ data });
});

/**
 * POST /api/periodos-fiscales
 */
const create = asyncHandler(async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) return res.status(400).json({ errors: errors.array() });

  const { ejercicio, periodo = null, label } = req.body;

  const exists = await periodoRepo.findByEjercicioPeriodo(ejercicio, periodo ?? null);
  if (exists) return res.status(409).json({ error: 'Este periodo ya existe' });

  const doc = await periodoRepo.create({
    ejercicio,
    periodo: periodo ?? null,
    label,
    createdBy: req.user?._id ?? null,   // PG integer id del usuario autenticado
  });

  res.status(201).json(doc);
});

/**
 * DELETE /api/periodos-fiscales/:id
 */
const remove = asyncHandler(async (req, res) => {
  const doc = await periodoRepo.remove(req.params.id);
  if (!doc) return res.status(404).json({ error: 'Periodo no encontrado' });
  res.json({ message: 'Eliminado' });
});

module.exports = { list, create, remove };
