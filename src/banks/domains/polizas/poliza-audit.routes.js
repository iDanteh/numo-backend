'use strict';

/**
 * Monitor de pólizas (2026-10-15) — solo admin vía `polizas:audit:read`
 * (sin asignar a ningún rol; admin lo tiene por el wildcard '*').
 * Filtros comunes (query): desde, hasta (YYYY-MM-DD, hora de México), rfc,
 * usuario, accion, tipo (I/E/D/T/B/G o COB = Cobranza), q.
 */

const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth.real');
const { asyncHandler }         = require('../../shared/middleware/error-handler');
const audit                    = require('./poliza-audit.service');

const router = express.Router();
const PERM   = 'polizas:audit:read';

router.get('/usuarios', authenticate, permit(PERM),
  asyncHandler(async (_req, res) => res.json({ usuarios: await audit.usuarios(), acciones: audit.ACCIONES })));

router.get('/resumen', authenticate, permit(PERM),
  asyncHandler(async (req, res) => res.json(await audit.resumenPorUsuario(req.query))));

router.get('/actividad-diaria', authenticate, permit(PERM),
  asyncHandler(async (req, res) => res.json(await audit.actividadDiaria(req.query))));

router.get('/bitacora', authenticate, permit(PERM),
  asyncHandler(async (req, res) => res.json(await audit.bitacora(req.query))));

router.get('/cancelaciones', authenticate, permit(PERM),
  asyncHandler(async (req, res) => res.json(await audit.cancelaciones(req.query))));

// Reporte .xlsx con TODO lo del monitor (resumen/uso, actividad diaria,
// bitácora, cancelaciones, exportaciones), respetando los filtros activos.
router.get('/reporte.xlsx', authenticate, permit(PERM),
  asyncHandler(async (req, res) => {
    const buffer = await audit.construirReporteXlsx(req.query);
    const hoy = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Mexico_City' }); // YYYY-MM-DD
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="Monitor_Polizas_${hoy}.xlsx"`);
    res.send(Buffer.from(buffer));
  }));

module.exports = router;
