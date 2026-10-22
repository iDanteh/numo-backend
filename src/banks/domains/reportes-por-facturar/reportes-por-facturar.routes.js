'use strict';

// Reporte "Por facturar" (Reportes, 2026-10-22) — ver reportes-por-facturar.service.js.
const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth.real');
const { asyncHandler }         = require('../../shared/middleware/error-handler');
const service                  = require('./reportes-por-facturar.service');

const router = express.Router();

// GET /api/reportes-por-facturar?rfc= — historial (sin los datos)
router.get('/', authenticate, permit('visor:reports'), asyncHandler(async (req, res) => {
  res.json({ data: await service.listar({ rfc: req.query.rfc }) });
}));

// POST /api/reportes-por-facturar { rfc, centroCostoId?, fechaInicio, fechaFin? }
router.post('/', authenticate, permit('visor:reports'), asyncHandler(async (req, res) => {
  const { rfc, centroCostoId, fechaInicio, fechaFin } = req.body ?? {};
  res.status(201).json(await service.generar({ rfc, centroCostoId, fechaInicio, fechaFin }, req.user));
}));

// GET /api/reportes-por-facturar/:id — reporte con sus tablas
router.get('/:id', authenticate, permit('visor:reports'), asyncHandler(async (req, res) => {
  res.json(await service.obtener(req.params.id));
}));

// GET /api/reportes-por-facturar/:id/excel
router.get('/:id/excel', authenticate, permit('visor:reports'), asyncHandler(async (req, res) => {
  const { buffer, filename } = await service.excel(req.params.id);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.send(Buffer.from(buffer));
}));

module.exports = router;
