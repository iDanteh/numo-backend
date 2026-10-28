'use strict';

// Reporte "Por facturar" (Reportes, 2026-10-22) — ver reportes-por-facturar.service.js.
const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth.real');
const { asyncHandler }         = require('../../shared/middleware/error-handler');
const service                  = require('./reportes-por-facturar.service');

const router = express.Router();

// Permiso propio del reporte (ver rbac.js REPORTES_POR_FACTURAR) — `visor:reports`
// ya no basta (2026-10-23, pedido del usuario).
const PERM = 'reportes:por-facturar';

// GET /api/reportes-por-facturar?rfc=&page=&limit= — historial paginado (sin los datos)
router.get('/', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  const { rfc, page, limit } = req.query;
  res.json(await service.listar({ rfc, page, limit }));
}));

// POST /api/reportes-por-facturar { rfc, centroCostoId?, fechaInicio, fechaFin? }
router.post('/', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  const { rfc, centroCostoId, fechaInicio, fechaFin } = req.body ?? {};
  res.status(201).json(await service.generar({ rfc, centroCostoId, fechaInicio, fechaFin }, req.user));
}));

// GET /api/reportes-por-facturar/:id — reporte con sus tablas
router.get('/:id', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  res.json(await service.obtener(req.params.id));
}));

// POST /api/reportes-por-facturar/:id/actualizar — recalcula el mismo reporte (misma sucursal y fechas)
router.post('/:id/actualizar', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  res.json(await service.actualizar(req.params.id, req.user));
}));

// DELETE /api/reportes-por-facturar/:id — borrado lógico (guarda quién lo eliminó)
router.delete('/:id', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  res.json(await service.eliminar(req.params.id, req.user));
}));

// GET /api/reportes-por-facturar/:id/excel
router.get('/:id/excel', authenticate, permit(PERM), asyncHandler(async (req, res) => {
  const { buffer, filename } = await service.excel(req.params.id);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.send(Buffer.from(buffer));
}));

module.exports = router;
