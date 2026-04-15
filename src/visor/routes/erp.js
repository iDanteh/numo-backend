'use strict';

const express = require('express');
const { body, query } = require('express-validator');
const { authenticate, authorize } = require('../middleware/auth');
const { cargar, previsualizar, enriquecerPagos } = require('../controllers/erp.controller');

const router = express.Router();

// ── Validadores reutilizables ────────────────────────────────────────────────

const validarEjercicioPeriodoQuery = [
  query('ejercicio')
    .isInt({ min: 2000, max: 2100 })
    .withMessage('ejercicio debe ser un año entre 2000 y 2100'),
  query('periodo')
    .isInt({ min: 1, max: 12 })
    .withMessage('periodo debe ser un mes válido (1–12)'),
];

// ── Rutas ────────────────────────────────────────────────────────────────────

/**
 * GET /api/erp/facturas?ejercicio=2026&periodo=2[&tipo=I]
 *
 * Descarga las facturas del ERP para el periodo indicado, las normaliza
 * y devuelve la lista al frontend SIN persistir en MongoDB.
 *
 * Query params:
 *   ejercicio  {number}  Requerido
 *   periodo    {number}  Requerido
 *   tipo       {string}  Opcional — I | E | P | T | N
 *
 * Respuesta:
 *   { total, totalERP, tipoFiltrado, tipoDescripcion, ejercicio, periodo, facturas[] }
 */
router.get(
  '/facturas',
  authenticate,
  authorize('admin', 'contador'),
  validarEjercicioPeriodoQuery,
  previsualizar,
);

/**
 * POST /api/erp/cargar
 *
 * Descarga las facturas del ERP, las transforma al modelo interno de CFDI
 * y las persiste en MongoDB (upsert por UUID).
 *
 * Body: { "ejercicio": 2026, "periodo": 3 }
 *
 * Respuesta:
 *   { totalRecibidos, nuevosInsertados, duplicados, errores, detalleErrores, message }
 */
router.post(
  '/cargar',
  authenticate,
  authorize('admin', 'contador'),
  [
    body('ejercicio')
      .isInt({ min: 2000, max: 2100 })
      .withMessage('ejercicio debe ser un año entre 2000 y 2100'),
    body('periodo')
      .isInt({ min: 1, max: 12 })
      .withMessage('periodo debe ser un mes válido (1–12)'),
  ],
  cargar,
);

/**
 * POST /api/erp/enriquecer-pagos
 *
 * Reprocesa CFDIs tipo P ya existentes en BD sin complementoPago.
 * Body (opcional): { ejercicio, periodo }
 */
router.post(
  '/enriquecer-pagos',
  authenticate,
  authorize('admin', 'contador'),
  enriquecerPagos,
);

module.exports = router;
