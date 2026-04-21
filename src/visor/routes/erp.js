'use strict';

const express = require('express');
const { body, query } = require('express-validator');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { cargar, previsualizar, enriquecerPagos } = require('../controllers/erp.controller');

const router = express.Router();

const validarEjercicioPeriodoQuery = [
  query('ejercicio').isInt({ min: 2000, max: 2100 }).withMessage('ejercicio debe ser un año entre 2000 y 2100'),
  query('periodo').isInt({ min: 1, max: 12 }).withMessage('periodo debe ser un mes válido (1–12)'),
];

router.get('/facturas',
  authenticate,
  permit('erp:manage'),
  validarEjercicioPeriodoQuery,
  previsualizar,
);

router.post('/cargar',
  authenticate,
  permit('erp:manage'),
  [
    body('ejercicio').isInt({ min: 2000, max: 2100 }).withMessage('ejercicio debe ser un año entre 2000 y 2100'),
    body('periodo').isInt({ min: 1, max: 12 }).withMessage('periodo debe ser un mes válido (1–12)'),
  ],
  cargar,
);

router.post('/enriquecer-pagos',
  authenticate,
  permit('erp:manage'),
  enriquecerPagos,
);

module.exports = router;
