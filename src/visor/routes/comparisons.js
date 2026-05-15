'use strict';

const express    = require('express');
const { body }   = require('express-validator');
const rateLimit  = require('express-rate-limit');
const { authenticate, permit } = require('../../shared/middleware/auth');
const {
  ejerciciosResumen, periodos, list, stats,
  listSessions, getSession, getById, batch, resolve, conciliarNotInErp,
} = require('../controllers/comparison.controller');

const router = express.Router();

const listLimiter = rateLimit({
  windowMs: 60 * 1000,
  max:      150,
  standardHeaders: true,
  legacyHeaders:   false,
  handler: (_req, res) => res.status(429).json({
    success: false,
    error:   'Demasiadas peticiones, espera un momento.',
    code:    'RATE_LIMIT_EXCEEDED',
    retryAfter: 60,
  }),
});

router.get('/ejercicios/resumen', authenticate, ejerciciosResumen);
router.get('/periodos',           authenticate, periodos);
router.get('/stats',              authenticate, stats);
router.get('/sessions',           authenticate, listSessions);
router.get('/sessions/:id',       authenticate, getSession);
router.get('/',                   authenticate, listLimiter, list);
router.get('/:id',                authenticate, getById);

router.post('/batch',
  authenticate,
  permit('visor:write'),
  [body('filters').optional().isObject()],
  batch,
);

router.patch('/:id/resolve',
  authenticate,
  permit('visor:write'),
  [body('resolutionNotes').optional().isString()],
  resolve,
);

router.post('/conciliar-not-in-erp',
  authenticate,
  permit('visor:write'),
  [
    body('cfdiId').isMongoId().withMessage('cfdiId inválido'),
    body('causa').isIn([
      'proveedor_sin_registro', 'cancelada_antes_de_registro', 'periodo_anterior',
      'factura_global_sat', 'error_descarga_sat', 'tercero_sin_impacto', 'otra',
    ]).withMessage('causa inválida'),
    body('notas').isString().trim().notEmpty().withMessage('notas requeridas').isLength({ max: 500 }),
  ],
  conciliarNotInErp,
);

module.exports = router;
