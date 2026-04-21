'use strict';

const express    = require('express');
const { body }   = require('express-validator');
const rateLimit  = require('express-rate-limit');
const { authenticate, permit } = require('../../shared/middleware/auth');
const {
  ejerciciosResumen, periodos, list, stats,
  listSessions, getSession, getById, batch, resolve,
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

module.exports = router;
