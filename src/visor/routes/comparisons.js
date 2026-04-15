const express = require('express');
const { body } = require('express-validator');
const rateLimit = require('express-rate-limit');
const { authenticate, authorize } = require('../middleware/auth');

const listLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 150,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    res.status(429).json({
      success: false,
      error: 'Demasiadas peticiones, espera un momento.',
      code: 'RATE_LIMIT_EXCEEDED',
      retryAfter: 60,
    });
  },
});
const {
  ejerciciosResumen, periodos, list, stats,
  listSessions, getSession, getById, batch, resolve,
} = require('../controllers/comparison.controller');

const router = express.Router();

router.get('/ejercicios/resumen', authenticate, ejerciciosResumen);
router.get('/periodos', authenticate, periodos);
router.get('/stats', authenticate, stats);
router.get('/sessions', authenticate, listSessions);
router.get('/sessions/:id', authenticate, getSession);
router.get('/', authenticate, listLimiter, list);
router.get('/:id', authenticate, getById);

router.post('/batch',
  authenticate,
  authorize('admin', 'contador'),
  [body('filters').optional().isObject()],
  batch,
);

router.patch('/:id/resolve',
  authenticate,
  authorize('admin', 'contador', 'auditor'),
  [body('resolutionNotes').optional().isString()],
  resolve,
);

module.exports = router;
