const express = require('express');
const rateLimit = require('express-rate-limit');
const { authenticate, authorize } = require('../middleware/auth');
const { list, summary, getById, updateStatus } = require('../controllers/discrepancy.controller');

const router = express.Router();

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

router.get('/summary', authenticate, listLimiter, summary);
router.get('/', authenticate, listLimiter, list);
router.get('/:id', authenticate, getById);
router.patch('/:id/status', authenticate, authorize('admin', 'contador', 'auditor'), updateStatus);

module.exports = router;
