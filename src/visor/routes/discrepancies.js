'use strict';

const express    = require('express');
const rateLimit  = require('express-rate-limit');
const { authenticate, permit } = require('../../shared/middleware/auth');
const { limitarAEmpresasDelUsuario: empresas, soloSinEmpresasAsignadas } = require('../../shared/middleware/empresas-usuario');
const { list, summary, getById, updateStatus, addComentario, addComentarioPorUUID } = require('../controllers/discrepancy.controller');

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

router.get('/summary', authenticate, permit('visor:read'), soloSinEmpresasAsignadas, listLimiter, summary);
router.get('/',        authenticate, permit('visor:read'), empresas(), listLimiter, list);
router.get('/:id',     authenticate, permit('visor:read'), soloSinEmpresasAsignadas, getById);
router.patch('/:id/status',            authenticate, permit('visor:write'), updateStatus);
router.post('/comentario-por-uuid',    authenticate, addComentarioPorUUID);
router.post('/:id/comentarios',        authenticate, addComentario);

module.exports = router;
