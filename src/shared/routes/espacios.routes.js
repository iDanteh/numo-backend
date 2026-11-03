'use strict';

/**
 * shared/routes/espacios.routes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * "Espacios de NUMO" que viven en otra aplicación. Hoy: el panel del Kiosco de
 * facturación (ver shared/services/kiosco-pase.service.js).
 */

const express        = require('express');
const { authenticate, permit } = require('../middleware/auth');
const { logger }     = require('../utils/logger');
const { emitirPase } = require('../services/kiosco-pase.service');

const router = express.Router();

// POST /api/espacios/kiosco/pase → { url }  (el navegador se va a esa url)
router.post('/kiosco/pase', authenticate, permit('kiosco:admin'), (req, res) => {
  try {
    const r = emitirPase(req.user, req.auth?.payload);
    if (!r) return res.status(503).json({ error: 'El acceso al Kiosco de facturación no está configurado' });
    logger.info(`[espacios] pase al kiosco emitido para ${req.user.email}`);
    // El pase va en la url: que ningún caché la guarde.
    return res.set('Cache-Control', 'no-store').json(r);
  } catch (err) {
    logger.error(`[espacios] no se pudo emitir el pase al kiosco: ${err.message}`);
    return res.status(err.status || 500).json({ error: err.status ? err.message : 'No se pudo abrir el Kiosco de facturación' });
  }
});

module.exports = router;
