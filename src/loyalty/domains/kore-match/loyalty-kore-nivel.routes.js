'use strict';

/**
 * domains/kore-match/loyalty-kore-nivel.routes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — router del webhook Kore→Numo de catálogo de niveles,
 * montado en `/api/loyalty/kore` (app.js), mismo prefijo que
 * `loyalty-kore-venta.routes.js`/`loyalty-kore-cliente.routes.js` pero en su
 * propio router (separación por dominio).
 *
 * `POST /niveles` lo llama Kore (`verifyKoreApiKey`, mismo secreto que el
 * resto de webhooks Kore→Numo) para avisar altas, ajustes y bajas del
 * catálogo de niveles — ver `loyalty-kore-nivel.service.js`.
 */
const express = require('express');
const { verifyKoreApiKey } = require('../../../shared/middleware/kore-api-key-auth');
const { asyncHandler } = require('../../../shared/middleware/error-handler');
const nivelService = require('./loyalty-kore-nivel.service');

const router = express.Router();

router.post('/niveles', verifyKoreApiKey, asyncHandler(async (req, res) => {
  res.json(await nivelService.procesarNivelKore(req.body));
}));

module.exports = router;
