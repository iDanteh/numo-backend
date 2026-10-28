'use strict';

/**
 * domains/kore-match/loyalty-kore-cliente.routes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — router del webhook Kore→Numo de catálogo de clientes,
 * montado en `/api/loyalty/kore` (app.js), mismo prefijo que
 * `loyalty-kore-venta.routes.js` pero en su propio router (separación por
 * dominio: venta vs. catálogo de clientes).
 *
 * `POST /clientes` lo llama Kore (`verifyKoreApiKey`, mismo secreto que
 * Solicitudes de Cobro/el webhook de venta) para avisar altas, bajas y
 * actualizaciones de clientes — ver `loyalty-kore-cliente.service.js`.
 */
const express = require('express');
const { verifyKoreApiKey } = require('../../../shared/middleware/kore-api-key-auth');
const { asyncHandler } = require('../../../shared/middleware/error-handler');
const clienteService = require('./loyalty-kore-cliente.service');

const router = express.Router();

router.post('/clientes', verifyKoreApiKey, asyncHandler(async (req, res) => {
  res.json(await clienteService.procesarClienteKore(req.body));
}));

module.exports = router;
