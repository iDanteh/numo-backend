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
const { logger } = require('../../../shared/utils/logger');
const clienteService = require('./loyalty-kore-cliente.service');

const router = express.Router();

/**
 * @swagger
 * /api/loyalty/kore/clientes:
 *   post:
 *     tags: [Lealtad Kore - Clientes]
 *     summary: Alta, baja o actualización de un cliente del catálogo de Kore
 *     security:
 *       - apiKeyAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             description: Payload de alta/baja/actualización de cliente Kore.
 *           examples:
 *             alta:
 *               summary: Alta de cliente
 *               value:
 *                 evento: "alta"
 *                 koreClienteId: "KORE-CLI-00981"
 *                 telefono: "9511771923"
 *                 nombre: "María López Hernández"
 *                 nivel: "Bronce"
 *             actualizacion:
 *               summary: Actualización de cliente
 *               value:
 *                 evento: "actualizacion"
 *                 koreClienteId: "KORE-CLI-00981"
 *                 telefono: "9511779900"
 *                 nombre: "María López Hernández"
 *                 activo: true
 *                 nivel: "Bronce"
 *             baja:
 *               summary: Baja de cliente
 *               value:
 *                 evento: "baja"
 *                 koreClienteId: "KORE-CLI-00981"
 *     responses:
 *       200:
 *         description: Cliente procesado.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *             example:
 *               ok: true
 *               evento: "alta"
 *               cardId: 1842
 *               creada: true
 *       401:
 *         description: X-Api-Key inválida o faltante.
 */
router.post('/clientes', verifyKoreApiKey, asyncHandler(async (req, res) => {
  // Diagnóstico temporal (2026-11-05): no había forma de correlacionar un 409/404 de este
  // webhook con el payload real que lo disparó — los logs de error solo traían el stack y la
  // URL. Loguear el body entero en cada request permite reconstruir la secuencia exacta de
  // eventos (alta/actualizacion/baja) que manda Kore al reproducir un caso en producción.
  logger.info('[KoreClienteWebhook] payload recibido', req.body);
  res.json(await clienteService.procesarClienteKore(req.body));
}));

module.exports = router;
