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

/**
 * @swagger
 * /api/loyalty/kore/niveles:
 *   post:
 *     tags: [Lealtad Kore - Niveles]
 *     summary: Alta, ajuste o baja de un nivel del catálogo de Kore
 *     security:
 *       - apiKeyAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             description: Payload de alta/ajuste/baja de nivel.
 *           examples:
 *             alta:
 *               summary: Alta de nivel (incluye color opcional, best-effort)
 *               value:
 *                 evento: "alta"
 *                 koreId: "n4"
 *                 nombre: "Platino"
 *                 porcentaje: 7.5
 *                 estatus: true
 *                 color: "#C0C0C0"
 *             actualizacion:
 *               summary: Actualización de nivel (incluye color opcional, best-effort)
 *               value:
 *                 evento: "actualizacion"
 *                 koreId: "n1"
 *                 porcentaje: 2
 *                 estatus: true
 *                 color: "#CD7F32"
 *             baja:
 *               summary: Baja de nivel
 *               value:
 *                 evento: "baja"
 *                 koreId: "n4"
 *     responses:
 *       200:
 *         description: Nivel procesado.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *             example:
 *               ok: true
 *               evento: "alta"
 *               nivelId: 58
 *               creado: true
 *       401:
 *         description: X-Api-Key inválida o faltante.
 */
router.post('/niveles', verifyKoreApiKey, asyncHandler(async (req, res) => {
  res.json(await nivelService.procesarNivelKore(req.body));
}));

module.exports = router;
