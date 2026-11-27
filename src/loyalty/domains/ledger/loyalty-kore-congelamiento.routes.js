'use strict';

/**
 * domains/ledger/loyalty-kore-congelamiento.routes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — router del webhook Kore→Numo de congelamiento por
 * inactividad, montado en `/api/loyalty/kore` (app.js), mismo prefijo que
 * `loyalty-kore-venta.routes.js`/`loyalty-kore-cliente.routes.js` pero en su
 * propio router (separación por dominio).
 *
 * `POST /congelamiento` lo llama Kore (`verifyKoreApiKey`, mismo secreto que
 * el resto de webhooks de Lealtad) para avisar cancelación de puntos por
 * inactividad o desactivación de tarjeta — ver
 * `loyalty-kore-congelamiento.service.js` para el detalle completo del
 * contrato y por qué reemplaza al cron diario viejo.
 */
const express = require('express');
const { verifyKoreApiKey } = require('../../../shared/middleware/kore-api-key-auth');
const { asyncHandler } = require('../../../shared/middleware/error-handler');
const { logger } = require('../../../shared/utils/logger');
const congelamientoService = require('./loyalty-kore-congelamiento.service');

const router = express.Router();

/**
 * @swagger
 * /api/loyalty/kore/congelamiento:
 *   post:
 *     tags: [Lealtad Kore - Congelamiento]
 *     summary: Cancelación de puntos por inactividad o desactivación de tarjeta (reemplaza el cron interno de congelamiento)
 *     security:
 *       - apiKeyAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [evento, koreClienteId, koreEventoId, telefono]
 *             properties:
 *               evento:
 *                 type: string
 *                 enum: [cancelacion_puntos, desactivacion_tarjeta]
 *                 example: "cancelacion_puntos"
 *               koreClienteId:
 *                 type: string
 *                 description: ID del cliente en Kore — identidad estable, mismo criterio que /clientes.
 *                 example: "KORE-CLI-00981"
 *               koreEventoId:
 *                 type: string
 *                 description: ID único de ESTE aviso puntual en Kore — clave de idempotencia (sobre todo en cancelacion_puntos, que mueve puntos reales).
 *                 example: "KORE-EVT-00123"
 *               telefono:
 *                 type: string
 *                 description: SIEMPRE requerido (10 dígitos) — a diferencia de /clientes, acá NO es un fallback de búsqueda, es una verificación obligatoria de que koreClienteId y telefono apuntan a la misma tarjeta. Si no coincide con el teléfono registrado, se rechaza el evento (409).
 *                 example: "9511771923"
 *               motivo:
 *                 type: string
 *                 description: Opcional, solo para auditoría/justificación — no es un campo de LoyaltyCard.
 *                 example: "Sin ventas en 6 meses"
 *           examples:
 *             cancelacion_puntos:
 *               summary: Cancelación de puntos por inactividad
 *               value:
 *                 evento: "cancelacion_puntos"
 *                 koreClienteId: "KORE-CLI-00981"
 *                 koreEventoId: "KORE-EVT-00123"
 *                 telefono: "9511771923"
 *                 motivo: "Sin ventas en 6 meses"
 *             desactivacion_tarjeta:
 *               summary: Desactivación de tarjeta por inactividad
 *               value:
 *                 evento: "desactivacion_tarjeta"
 *                 koreClienteId: "KORE-CLI-00981"
 *                 koreEventoId: "KORE-EVT-00456"
 *                 telefono: "9511771923"
 *     responses:
 *       200:
 *         description: Evento procesado.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *             example:
 *               ok: true
 *               evento: "cancelacion_puntos"
 *               cardId: "b1f6e2b0-7e8a-4e8a-9b7a-9c1a1b2c3d4e"
 *               puntosCancelados: 350
 *               saldoResultante: 0
 *       400:
 *         description: Payload inválido (evento desconocido, o falta koreClienteId/koreEventoId/telefono).
 *       401:
 *         description: X-Api-Key inválida o faltante.
 *       404:
 *         description: No existe ninguna tarjeta con ese koreClienteId.
 *       409:
 *         description: El teléfono recibido no coincide con el registrado para ese koreClienteId.
 *       422:
 *         description: La tarjeta ya está desactivada (solo aplica a cancelacion_puntos) — Kore debe mandar cancelacion_puntos antes o independientemente de desactivacion_tarjeta, nunca después.
 */
router.post('/congelamiento', verifyKoreApiKey, asyncHandler(async (req, res) => {
  // Mismo criterio de diagnóstico que loyalty-kore-cliente.routes.js: loguear el body completo
  // permite reconstruir la secuencia exacta de eventos que manda Kore al investigar un caso real.
  logger.info('[KoreCongelamientoWebhook] payload recibido', req.body);
  res.json(await congelamientoService.procesarCongelamientoKore(req.body));
}));

module.exports = router;
