'use strict';

/**
 * domains/ledger/loyalty-kore-venta.routes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — router del webhook Kore→Numo de venta, montado en
 * `/api/loyalty/kore` (app.js). Separado de `loyalty.routes.js` a propósito:
 * ese router es staff-only de punta a punta por diseño (ver su propio
 * docstring) y nunca debe mezclar autenticación por API key server-to-server.
 *
 * `GET /saldo`, `POST /venta`, `POST /ventas` y `POST /venta-cancelada` los llama
 * Kore (`verifyKoreApiKey`, mismo secreto que Solicitudes de Cobro/`cxc-reversiones`
 * — ver contrato en `Docs/integracion-kore-puntos-lealtad-DRAFT.md`). `POST /ventas`
 * (plural, 2026-10-27, pedido explícito del usuario) procesa varias ventas en un
 * solo request — ver `loyalty-kore-venta.service.js#procesarLoteVentasKore`; no
 * reemplaza a `POST /venta`, que sigue existiendo para una sola venta.
 * `POST /venta-cancelada` (2026-11-04, pedido explícito del usuario) avisa que Kore
 * canceló una venta ya procesada — ver
 * `loyalty-kore-venta-cancelada.service.js#procesarVentaCanceladaKore`.
 * `GET /ventas-fallidas`, `PATCH /ventas-fallidas/:id/resolver` y
 * `GET /ventas-canceladas-con-deposito` son para el equipo de Numo (sesión
 * staff normal, mismo permiso que el resto de Configuración de Lealtad) — sin
 * pantalla de Angular todavía, eso es un paso aparte.
 *
 * `POST /canje/solicitar-codigo` y `POST /canje/validar-codigo` (2026-11-13, pedido explícito del
 * usuario) — código de autorización de un solo uso para que el cliente autorice canjear sus
 * puntos en una venta. Mismo auth `verifyKoreApiKey` — ver `loyalty-kore-canje.service.js` para el
 * detalle completo y la aclaración del cambio de responsabilidad (antes era de Kore, ahora de
 * Numo). Se llaman ANTES de `POST /venta` — ese endpoint no cambia en nada.
 *
 * `limiterSolicitarCanje` (2026-11-13, hardening pedido explícito del usuario, revisión de
 * seguridad) — límite de 3 solicitudes / 15 min por TELÉFONO (no por IP, a diferencia de los
 * limiters de `loyalty-public.routes.js`): este tráfico es servidor-a-servidor con una sola IP real
 * (el servidor de Kore), así que limitar por IP no protegería nada. Lo que hace falta acotar es
 * cuántas veces se puede molestar/invalidar el código vigente de UN MISMO cliente, incluso si la
 * `X-Api-Key` se filtrara — sin esto, nada impedía spamear códigos sin parar al mismo teléfono ni
 * griefear a un cajero en curso (cada `solicitar-codigo` nuevo pisa el código anterior, ver
 * `loyalty-kore-canje.service.js`). Corre DESPUÉS de `verifyKoreApiKey` a propósito: si corriera
 * antes, cualquiera sin la API key podría agotar el presupuesto de un teléfono ajeno sin
 * autenticarse, negándole el servicio al tráfico real de Kore. `RedisStore` (no memoria), mismo
 * motivo que el resto de los limiters de Lealtad: presupuesto real cross-worker bajo cluster
 * nativo (ver `loyalty-public.routes.js#_redisStore`).
 */
const express = require('express');
const rateLimit = require('express-rate-limit');
const { RedisStore } = require('rate-limit-redis');
const redisClient = require('../../../shared/utils/redis-client');
const { verifyKoreApiKey } = require('../../../shared/middleware/kore-api-key-auth');
const { authenticate, permit } = require('../../../shared/middleware/auth');
const { asyncHandler } = require('../../../shared/middleware/error-handler');
const { PERMISSIONS } = require('../../../shared/config/rbac');
const { logger } = require('../../../shared/utils/logger');
const ventaService = require('./loyalty-kore-venta.service');
const ventaCanceladaService = require('./loyalty-kore-venta-cancelada.service');
const ventaFallidaService = require('./loyalty-venta-fallida.service');
const canjeCodigoService = require('./loyalty-kore-canje.service');
const ventaLogService = require('./loyalty-kore-venta-log.service');

const router = express.Router();

const limiterSolicitarCanje = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 3,
  standardHeaders: true,
  legacyHeaders: false,
  passOnStoreError: true,
  // Key por teléfono del body (ya parseado por express.json() en app.js antes de llegar acá), no
  // por IP — ver docstring de arriba.
  keyGenerator: (req) => String(req.body?.telefono ?? 'sin-telefono'),
  handler: (_req, res) => res.status(429).json({
    success: false,
    error: 'Demasiadas solicitudes de código para este teléfono. Intentá de nuevo más tarde.',
    code: 'RATE_LIMIT_EXCEEDED',
    retryAfter: 60,
  }),
  store: new RedisStore({ prefix: 'rl:loyalty-canje-solicitar:', sendCommand: (...args) => redisClient.sendCommand(args) }),
});

/**
 * @swagger
 * /api/loyalty/kore/saldo:
 *   get:
 *     tags: [Lealtad Kore - Venta]
 *     summary: Consulta el saldo de puntos de un cliente
 *     security:
 *       - apiKeyAuth: []
 *     parameters:
 *       - in: query
 *         name: telefono
 *         schema:
 *           type: string
 *           example: "9511771923"
 *         description: Teléfono del cliente. Requerido si no se envía koreClienteId.
 *       - in: query
 *         name: koreClienteId
 *         schema:
 *           type: string
 *           example: "1134"
 *         description: ID del cliente en Kore. Requerido si no se envía telefono.
 *     responses:
 *       200:
 *         description: Saldo de puntos del cliente.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *             example:
 *               tieneTarjeta: true
 *               saldoDisponible: 1250.00
 *               nivel: "Plata"
 *       401:
 *         description: X-Api-Key inválida o faltante.
 */
router.get('/saldo', verifyKoreApiKey, asyncHandler(async (req, res) => {
  const { telefono, koreClienteId } = req.query ?? {};
  res.json(await ventaService.consultarSaldoKore({ telefono, koreClienteId }));
}));

// Auditoría append-only (2026-11-17, pedido explícito del usuario, ver
// loyalty-kore-venta-log.service.js): registra CADA request real de Kore a /venta y /ventas, con
// su resultado — nunca bloquea ni cambia la respuesta real del webhook (si el log falla, se traga
// el error, mismo criterio que la emisión de socket en otros webhooks de Lealtad).
/**
 * @swagger
 * /api/loyalty/kore/venta:
 *   post:
 *     tags: [Lealtad Kore - Venta]
 *     summary: Procesa una venta (acumulación/canje de puntos)
 *     security:
 *       - apiKeyAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             description: Payload de venta de Kore.
 *           example:
 *             telefono: "9511771923"
 *             koreClienteId: "1134"
 *             serieFolio: "A0-261100034"
 *             conceptoId: "5fbd63472ab0c60001c64692"
 *             metodoPago: "PUE"
 *             abonos:
 *               - idAbono: "6aee0e1b2ff6fb0001f89db0"
 *                 fecha: "2026-11-06T17:27:55.620246Z"
 *                 subtotal: 579.3621
 *                 total: 672.06
 *                 formasPago:
 *                   - formaPagoId: "5f85d826acfcf300018a088a"
 *                     importe: 672.06
 *                 usuarioAplicaId: "auth0|6494db046ee6f0aa8795c14e"
 *                 puntosAbonados: 8.69
 *     responses:
 *       200:
 *         description: Venta procesada.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *             example:
 *               serieFolio: "A0-261100034"
 *               status: "aplicado"
 *               puntosDescontados: 0
 *               puntosAcumulados: 8.69
 *               saldoResultante: 108.69
 *               abonos:
 *                 - idAbono: "6aee0e1b2ff6fb0001f89db0"
 *                   success: true
 *                   status: "aplicado"
 *                   puntosDescontados: 0
 *                   puntosAcumulados: 8.69
 *       401:
 *         description: X-Api-Key inválida o faltante.
 */
router.post('/venta', verifyKoreApiKey, asyncHandler(async (req, res) => {
  // Diagnóstico (2026-11-17, pedido explícito del usuario) — mismo criterio que
  // loyalty-kore-cliente.routes.js: loguear el payload real de Kore para poder inspeccionar el
  // contrato exacto de acumulación de puntos en test/producción.
  logger.info('[KoreVentaWebhook] payload recibido (/venta)', req.body);
  try {
    const resultado = await ventaService.procesarVentaKore(req.body);
    ventaLogService.registrarVentaLog({ endpoint: 'venta', payload: req.body, resultado: 'exito' })
      .catch((err) => logger.error(`[loyalty-kore-venta-log] No se pudo registrar el log (/venta): ${err.message}`));
    res.json(resultado);
  } catch (err) {
    ventaLogService.registrarVentaLog({ endpoint: 'venta', payload: req.body, resultado: 'error', detalle: err.message })
      .catch((logErr) => logger.error(`[loyalty-kore-venta-log] No se pudo registrar el log (/venta): ${logErr.message}`));
    throw err;
  }
}));

/**
 * @swagger
 * /api/loyalty/kore/ventas:
 *   post:
 *     tags: [Lealtad Kore - Venta]
 *     summary: Procesa un lote de ventas (acumulación/canje de puntos)
 *     security:
 *       - apiKeyAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               ventas:
 *                 type: array
 *                 items:
 *                   type: object
 *                   description: Payload de venta de Kore (mismo formato que POST /venta).
 *           example:
 *             ventas:
 *               - telefono: "9511771923"
 *                 serieFolio: "A1-260605500"
 *                 conceptoId: "5fbbe696d6cdbf0001adde15"
 *                 metodoPago: "PUE"
 *                 abonos:
 *                   - idAbono: "6aee0d412ff6fb0001f89d55"
 *                     fecha: "2026-10-27T12:00:00Z"
 *                     subtotal: 1000.00
 *                     total: 1160.00
 *                     formasPago:
 *                       - formaPagoId: "5f85d826acfcf300018a088a"
 *                         importe: 1160.00
 *                     usuarioAplicaId: "auth0|6494db046ee6f0aa8795c14e"
 *                     puntosAbonados: 15.00
 *     responses:
 *       200:
 *         description: Lote de ventas procesado.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *             example:
 *               resultados:
 *                 - serieFolio: "A1-260605500"
 *                   success: true
 *                   status: "aplicado"
 *                   puntosDescontados: 0
 *                   puntosAcumulados: 15.00
 *                   saldoResultante: 115.00
 *                   abonos:
 *                     - idAbono: "6aee0d412ff6fb0001f89d55"
 *                       success: true
 *                       status: "aplicado"
 *                       puntosDescontados: 0
 *                       puntosAcumulados: 15.00
 *       401:
 *         description: X-Api-Key inválida o faltante.
 */
router.post('/ventas', verifyKoreApiKey, asyncHandler(async (req, res) => {
  logger.info('[KoreVentaWebhook] payload recibido (/ventas)', req.body);
  try {
    const resultado = await ventaService.procesarLoteVentasKore(req.body?.ventas);
    ventaLogService.registrarVentaLog({ endpoint: 'ventas', payload: req.body, resultado: 'exito' })
      .catch((err) => logger.error(`[loyalty-kore-venta-log] No se pudo registrar el log (/ventas): ${err.message}`));
    res.json(resultado);
  } catch (err) {
    ventaLogService.registrarVentaLog({ endpoint: 'ventas', payload: req.body, resultado: 'error', detalle: err.message })
      .catch((logErr) => logger.error(`[loyalty-kore-venta-log] No se pudo registrar el log (/ventas): ${logErr.message}`));
    throw err;
  }
}));

/**
 * @swagger
 * /api/loyalty/kore/venta-cancelada:
 *   post:
 *     tags: [Lealtad Kore - Venta]
 *     summary: Avisa que Kore canceló una venta ya procesada
 *     security:
 *       - apiKeyAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               serieFolio:
 *                 type: string
 *               serieFolioCancelacion:
 *                 type: string
 *               tipoCancelacion:
 *                 type: string
 *               importeCancelado:
 *                 type: number
 *               montoVenta:
 *                 type: number
 *               motivoCancelacion:
 *                 type: string
 *               clienteId:
 *                 type: string
 *               nombreCliente:
 *                 type: string
 *           examples:
 *             cancelacionTotal:
 *               summary: Cancelación total de la venta
 *               value:
 *                 serieFolio: "A0-260605320"
 *                 serieFolioCancelacion: "A0-900000001"
 *                 tipoCancelacion: "total"
 *                 motivoCancelacion: "cliente canceló la compra"
 *             cancelacionParcial:
 *               summary: Devolución parcial de la venta
 *               value:
 *                 serieFolio: "A0-260605320"
 *                 serieFolioCancelacion: "A0-900000002"
 *                 tipoCancelacion: "parcial"
 *                 importeCancelado: 200.00
 *                 motivoCancelacion: "devolución parcial de mercancía"
 *     responses:
 *       200:
 *         description: Cancelación procesada.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *             example:
 *               ok: true
 *               revertido: true
 *               tipoCancelacion: "total"
 *               puntosRevertidos: "-60.00"
 *               saldoResultante: 140
 *       401:
 *         description: X-Api-Key inválida o faltante.
 */
router.post('/venta-cancelada', verifyKoreApiKey, asyncHandler(async (req, res) => {
  const {
    serieFolio, serieFolioCancelacion, tipoCancelacion, importeCancelado,
    montoVenta, motivoCancelacion, clienteId, nombreCliente,
  } = req.body ?? {};
  res.json(await ventaCanceladaService.procesarVentaCanceladaKore({
    serieFolio, serieFolioCancelacion, tipoCancelacion, importeCancelado,
    montoVenta, motivoCancelacion, clienteId, nombreCliente,
  }));
}));

router.get('/ventas-fallidas', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  const { soloNoResueltas } = req.query ?? {};
  res.json(await ventaFallidaService.listarVentasFallidas({ soloNoResueltas: soloNoResueltas === 'true' }));
}));

router.patch('/ventas-fallidas/:id/resolver', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await ventaFallidaService.marcarVentaFallidaResuelta(req.params.id, req.user));
}));

router.get('/ventas-canceladas-con-deposito', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await ventaCanceladaService.listarVentasCanceladasConDeposito());
}));

// Auditoría de pagos Kore (2026-11-17, pedido explícito del usuario) — mismo permiso que
// GET /auditoria/eventos-seguridad (loyalty.routes.js), para que viva en la misma pestaña de
// "Auditoría de Lealtad" del frontend.
router.get('/ventas-log', authenticate, permit(PERMISSIONS.LOYALTY_AUDITORIA), asyncHandler(async (req, res) => {
  const { resultado, endpoint, desde, hasta, limit, offset } = req.query ?? {};
  res.json(await ventaLogService.listarVentasLog({ resultado, endpoint, desde, hasta, limit, offset }));
}));

/**
 * @swagger
 * /api/loyalty/kore/canje/solicitar-codigo:
 *   post:
 *     tags: [Lealtad Kore - Venta]
 *     summary: Solicita un código de autorización de un solo uso para canjear puntos
 *     description: Limitado a 3 solicitudes / 15 min por teléfono.
 *     security:
 *       - apiKeyAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               telefono:
 *                 type: string
 *           example:
 *             telefono: "5512345678"
 *     responses:
 *       200:
 *         description: Código generado y enviado al cliente.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *             example:
 *               mensaje: "Código enviado"
 *       401:
 *         description: X-Api-Key inválida o faltante.
 *       429:
 *         description: Demasiadas solicitudes de código para este teléfono.
 */
router.post('/canje/solicitar-codigo', verifyKoreApiKey, limiterSolicitarCanje, asyncHandler(async (req, res) => {
  res.json(await canjeCodigoService.solicitarCodigoCanje(req.body?.telefono));
}));

/**
 * @swagger
 * /api/loyalty/kore/canje/validar-codigo:
 *   post:
 *     tags: [Lealtad Kore - Venta]
 *     summary: Valida el código de autorización para canjear puntos en una venta
 *     security:
 *       - apiKeyAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               telefono:
 *                 type: string
 *               codigo:
 *                 type: string
 *           example:
 *             telefono: "5512345678"
 *             codigo: "483920"
 *     responses:
 *       200:
 *         description: Código válido.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *             example:
 *               valido: true
 *       401:
 *         description: X-Api-Key inválida o faltante.
 */
router.post('/canje/validar-codigo', verifyKoreApiKey, asyncHandler(async (req, res) => {
  res.json(await canjeCodigoService.validarCodigoCanje(req.body?.telefono, req.body?.codigo));
}));

module.exports = router;
