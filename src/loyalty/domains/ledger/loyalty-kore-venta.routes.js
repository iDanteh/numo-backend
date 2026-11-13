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
const ventaService = require('./loyalty-kore-venta.service');
const ventaCanceladaService = require('./loyalty-kore-venta-cancelada.service');
const ventaFallidaService = require('./loyalty-venta-fallida.service');
const canjeCodigoService = require('./loyalty-kore-canje.service');

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

router.get('/saldo', verifyKoreApiKey, asyncHandler(async (req, res) => {
  const { telefono, koreClienteId } = req.query ?? {};
  res.json(await ventaService.consultarSaldoKore({ telefono, koreClienteId }));
}));

router.post('/venta', verifyKoreApiKey, asyncHandler(async (req, res) => {
  res.json(await ventaService.procesarVentaKore(req.body));
}));

router.post('/ventas', verifyKoreApiKey, asyncHandler(async (req, res) => {
  res.json(await ventaService.procesarLoteVentasKore(req.body?.ventas));
}));

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

router.post('/canje/solicitar-codigo', verifyKoreApiKey, limiterSolicitarCanje, asyncHandler(async (req, res) => {
  res.json(await canjeCodigoService.solicitarCodigoCanje(req.body?.telefono));
}));

router.post('/canje/validar-codigo', verifyKoreApiKey, asyncHandler(async (req, res) => {
  res.json(await canjeCodigoService.validarCodigoCanje(req.body?.telefono, req.body?.codigo));
}));

module.exports = router;
