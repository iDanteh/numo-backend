'use strict';

/**
 * domains/ledger/loyalty-kore-venta.routes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — router del webhook Kore→Numo de venta, montado en
 * `/api/loyalty/kore` (app.js). Separado de `loyalty.routes.js` a propósito:
 * ese router es staff-only de punta a punta por diseño (ver su propio
 * docstring) y nunca debe mezclar autenticación por API key server-to-server.
 *
 * `GET /saldo`, `POST /venta` y `POST /ventas` los llama Kore (`verifyKoreApiKey`, mismo
 * secreto que Solicitudes de Cobro/`cxc-reversiones` — ver contrato en
 * `Docs/integracion-kore-puntos-lealtad-DRAFT.md`). `POST /ventas` (plural, 2026-10-27,
 * pedido explícito del usuario) procesa varias ventas en un solo request — ver
 * `loyalty-kore-venta.service.js#procesarLoteVentasKore`; no reemplaza a `POST /venta`,
 * que sigue existiendo para una sola venta. `GET /ventas-fallidas` y
 * `PATCH /ventas-fallidas/:id/resolver` son para el equipo de Numo (sesión
 * staff normal, mismo permiso que el resto de Configuración de Lealtad) — sin
 * pantalla de Angular todavía, eso es un paso aparte.
 */
const express = require('express');
const { verifyKoreApiKey } = require('../../../shared/middleware/kore-api-key-auth');
const { authenticate, permit } = require('../../../shared/middleware/auth');
const { asyncHandler } = require('../../../shared/middleware/error-handler');
const { PERMISSIONS } = require('../../../shared/config/rbac');
const ventaService = require('./loyalty-kore-venta.service');
const ventaFallidaService = require('./loyalty-venta-fallida.service');

const router = express.Router();

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

router.get('/ventas-fallidas', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  const { soloNoResueltas } = req.query ?? {};
  res.json(await ventaFallidaService.listarVentasFallidas({ soloNoResueltas: soloNoResueltas === 'true' }));
}));

router.patch('/ventas-fallidas/:id/resolver', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await ventaFallidaService.marcarVentaFallidaResuelta(req.params.id, req.user));
}));

module.exports = router;
