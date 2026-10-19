'use strict';

/**
 * loyalty/loyalty.routes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Sistema de Lealtad — router de STAFF, montado en `/api/loyalty` (PR4/8). Cada
 * ruta usa `authenticate, permit(PERMISSIONS.LOYALTY_X), asyncHandler`, mismo
 * patrón que `system-monitor.routes.js`/`bank.routes.js:117`. El lookup público
 * (anónimo) vive en un router aparte (`loyalty-public.routes.js`) — JAMÁS se
 * monta acá, y este router nunca importa `authenticate`/`permit` de forma
 * condicional: es staff-only de punta a punta (D9, ver design.md).
 *
 * La lógica de negocio vive por completo en los servicios de dominio
 * (PR2/PR3): este archivo solo valida permisos y hace el mapeo HTTP↔servicio.
 */
const express = require('express');
const { authenticate, permit } = require('../shared/middleware/auth');
const { asyncHandler } = require('../shared/middleware/error-handler');
const { PERMISSIONS } = require('../shared/config/rbac');
const cardService = require('./domains/cards/loyalty-card.service');
const ledgerService = require('./domains/ledger/loyalty-ledger.service');
const configService = require('./domains/config/loyalty-config.service');

const router = express.Router();

// ── Tarjetas ──────────────────────────────────────────────────────────────────
router.get('/cards', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  const { search, nivel, activa, page, limit } = req.query;
  res.json(await cardService.listar({ search, nivel, activa, page, limit }));
}));

router.post('/cards', authenticate, permit(PERMISSIONS.LOYALTY_TARJETAS_CREAR), asyncHandler(async (req, res) => {
  const { telefono, nombre } = req.body ?? {};
  const resultado = await cardService.crearTarjeta({ telefono, nombre }, req.user);
  res.status(201).json(resultado);
}));

// ── Rutas estáticas ANTES que /:id para no colisionar con el parámetro ───────
router.get('/cards/:id/movimientos', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  const { page, limit } = req.query;
  res.json(await ledgerService.listarMovimientos(req.params.id, { page, limit }));
}));

router.get('/cards/:id/niveles', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await ledgerService.listarNiveles(req.params.id));
}));

router.get('/cards/:id', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await cardService.obtener(req.params.id));
}));

router.patch('/cards/:id', authenticate, permit(PERMISSIONS.LOYALTY_TARJETAS_EDITAR), asyncHandler(async (req, res) => {
  const { nombre, telefono, activa } = req.body ?? {};
  res.json(await cardService.editarTarjeta(req.params.id, { nombre, telefono, activa }, req.user));
}));

router.post('/cards/:id/nip', authenticate, permit(PERMISSIONS.LOYALTY_TARJETAS_EDITAR), asyncHandler(async (req, res) => {
  res.json(await cardService.cambiarNip(req.params.id, req.user));
}));

router.put('/cards/:id/nivel', authenticate, permit(PERMISSIONS.LOYALTY_TARJETAS_NIVEL), asyncHandler(async (req, res) => {
  const { nivel, motivo } = req.body ?? {};
  res.json(await cardService.cambiarNivel(req.params.id, nivel, { motivo }, req.user));
}));

// ── Movimientos ───────────────────────────────────────────────────────────────
router.post('/cards/:id/acumulaciones', authenticate, permit(PERMISSIONS.LOYALTY_MOVIMIENTOS), asyncHandler(async (req, res) => {
  const { montoVenta, centroCostoId } = req.body ?? {};
  const resultado = await ledgerService.registrarAcumulacion({ cardId: req.params.id, montoVenta, centroCostoId }, req.user);
  res.status(201).json(resultado);
}));

router.post('/cards/:id/canjes', authenticate, permit(PERMISSIONS.LOYALTY_MOVIMIENTOS), asyncHandler(async (req, res) => {
  const { puntos, centroCostoId } = req.body ?? {};
  const resultado = await ledgerService.registrarCanje({ cardId: req.params.id, puntos, centroCostoId }, req.user);
  res.status(201).json(resultado);
}));

// AJUSTES: permiso independiente de LOYALTY_TARJETAS_NIVEL (specs/loyalty-permissions
// "Ajustes and Tarjetas:Nivel Do Not Imply Each Other") — ninguno de los dos
// sustituye al otro, cada uno gatea exclusivamente su propia ruta.
router.post('/cards/:id/ajustes', authenticate, permit(PERMISSIONS.LOYALTY_AJUSTES), asyncHandler(async (req, res) => {
  const { puntos, reasonId, justificacion } = req.body ?? {};
  const resultado = await ledgerService.registrarAjuste({ cardId: req.params.id, puntos, reasonId, justificacion }, req.user);
  res.status(201).json(resultado);
}));

// ── Configuración ─────────────────────────────────────────────────────────────
// NOTA (D10): esto es LOYALTY_CONFIG/LOYALTY_READ, nunca el genérico
// config:manage — ese es admin-only y expone todas las secciones de
// Configuraciones Globales, no solo 'lealtad'.
router.get('/config', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await configService.obtenerPorcentajes());
}));

router.put('/config/porcentajes', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.guardarPorcentajes(req.body, req.user));
}));

router.get('/reasons', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await configService.listarReasons({ soloActivos: req.query.soloActivos === 'true' }));
}));

router.post('/reasons', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.status(201).json(await configService.crearReason(req.body ?? {}));
}));

router.patch('/reasons/:id', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.editarReason(req.params.id, req.body ?? {}));
}));

router.get('/sucursales', authenticate, permit(PERMISSIONS.LOYALTY_READ), asyncHandler(async (req, res) => {
  res.json(await configService.listarSucursales());
}));

router.patch('/sucursales/:id', authenticate, permit(PERMISSIONS.LOYALTY_CONFIG), asyncHandler(async (req, res) => {
  res.json(await configService.actualizarSucursal(req.params.id, req.body ?? {}));
}));

module.exports = router;
