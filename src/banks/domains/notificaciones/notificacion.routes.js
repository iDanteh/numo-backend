'use strict';

const express = require('express');
const { authenticate }  = require('../../shared/middleware/auth.real');
const { asyncHandler }  = require('../../shared/middleware/error-handler');
const service           = require('./notificacion.service');

const router = express.Router();

// GET /api/notificaciones?limit=
// Response: { items: Notificacion[], noLeidas: number }
// Cache-Control explícito: el polling de la campana necesita el estado real
// en cada llamada — un 304 (ETag/If-None-Match) le haría mostrar al usuario
// una notificación ya marcada como leída en otra pestaña/sesión hasta que el
// contenido cacheado "cambiara por casualidad" (confirmado con el usuario
// 2026-08-13: una notificación leída seguía apareciendo tras recargar).
router.get('/', authenticate, asyncHandler(async (req, res) => {
  res.set('Cache-Control', 'no-store');
  res.json(await service.list(req.query));
}));

// POST /api/notificaciones/:id/marcar-leida
router.post('/:id/marcar-leida', authenticate, asyncHandler(async (req, res) => {
  const notif = await service.marcarLeida(req.params.id, req.user);
  if (!notif) return res.status(404).json({ error: 'Notificación no encontrada' });
  res.json(notif);
}));

// POST /api/notificaciones/marcar-todas-leidas
router.post('/marcar-todas-leidas', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.marcarTodasLeidas(req.user));
}));

// POST /api/notificaciones/:id/marcar-resuelta
// Distinto de marcar-leida: esta es la que realmente la saca de la bandeja
// (el problema real ya se atendió), ver notificacion.service.js.
router.post('/:id/marcar-resuelta', authenticate, asyncHandler(async (req, res) => {
  const notif = await service.marcarResuelta(req.params.id, req.user);
  if (!notif) return res.status(404).json({ error: 'Notificación no encontrada' });
  res.json(notif);
}));

module.exports = router;
