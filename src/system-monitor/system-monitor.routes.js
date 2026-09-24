'use strict';

// Decisión de diseño (2026-09-24): se evaluó Server-Sent Events para "tiempo real",
// pero se descartó — la autenticación de este backend es 100% Bearer JWT (Auth0),
// inyectado por el AuthHttpInterceptor de @auth0/auth0-angular SOLO en llamadas de
// HttpClient. `EventSource` (API nativa de SSE) no pasa por ese interceptor y no
// puede mandar headers custom — la única forma de autenticarlo sería el JWT en la
// query string, un downgrade de seguridad real (queda en logs de acceso, historial
// del navegador). Tampoco se reusa el Socket.IO ya inicializado
// (banks/shared/socket.js): esa conexión no autentica nada, solo confía en el
// auth0Sub que el propio cliente declara — servir datos admin-only por ahí rompería
// el gate de permisos. Se resuelve con polling autenticado desde el frontend
// (HttpClient normal, mismo interceptor que el resto de la app) — /snapshot ya
// devuelve el estado completo en cada llamada, sin necesidad de un endpoint push.
const express = require('express');
const { authenticate, permit } = require('../shared/middleware/auth');
const { asyncHandler } = require('../shared/middleware/error-handler');
const { PERMISSIONS } = require('../shared/config/rbac');
const { getSnapshot } = require('./system-monitor.service');
const { getHistorial } = require('./system-monitor-historial.service');

const router = express.Router();

router.get('/snapshot', authenticate, permit(PERMISSIONS.SYSTEM_MONITOR_READ), asyncHandler(async (req, res) => {
  const snapshot = await getSnapshot();
  res.json(snapshot);
}));

// GET /api/system-monitor/historial?fechaInicio=&fechaFin= — mismos nombres de
// query param que <app-date-range-popover> emite (rangeChange), sin rango explícito
// cae a las últimas 24h (ver getHistorial). Mismo permiso que /snapshot: es la
// misma data, solo que persistida, no otro nivel de acceso.
router.get('/historial', authenticate, permit(PERMISSIONS.SYSTEM_MONITOR_READ), asyncHandler(async (req, res) => {
  const { fechaInicio, fechaFin } = req.query;
  const historial = await getHistorial({ fechaInicio, fechaFin });
  res.json(historial);
}));

module.exports = router;
