'use strict';

/**
 * shared/routes/config.routes.js
 * ─────────────────────────────────────────────────────────────────────────────
 * "Configuraciones Globales" — administración runtime del catálogo de
 * secciones/valores (ver global-config.service.js). Montado en app.js bajo
 * /api/config.
 *
 * Permisos en dos niveles (confirmado con el usuario, sigue el patrón granular
 * ya usado en banks:erp:read/link/unlink):
 *   - config:manage         → ver/crear secciones, ver valores (secretos
 *                              enmascarados), editar cualquier valor.
 *   - config:secrets:reveal → además, desenmascarar el valor real de un
 *                              secreto puntual (acción registrada en el audit log).
 */

const express = require('express');
const { authenticate, permit } = require('../middleware/auth');
const { asyncHandler }         = require('../middleware/error-handler');
const { PERMISSIONS }          = require('../config/rbac');
const svc = require('../services/global-config.service');

const router = express.Router();

// GET /api/config/sections — catálogo completo
router.get('/sections', authenticate, permit(PERMISSIONS.CONFIG_MANAGE), asyncHandler(async (_req, res) => {
  res.json(await svc.listSections());
}));

// POST /api/config/sections — alta de una sección nueva (para ir migrando el resto del .env)
router.post('/sections', authenticate, permit(PERMISSIONS.CONFIG_MANAGE), asyncHandler(async (req, res) => {
  const { clave, nombre, descripcion, modulosAfectados } = req.body;
  if (!clave || !nombre) return res.status(400).json({ error: 'Se requiere clave y nombre' });
  const section = await svc.createSection({ clave, nombre, descripcion, modulosAfectados });
  res.status(201).json(section);
}));

// PUT /api/config/sections/:sectionId — edita nombre/descripcion/modulosAfectados de una sección
// ya existente. La clave NUNCA se edita acá (ver updateSection en global-config.service.js).
router.put('/sections/:sectionId', authenticate, permit(PERMISSIONS.CONFIG_MANAGE), asyncHandler(async (req, res) => {
  const { nombre, descripcion, modulosAfectados } = req.body;
  if (!nombre) return res.status(400).json({ error: 'Se requiere nombre' });
  const section = await svc.updateSection(req.params.sectionId, { nombre, descripcion, modulosAfectados });
  res.json(section);
}));

// GET /api/config/sections/:sectionId/configs — valores de una sección (secretos enmascarados)
router.get('/sections/:sectionId/configs', authenticate, permit(PERMISSIONS.CONFIG_MANAGE), asyncHandler(async (req, res) => {
  res.json(await svc.listConfigsBySection(req.params.sectionId));
}));

// PUT /api/config/sections/:sectionClave/configs/:clave — crea/edita un valor por su clave
// natural (sectionClave+clave, no id — un config nuevo todavía no tiene id numérico).
router.put('/sections/:sectionClave/configs/:clave', authenticate, permit(PERMISSIONS.CONFIG_MANAGE), asyncHandler(async (req, res) => {
  const { sectionClave, clave } = req.params;
  const { valor, esSecreto, tipo, descripcion } = req.body;
  if (valor === undefined || valor === null) return res.status(400).json({ error: 'Se requiere valor' });

  const configId = await svc.setValue(sectionClave, clave, String(valor), {
    esSecreto:     !!esSecreto,
    tipo:          tipo || 'texto',
    descripcion:   descripcion ?? null,
    usuarioId:     req.user.dbId ?? req.user._id,
    usuarioNombre: req.user.nombre,
  });
  res.json({ id: configId });
}));

// POST /api/config/:configId/reveal — desenmascara el valor real de un secreto
router.post('/:configId/reveal', authenticate, permit(PERMISSIONS.CONFIG_SECRETS_REVEAL), asyncHandler(async (req, res) => {
  const valor = await svc.revealSecret(req.params.configId, {
    usuarioId:     req.user.dbId ?? req.user._id,
    usuarioNombre: req.user.nombre,
  });
  res.json({ valor });
}));

// GET /api/config/:configId/audit — historial de cambios de un valor puntual
router.get('/:configId/audit', authenticate, permit(PERMISSIONS.CONFIG_MANAGE), asyncHandler(async (req, res) => {
  res.json(await svc.listAuditLog(req.params.configId));
}));

module.exports = router;
