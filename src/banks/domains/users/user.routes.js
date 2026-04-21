'use strict';

const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth.real');
const { asyncHandler }         = require('../../shared/middleware/error-handler');
const userSvc = require('./user.service');
const roleSvc = require('./role.service');

const router = express.Router();

// ── Roles CRUD — /api/users/roles ─────────────────────────────────────────────

// Catálogo de roles con permisos (cualquier usuario autenticado)
router.get('/roles', authenticate,
  asyncHandler(async (_req, res) => {
    res.json(await roleSvc.listRoles());
  }),
);

router.post('/roles', authenticate, permit('users:manage'),
  asyncHandler(async (req, res) => {
    res.status(201).json(await roleSvc.createRole(req.body));
  }),
);

router.patch('/roles/:value', authenticate, permit('users:manage'),
  asyncHandler(async (req, res) => {
    res.json(await roleSvc.updateRole(req.params.value, req.body));
  }),
);

router.delete('/roles/:value', authenticate, permit('users:manage'),
  asyncHandler(async (req, res) => {
    await roleSvc.deleteRole(req.params.value);
    res.json({ message: 'Rol eliminado correctamente.' });
  }),
);

// ── Permisos CRUD — /api/users/permissions ────────────────────────────────────

// Catálogo de permisos (cualquier usuario autenticado)
router.get('/permissions', authenticate,
  asyncHandler(async (_req, res) => {
    res.json(await roleSvc.listPermissions());
  }),
);

router.post('/permissions', authenticate, permit('users:manage'),
  asyncHandler(async (req, res) => {
    res.status(201).json(await roleSvc.createPermission(req.body));
  }),
);

// :key contiene ":" (ej: banks:read) — Express lo maneja correctamente
router.delete('/permissions/:key', authenticate, permit('users:manage'),
  asyncHandler(async (req, res) => {
    await roleSvc.deletePermission(req.params.key);
    res.json({ message: 'Permiso eliminado correctamente.' });
  }),
);

// ── Usuario actual ────────────────────────────────────────────────────────────

router.get('/me', authenticate,
  asyncHandler(async (req, res) => {
    const role = await roleSvc.getRoleByValue(req.user.role);
    res.json({
      dbId:        req.user.dbId,
      nombre:      req.user.nombre,
      role:        req.user.role,
      permissions: role?.permissions ?? [],
    });
  }),
);

// ── Gestión de usuarios ───────────────────────────────────────────────────────

router.get('/', authenticate, permit('users:manage'),
  asyncHandler(async (_req, res) => {
    res.json(await userSvc.listUsers());
  }),
);

router.patch('/:id/role', authenticate, permit('users:manage'),
  asyncHandler(async (req, res) => {
    res.json(await userSvc.updateRole(req.params.id, req.body.role));
  }),
);

router.patch('/:id/toggle', authenticate, permit('users:manage'),
  asyncHandler(async (req, res) => {
    res.json(await userSvc.toggleActive(req.params.id));
  }),
);

module.exports = router;
