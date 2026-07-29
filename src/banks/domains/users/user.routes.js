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
    const empresaRfcs = req.user.empresaRfcs ?? [];
    let empresas = [];
    if (empresaRfcs.length) {
      const { Entity } = require('../../../shared/models/postgres');
      const { Op } = require('sequelize');
      const rows = await Entity.findAll({ where: { rfc: { [Op.in]: empresaRfcs } }, attributes: ['rfc', 'nombre'], raw: true });
      empresas = rows.map(r => ({ rfc: r.rfc, nombre: r.nombre }));
    }
    // Permisos efectivos = permisos del rol UNIÓN permisos extra del usuario
    // (aditivo — nunca resta lo que el rol ya concede). Ver rbac-store.js.
    const permissions = [...new Set([...(role?.permissions ?? []), ...(req.user.extraPermissions ?? [])])];
    res.json({
      dbId:   req.user.dbId,
      nombre: req.user.nombre,
      role:   req.user.role,
      permissions,
      empresas,
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

// Empresas fijas asignadas directo a este usuario (puede tener varias).
// Body: { empresaRfcs: string[] } — reemplaza la lista completa ([] = sin restricción).
router.patch('/:id/empresas', authenticate, permit('users:manage'),
  asyncHandler(async (req, res) => {
    res.json(await userSvc.updateEmpresas(req.params.id, req.body.empresaRfcs));
  }),
);

// Permisos extra asignados directo a este usuario, ADEMÁS de los que ya le da
// su rol (nunca los sustituye ni los revoca — puramente aditivo).
// Body: { extraPermissions: string[] } — reemplaza la lista completa ([] = ninguno extra).
router.patch('/:id/permissions', authenticate, permit('users:manage'),
  asyncHandler(async (req, res) => {
    res.json(await userSvc.updateExtraPermissions(req.params.id, req.body.extraPermissions));
  }),
);

router.patch('/:id/toggle', authenticate, permit('users:manage'),
  asyncHandler(async (req, res) => {
    res.json(await userSvc.toggleActive(req.params.id));
  }),
);

module.exports = router;
