'use strict';

const express = require('express');
const { authenticate, authorize } = require('../../shared/middleware/auth.real');
const { asyncHandler }            = require('../../shared/middleware/error-handler');
const service                     = require('./user.service');

const router = express.Router();

// GET /api/users/me  — perfil del usuario autenticado (rol viene de DB)
router.get('/me',
  authenticate,
  asyncHandler(async (req, res) => {
    res.json({
      dbId:    req.user.dbId,
      nombre:  req.user.nombre,
      role:    req.user.role,
    });
  }),
);

// GET /api/users
router.get('/',
  authenticate,
  authorize('admin'),
  asyncHandler(async (_req, res) => {
    res.json(await service.listUsers());
  }),
);

// PATCH /api/users/:id/role
router.patch('/:id/role',
  authenticate,
  authorize('admin'),
  asyncHandler(async (req, res) => {
    res.json(await service.updateRole(req.params.id, req.body.role));
  }),
);

// PATCH /api/users/:id/toggle
router.patch('/:id/toggle',
  authenticate,
  authorize('admin'),
  asyncHandler(async (req, res) => {
    res.json(await service.toggleActive(req.params.id));
  }),
);

module.exports = router;
