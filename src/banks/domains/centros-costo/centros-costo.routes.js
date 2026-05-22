'use strict';

const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth.real');
const { asyncHandler }         = require('../../shared/middleware/error-handler');
const service                  = require('./centros-costo.service');

const router = express.Router();

// GET /api/centros-costo
router.get('/', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.list(req.query));
}));

// GET /api/centros-costo/:id
router.get('/:id', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.getById(req.params.id));
}));

// POST /api/centros-costo
router.post('/',
  authenticate,
  permit('account-plan:write'),
  asyncHandler(async (req, res) => {
    res.status(201).json(await service.create(req.body));
  }),
);

// PATCH /api/centros-costo/:id
router.patch('/:id',
  authenticate,
  permit('account-plan:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.update(req.params.id, req.body));
  }),
);

// DELETE /api/centros-costo/:id
router.delete('/:id',
  authenticate,
  permit('account-plan:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.softDelete(req.params.id));
  }),
);

module.exports = router;
