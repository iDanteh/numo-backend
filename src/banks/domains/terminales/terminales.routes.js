'use strict';

const express = require('express');
const { authenticate, permit } = require('../../shared/middleware/auth.real');
const { asyncHandler }         = require('../../shared/middleware/error-handler');
const service                  = require('./terminales.service');

const router = express.Router();

// GET /api/terminales
router.get('/', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.list(req.query));
}));

// GET /api/terminales/:id
router.get('/:id', authenticate, asyncHandler(async (req, res) => {
  res.json(await service.getById(req.params.id));
}));

// POST /api/terminales
router.post('/',
  authenticate,
  permit('account-plan:write'),
  asyncHandler(async (req, res) => {
    res.status(201).json(await service.create(req.body));
  }),
);

// PATCH /api/terminales/:id
router.patch('/:id',
  authenticate,
  permit('account-plan:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.update(req.params.id, req.body));
  }),
);

// DELETE /api/terminales/:id
router.delete('/:id',
  authenticate,
  permit('account-plan:write'),
  asyncHandler(async (req, res) => {
    res.json(await service.softDelete(req.params.id));
  }),
);

module.exports = router;
