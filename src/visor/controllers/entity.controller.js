'use strict';

/**
 * visor/controllers/entity.controller.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Endpoints de entidades fiscales. Ahora usa PostgreSQL via entity.repository.
 */

const entityRepo          = require('../repositories/entity.repository');
const { asyncHandler }    = require('../../shared/middleware/error-handler');

/**
 * GET /api/entities
 */
const list = asyncHandler(async (_req, res) => {
  const entities = await entityRepo.findAll({ isActive: true });
  res.json(entities);
});

/**
 * POST /api/entities
 */
const create = asyncHandler(async (req, res) => {
  const entity = await entityRepo.create(req.body);
  res.status(201).json(entity);
});

/**
 * PATCH /api/entities/:id
 */
const update = asyncHandler(async (req, res) => {
  const entity = await entityRepo.update(req.params.id, req.body);
  if (!entity) return res.status(404).json({ error: 'Entidad no encontrada' });
  res.json(entity);
});

module.exports = { list, create, update };
