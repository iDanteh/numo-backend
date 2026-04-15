const Entity = require('../models/Entity');
const { asyncHandler } = require('../middleware/errorHandler');

/**
 * GET /api/entities
 */
const list = asyncHandler(async (req, res) => {
  const entities = await Entity.find({ isActive: true }, { fiel: 0 }).lean();
  res.json(entities);
});

/**
 * POST /api/entities
 */
const create = asyncHandler(async (req, res) => {
  const entity = await Entity.create(req.body);
  res.status(201).json(entity);
});

/**
 * PATCH /api/entities/:id
 */
const update = asyncHandler(async (req, res) => {
  const entity = await Entity.findByIdAndUpdate(req.params.id, req.body, { new: true, runValidators: true });
  if (!entity) return res.status(404).json({ error: 'Entidad no encontrada' });
  res.json(entity);
});

module.exports = { list, create, update };
