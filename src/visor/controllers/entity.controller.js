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
  const { rfc, nombre, tipo, regimenFiscal, domicilioFiscal, syncConfig, isOwn, notes } = req.body;

  if (!rfc || !nombre || !tipo) {
    return res.status(400).json({ error: 'Los campos rfc, nombre y tipo son obligatorios.' });
  }
  if (!['moral', 'fisica'].includes(tipo)) {
    return res.status(400).json({ error: 'El campo tipo debe ser "moral" o "fisica".' });
  }

  // Whitelist explícito: nunca aceptar `fiel`, `isActive` ni campos internos del body
  const entity = await entityRepo.create({
    rfc,
    nombre,
    tipo,
    ...(regimenFiscal    !== undefined && { regimenFiscal }),
    ...(domicilioFiscal  !== undefined && { domicilioFiscal }),
    ...(syncConfig       !== undefined && { syncConfig }),
    ...(isOwn            !== undefined && { isOwn }),
    ...(notes            !== undefined && { notes }),
  });
  res.status(201).json(entity);
});

/**
 * PATCH /api/entities/:id
 */
const update = asyncHandler(async (req, res) => {
  const { nombre, tipo, regimenFiscal, domicilioFiscal, syncConfig, isOwn, isActive, notes } = req.body;

  if (tipo !== undefined && !['moral', 'fisica'].includes(tipo)) {
    return res.status(400).json({ error: 'El campo tipo debe ser "moral" o "fisica".' });
  }

  // Whitelist explícito: nunca aceptar `fiel` ni campos de auditoría del body
  const data = {
    ...(nombre          !== undefined && { nombre }),
    ...(tipo            !== undefined && { tipo }),
    ...(regimenFiscal   !== undefined && { regimenFiscal }),
    ...(domicilioFiscal !== undefined && { domicilioFiscal }),
    ...(syncConfig      !== undefined && { syncConfig }),
    ...(isOwn           !== undefined && { isOwn }),
    ...(isActive        !== undefined && { isActive }),
    ...(notes           !== undefined && { notes }),
  };

  const entity = await entityRepo.update(req.params.id, data);
  if (!entity) return res.status(404).json({ error: 'Entidad no encontrada' });
  res.json(entity);
});

module.exports = { list, create, update };
