'use strict';

/**
 * visor/controllers/entity.controller.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Endpoints de entidades fiscales. Ahora usa PostgreSQL via entity.repository.
 */

const { Op }              = require('sequelize');
const entityRepo          = require('../repositories/entity.repository');
const { asyncHandler }    = require('../../shared/middleware/error-handler');
const { enviarAlertaManual } = require('../jobs/credencialesAlertJob');

/**
 * GET /api/entities
 *
 * Si el usuario tiene empresas fijas asignadas (User.empresaRfcs, asignadas
 * desde la pantalla de Roles), solo se le devuelven esas — nunca ve ni puede
 * elegir otra desde el selector. Sin restricción (array vacío) ve todas.
 */
const list = asyncHandler(async (req, res) => {
  const empresaRfcs = req.user?.empresaRfcs ?? [];
  const entities = await entityRepo.findAll({
    isActive: true,
    ...(empresaRfcs.length ? { rfc: { [Op.in]: empresaRfcs } } : {}),
  });
  res.json(entities);
});

/**
 * POST /api/entities
 */
const create = asyncHandler(async (req, res) => {
  const { rfc, nombre, tipo, regimenFiscal, domicilioFiscal, syncConfig, isOwn, esIntercompania, notes, emailsAlerta } = req.body;

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
    ...(esIntercompania  !== undefined && { esIntercompania }),
    ...(notes            !== undefined && { notes }),
    ...(emailsAlerta     !== undefined && { emailsAlerta }),
  });
  res.status(201).json(entity);
});

/**
 * PATCH /api/entities/:id
 */
const update = asyncHandler(async (req, res) => {
  const { nombre, tipo, regimenFiscal, domicilioFiscal, syncConfig, isOwn, isActive, esIntercompania, notes, emailsAlerta } = req.body;

  if (tipo !== undefined && !['moral', 'fisica'].includes(tipo)) {
    return res.status(400).json({ error: 'El campo tipo debe ser "moral" o "fisica".' });
  }

  // Whitelist explícito: nunca aceptar `fiel` ni campos de auditoría del body
  const data = {
    ...(nombre           !== undefined && { nombre }),
    ...(tipo             !== undefined && { tipo }),
    ...(regimenFiscal    !== undefined && { regimenFiscal }),
    ...(domicilioFiscal  !== undefined && { domicilioFiscal }),
    ...(syncConfig       !== undefined && { syncConfig }),
    ...(isOwn            !== undefined && { isOwn }),
    ...(isActive         !== undefined && { isActive }),
    ...(esIntercompania  !== undefined && { esIntercompania }),
    ...(notes            !== undefined && { notes }),
    ...(emailsAlerta     !== undefined && { emailsAlerta }),
  };

  const entity = await entityRepo.update(req.params.id, data);
  if (!entity) return res.status(404).json({ error: 'Entidad no encontrada' });
  res.json(entity);
});

/**
 * POST /api/entities/:id/alertar-credenciales-sat
 * Envío manual e inmediato del correo de aviso de vencimiento de credenciales.
 */
const alertarCredencialesSat = asyncHandler(async (req, res) => {
  const entity = await entityRepo.findById(req.params.id);
  if (!entity) return res.status(404).json({ error: 'Entidad no encontrada' });

  const resultado = await enviarAlertaManual(entity);
  if (!resultado.ok) return res.status(400).json({ error: resultado.motivo });
  res.json({ ok: true });
});

module.exports = { list, create, update, alertarCredencialesSat };
