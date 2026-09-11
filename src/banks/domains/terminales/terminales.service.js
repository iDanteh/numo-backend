'use strict';

const { Terminal, CentroCosto } = require('../../../shared/models/postgres');
const { Op }                    = require('sequelize');
const { NotFoundError, BadRequestError } = require('../../shared/errors/AppError');

/**
 * Listar terminales.
 * Acepta ?search=texto para filtrar por nombre comercial o número de serie.
 * Acepta ?includeInactive=true para incluir inactivas.
 */
async function list({ search, includeInactive } = {}) {
  const where = {};
  if (!includeInactive) where.isActive = true;
  if (search) {
    where[Op.or] = [
      { nombreComercial: { [Op.iLike]: `%${search}%` } },
      { numeroSerie:      { [Op.iLike]: `%${search}%` } },
    ];
  }
  return Terminal.findAll({
    where,
    include: [{ model: CentroCosto, as: 'centroCosto', attributes: ['id', 'clave', 'sucursal'] }],
    order: [['nombreComercial', 'ASC']],
  });
}

async function getById(id) {
  const t = await Terminal.findByPk(id, {
    include: [{ model: CentroCosto, as: 'centroCosto', attributes: ['id', 'clave', 'sucursal'] }],
  });
  if (!t) throw new NotFoundError(`Terminal #${id} no encontrada`);
  return t;
}

async function _validarCentroCosto(centroCostoId) {
  const cc = await CentroCosto.findByPk(centroCostoId);
  if (!cc) throw new BadRequestError(`Centro de costo #${centroCostoId} no encontrado`);
  return cc;
}

async function create(data) {
  const { nombreComercial, numeroSerie, centroCostoId } = data;
  if (!nombreComercial) throw new BadRequestError('nombreComercial es requerido');
  if (!numeroSerie)     throw new BadRequestError('numeroSerie es requerido');
  if (!centroCostoId)   throw new BadRequestError('centroCostoId es requerido');

  await _validarCentroCosto(centroCostoId);

  const existe = await Terminal.findOne({ where: { numeroSerie } });
  if (existe) throw new BadRequestError(`Ya existe una terminal con número de serie "${numeroSerie}"`);

  const t = await Terminal.create({ nombreComercial, numeroSerie, centroCostoId, isActive: true });
  return getById(t.id);
}

async function update(id, data) {
  const t = await Terminal.findByPk(id);
  if (!t) throw new NotFoundError(`Terminal #${id} no encontrada`);

  const { nombreComercial, numeroSerie, centroCostoId, isActive } = data;

  if (numeroSerie && numeroSerie !== t.numeroSerie) {
    const existe = await Terminal.findOne({ where: { numeroSerie } });
    if (existe) throw new BadRequestError(`Ya existe una terminal con número de serie "${numeroSerie}"`);
  }
  if (centroCostoId != null) await _validarCentroCosto(centroCostoId);

  const fields = {};
  if (nombreComercial != null) fields.nombreComercial = nombreComercial;
  if (numeroSerie      != null) fields.numeroSerie     = numeroSerie;
  if (centroCostoId    != null) fields.centroCostoId   = centroCostoId;
  if (isActive         != null) fields.isActive        = isActive;

  await t.update(fields);
  return getById(id);
}

async function softDelete(id) {
  const t = await Terminal.findByPk(id);
  if (!t) throw new NotFoundError(`Terminal #${id} no encontrada`);
  await t.update({ isActive: false });
  return { ok: true, id };
}

module.exports = { list, getById, create, update, softDelete };
