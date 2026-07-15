'use strict';

const { CentroCosto } = require('../../../shared/models/postgres');
const { Op }          = require('sequelize');
const { NotFoundError, BadRequestError } = require('../../shared/errors/AppError');

/**
 * Listar centros de costo.
 * Acepta ?search=texto para filtrar por clave o sucursal.
 * Acepta ?includeInactive=true para incluir inactivos.
 */
async function list({ search, includeInactive } = {}) {
  const where = {};
  if (!includeInactive) where.isActive = true;
  if (search) {
    where[Op.or] = [
      { clave:     { [Op.iLike]: `%${search}%` } },
      { sucursal:  { [Op.iLike]: `%${search}%` } },
    ];
  }
  return CentroCosto.findAll({ where, order: [['clave', 'ASC']] });
}

async function getById(id) {
  const cc = await CentroCosto.findByPk(id);
  if (!cc) throw new NotFoundError(`Centro de costo #${id} no encontrado`);
  return cc;
}

async function create(data) {
  const { clave, sucursal, serieFacturacion } = data;
  if (!clave)    throw new BadRequestError('clave es requerida');
  if (!sucursal) throw new BadRequestError('sucursal es requerida');

  const existe = await CentroCosto.findOne({ where: { clave } });
  if (existe) throw new BadRequestError(`Ya existe un centro de costo con clave "${clave}"`);

  return CentroCosto.create({ clave, sucursal, serieFacturacion: serieFacturacion ?? null, isActive: true });
}

async function update(id, data) {
  const cc = await getById(id);
  const { clave, sucursal, serieFacturacion, isActive } = data;

  if (clave && clave !== cc.clave) {
    const existe = await CentroCosto.findOne({ where: { clave } });
    if (existe) throw new BadRequestError(`Ya existe un centro de costo con clave "${clave}"`);
  }

  const fields = {};
  if (clave            != null) fields.clave             = clave;
  if (sucursal         != null) fields.sucursal           = sucursal;
  if (serieFacturacion != null) fields.serieFacturacion   = serieFacturacion;
  if (isActive         != null) fields.isActive           = isActive;

  await cc.update(fields);
  return cc;
}

async function softDelete(id) {
  const cc = await getById(id);
  await cc.update({ isActive: false });
  return { ok: true, id };
}

/**
 * Devuelve un mapa serieFacturacion → { id, clave } para todos los centros activos
 * que tengan serieFacturacion definida.
 * Usado por el generador de pólizas para asignar centroCostoId automáticamente
 * a partir de la serie del CFDI.
 */
async function resolveBySerieMap() {
  const rows = await CentroCosto.findAll({
    where:      { isActive: true, serieFacturacion: { [Op.ne]: null } },
    attributes: ['id', 'clave', 'sucursal', 'serieFacturacion'],
    raw:        true,
  });
  // Un mismo serie puede apuntar a un solo centro; si hay duplicados gana el primero.
  const map = {};
  for (const r of rows) {
    if (r.serieFacturacion && !map[r.serieFacturacion]) {
      map[r.serieFacturacion] = { id: r.id, clave: r.clave, sucursal: r.sucursal };
    }
  }
  return map;
}

module.exports = { list, getById, create, update, softDelete, resolveBySerieMap };
