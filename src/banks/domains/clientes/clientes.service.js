'use strict';

const { ClienteCatalogo } = require('../../../shared/models/postgres');
const { Op }              = require('sequelize');
const { NotFoundError, BadRequestError } = require('../../shared/errors/AppError');

async function list({ search, includeInactive, tipo } = {}) {
  const where = {};
  if (!includeInactive) where.isActive = true;
  if (tipo)   where.tipo = tipo;
  if (search) {
    where[Op.or] = [
      { cuenta: { [Op.iLike]: `%${search}%` } },
      { nombre: { [Op.iLike]: `%${search}%` } },
      { rfc:    { [Op.iLike]: `%${search}%` } },
    ];
  }
  return ClienteCatalogo.findAll({ where, order: [['nombre', 'ASC']] });
}

async function getById(id) {
  const c = await ClienteCatalogo.findByPk(id);
  if (!c) throw new NotFoundError(`Cliente #${id} no encontrado`);
  return c;
}

async function create(data) {
  const { cuenta, nombre, tipo, rfc } = data;
  if (!cuenta) throw new BadRequestError('cuenta es requerida');
  if (!nombre) throw new BadRequestError('nombre es requerido');
  if (!rfc)    throw new BadRequestError('rfc es requerido');

  const existe = await ClienteCatalogo.findOne({ where: { rfc: rfc.toUpperCase() } });
  if (existe)  throw new BadRequestError(`Ya existe un cliente con RFC "${rfc.toUpperCase()}"`);

  return ClienteCatalogo.create({
    cuenta,
    nombre,
    tipo:     tipo ?? 'CLIENTE',
    rfc:      rfc.toUpperCase(),
    isActive: true,
  });
}

async function update(id, data) {
  const c = await getById(id);
  const { cuenta, nombre, tipo, rfc, isActive } = data;

  if (rfc && rfc.toUpperCase() !== c.rfc) {
    const existe = await ClienteCatalogo.findOne({ where: { rfc: rfc.toUpperCase() } });
    if (existe) throw new BadRequestError(`Ya existe un cliente con RFC "${rfc.toUpperCase()}"`);
  }

  const fields = {};
  if (cuenta    != null) fields.cuenta    = cuenta;
  if (nombre    != null) fields.nombre    = nombre;
  if (tipo      != null) fields.tipo      = tipo;
  if (rfc       != null) fields.rfc       = rfc.toUpperCase();
  if (isActive  != null) fields.isActive  = isActive;

  await c.update(fields);
  return c;
}

async function softDelete(id) {
  const c = await getById(id);
  await c.update({ isActive: false });
  return { ok: true, id };
}

const VALID_TIPOS = ['CLIENTE', 'PROVEEDOR', 'CLIENTE-PROVEEDOR'];

async function importBulk(rows) {
  let inserted = 0, updated = 0;
  const errors = [];

  for (let i = 0; i < rows.length; i++) {
    const row    = rows[i];
    const rowNum = i + 2; // fila 1 = encabezado
    try {
      const cuenta  = String(row.cuenta  ?? '').trim();
      const nombre  = String(row.nombre  ?? '').trim();
      const rfc     = String(row.rfc     ?? '').trim().toUpperCase();
      const tipoRaw = String(row.tipo    ?? '').trim().toUpperCase();
      const tipo    = VALID_TIPOS.includes(tipoRaw) ? tipoRaw : 'CLIENTE';

      if (!cuenta) throw new Error('"cuenta" es requerida');
      if (!nombre) throw new Error('"nombre" es requerido');
      if (!rfc)    throw new Error('"rfc" es requerido');

      const existing = await ClienteCatalogo.findOne({ where: { rfc } });
      if (existing) {
        await existing.update({ cuenta, nombre, tipo, isActive: true });
        updated++;
      } else {
        await ClienteCatalogo.create({ cuenta, nombre, tipo, rfc, isActive: true });
        inserted++;
      }
    } catch (err) {
      errors.push({ fila: rowNum, rfc: String(row.rfc ?? '—'), error: err.message });
    }
  }

  return { inserted, updated, errors };
}

module.exports = { list, getById, create, update, softDelete, importBulk };
