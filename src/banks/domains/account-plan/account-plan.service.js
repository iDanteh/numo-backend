'use strict';

const AccountPlan              = require('./AccountPlan.model');
const { parseAccountPlanFile } = require('./account-plan.parser');
const { NotFoundError }        = require('../../shared/errors/AppError');

const ALLOWED_UPDATE_FIELDS = ['nombre', 'ctaMayor', 'parentId', 'isActive'];

async function list(filters) {
  const { tipo, naturaleza, search, includeInactive } = filters;
  const filter = {};
  if (!includeInactive) filter.isActive   = true;
  if (tipo)             filter.tipo       = tipo.toUpperCase();
  if (naturaleza)       filter.naturaleza = naturaleza.toUpperCase();
  if (search)           filter.$text      = { $search: search };
  return AccountPlan.find(filter).sort({ codigo: 1 }).lean();
}

async function tree() {
  return AccountPlan.find({ isActive: true }).sort({ nivel: 1, codigo: 1 }).lean();
}

async function search(q, tipo) {
  if (!q || q.length < 1) return [];
  const filter = {
    isActive: true,
    $or: [
      { codigo: new RegExp(q, 'i') },
      { nombre: new RegExp(q, 'i') },
    ],
  };
  if (tipo) filter.tipo = tipo.toUpperCase();
  return AccountPlan.find(filter).sort({ nivel: 1, codigo: 1 }).limit(25).lean();
}

async function getById(id) {
  const account = await AccountPlan.findById(id).populate('parentId', 'codigo nombre').lean();
  if (!account) throw new NotFoundError('Cuenta');
  return account;
}

async function create(data) {
  const { codigo, nombre, ctaMayor } = data;

  // Resolver parentId desde ctaMayor
  let parentId = null;
  if (ctaMayor) {
    const parent = await AccountPlan.findOne({ codigo: ctaMayor }).select('_id').lean();
    if (parent) parentId = parent._id;
  }

  const account = new AccountPlan({ codigo, nombre, ctaMayor: ctaMayor || null, parentId });
  await account.save();
  return account;
}

async function update(id, data) {
  const updateData = {};
  for (const key of ALLOWED_UPDATE_FIELDS) {
    if (data[key] !== undefined) updateData[key] = data[key];
  }

  // Si cambió ctaMayor, recalcular parentId
  if (updateData.ctaMayor !== undefined) {
    if (updateData.ctaMayor) {
      const parent = await AccountPlan.findOne({ codigo: updateData.ctaMayor }).select('_id').lean();
      updateData.parentId = parent ? parent._id : null;
    } else {
      updateData.parentId = null;
    }
  }

  const account = await AccountPlan.findByIdAndUpdate(
    id, { $set: updateData }, { new: true, runValidators: true },
  );
  if (!account) throw new NotFoundError('Cuenta');
  return account;
}

async function softDelete(id) {
  const account = await AccountPlan.findByIdAndUpdate(id, { isActive: false }, { new: true });
  if (!account) throw new NotFoundError('Cuenta');
  return { message: 'Cuenta desactivada', id: account._id };
}

async function importFile(buffer, opts = {}) {
  return parseAccountPlanFile(buffer, opts);
}

module.exports = { list, tree, search, getById, create, update, softDelete, importFile };
