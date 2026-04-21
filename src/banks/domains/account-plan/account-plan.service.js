'use strict';

/**
 * account-plan/account-plan.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Lógica de negocio del catálogo de cuentas contables.
 * Delegada a PostgreSQL mediante account-plan.repository.js.
 */

const repo                     = require('./repositories/account-plan.repository');
const { parseAccountPlanFile } = require('./account-plan.parser');
const { NotFoundError }        = require('../../shared/errors/AppError');

const ALLOWED_UPDATE_FIELDS = ['nombre', 'ctaMayor', 'parentId', 'isActive'];

async function list(filters) {
  return repo.findAll(filters);
}

async function tree() {
  return repo.findTree();
}

async function search(q, tipo) {
  return repo.search(q, tipo);
}

async function getById(id) {
  const account = await repo.findById(id);
  if (!account) throw new NotFoundError('Cuenta');
  return account;
}

async function create(data) {
  const { codigo, nombre, ctaMayor } = data;

  // Resolver parentId desde ctaMayor
  let parentId = null;
  if (ctaMayor) {
    const parent = await repo.findByCodigo(ctaMayor);
    if (parent) parentId = parent.id;
  }

  return repo.create({ codigo, nombre, ctaMayor: ctaMayor || null, parentId });
}

async function update(id, data) {
  const updateData = {};
  for (const key of ALLOWED_UPDATE_FIELDS) {
    if (data[key] !== undefined) updateData[key] = data[key];
  }

  // Si cambió ctaMayor, recalcular parentId
  if (updateData.ctaMayor !== undefined) {
    if (updateData.ctaMayor) {
      const parent = await repo.findByCodigo(updateData.ctaMayor);
      updateData.parentId = parent ? parent.id : null;
    } else {
      updateData.parentId = null;
    }
  }

  const account = await repo.update(id, updateData);
  if (!account) throw new NotFoundError('Cuenta');
  return account;
}

async function softDelete(id) {
  const result = await repo.softDelete(id);
  if (!result) throw new NotFoundError('Cuenta');
  return { message: 'Cuenta desactivada', id: result.id };
}

async function importFile(buffer, opts = {}) {
  return parseAccountPlanFile(buffer, opts);
}

module.exports = { list, tree, search, getById, create, update, softDelete, importFile };
