'use strict';

/**
 * account-plan/repositories/account-plan.repository.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Acceso a datos del catálogo de cuentas en PostgreSQL.
 * Reemplaza el acceso directo a Mongoose con llamadas Sequelize equivalentes.
 */

const { Op }         = require('sequelize');
const { AccountPlan } = require('../../../../shared/models/postgres');

async function findAll(filters = {}) {
  const where = {};
  if (!filters.includeInactive) where.isActive = true;
  if (filters.tipo)        where.tipo      = filters.tipo.toUpperCase();
  if (filters.naturaleza)  where.naturaleza = filters.naturaleza.toUpperCase();
  if (filters.search) {
    where[Op.or] = [
      { codigo: { [Op.iLike]: `%${filters.search}%` } },
      { nombre: { [Op.iLike]: `%${filters.search}%` } },
    ];
  }
  return AccountPlan.findAll({ where, order: [['codigo', 'ASC']] });
}

async function findTree() {
  return AccountPlan.findAll({
    where: { isActive: true },
    order: [['nivel', 'ASC'], ['codigo', 'ASC']],
  });
}

async function search(q, tipo) {
  if (!q) return [];
  const where = {
    isActive: true,
    [Op.or]: [
      { codigo: { [Op.iLike]: `%${q}%` } },
      { nombre: { [Op.iLike]: `%${q}%` } },
    ],
  };
  if (tipo) where.tipo = tipo.toUpperCase();
  return AccountPlan.findAll({
    where,
    order: [['nivel', 'ASC'], ['codigo', 'ASC']],
    limit: 25,
  });
}

async function findById(id) {
  return AccountPlan.findByPk(id, {
    include: [{ model: AccountPlan, as: 'parent', attributes: ['codigo', 'nombre'] }],
  });
}

async function findByCodigo(codigo) {
  return AccountPlan.findOne({ where: { codigo } });
}

async function create(data) {
  return AccountPlan.create(data);
}

async function update(id, data) {
  const [count] = await AccountPlan.update(data, { where: { id } });
  if (!count) return null;
  return AccountPlan.findByPk(id);
}

async function softDelete(id) {
  const [count] = await AccountPlan.update({ isActive: false }, { where: { id } });
  return count > 0 ? { id } : null;
}

/**
 * Upsert por código (importación masiva desde Excel).
 * Devuelve { isNew, record }.
 */
async function upsertByCodigo(data) {
  const existing = await AccountPlan.findOne({ where: { codigo: data.codigo } });
  if (existing) {
    await existing.update(data);
    return { isNew: false, record: existing };
  }
  const record = await AccountPlan.create(data);
  return { isNew: true, record };
}

/**
 * Actualiza solo el parentId de un registro identificado por su código.
 */
async function updateParentId(codigo, parentId) {
  return AccountPlan.update({ parentId }, { where: { codigo } });
}

module.exports = {
  findAll,
  findTree,
  search,
  findById,
  findByCodigo,
  create,
  update,
  softDelete,
  upsertByCodigo,
  updateParentId,
};
