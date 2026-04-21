'use strict';

/**
 * visor/repositories/entity.repository.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Acceso a datos de entidades fiscales en PostgreSQL.
 *
 * Las consultas sobre campos JSONB (fiel, syncConfig) usan el operador
 * `Op.contains` de Sequelize, que se traduce a `@>` en Postgres.
 */

const { Op }    = require('sequelize');
const { Entity } = require('../../shared/models/postgres');

async function findAll(where = {}) {
  return Entity.findAll({
    where,
    attributes: { exclude: ['fiel'] },   // nunca exponer credenciales e.firma
    order: [['nombre', 'ASC']],
  });
}

async function findById(id) {
  return Entity.findByPk(id);
}

async function findByRfc(rfc) {
  return Entity.findOne({ where: { rfc: rfc?.toUpperCase() } });
}

async function create(data) {
  return Entity.create(data);
}

async function update(id, data) {
  const [count] = await Entity.update(data, { where: { id } });
  if (!count) return null;
  return Entity.findByPk(id);
}

/**
 * Devuelve entidades activas con descarga nocturna habilitada.
 * Equivalente al Mongoose: Entity.find({ isActive: true, 'syncConfig.autoSync': true })
 */
async function findWithAutoSync() {
  return Entity.findAll({
    where: {
      isActive:   true,
      syncConfig: { [Op.contains]: { autoSync: true } },
    },
    attributes: ['id', 'rfc', 'nombre', 'syncConfig'],
  });
}

module.exports = { findAll, findById, findByRfc, create, update, findWithAutoSync };
