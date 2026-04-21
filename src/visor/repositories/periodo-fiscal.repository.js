'use strict';

/**
 * visor/repositories/periodo-fiscal.repository.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Acceso a datos de períodos fiscales en PostgreSQL.
 */

const { PeriodoFiscal, User } = require('../../shared/models/postgres');

async function findAll() {
  return PeriodoFiscal.findAll({
    include: [{
      model:      User,
      as:         'creator',
      attributes: ['nombre', 'email'],
    }],
    order: [['ejercicio', 'DESC'], ['periodo', 'ASC']],
  });
}

async function findById(id) {
  return PeriodoFiscal.findByPk(id);
}

async function findByEjercicioPeriodo(ejercicio, periodo) {
  return PeriodoFiscal.findOne({ where: { ejercicio, periodo: periodo ?? null } });
}

async function create(data) {
  return PeriodoFiscal.create(data);
}

async function remove(id) {
  const doc = await PeriodoFiscal.findByPk(id);
  if (!doc) return null;
  await doc.destroy();
  return doc;
}

module.exports = { findAll, findById, findByEjercicioPeriodo, create, remove };
