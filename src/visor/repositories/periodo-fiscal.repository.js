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

/** Marca el período como cerrado. No valida estado previo — eso lo hace el controller. */
async function cerrar(id, userId) {
  const doc = await PeriodoFiscal.findByPk(id);
  if (!doc) return null;
  doc.cerrado       = true;
  doc.cerradoPorId  = userId ?? null;
  doc.cerradoEn     = new Date();
  await doc.save();
  return doc;
}

/** Revierte el cierre de un período. */
async function revertirCierre(id, userId) {
  const doc = await PeriodoFiscal.findByPk(id);
  if (!doc) return null;
  doc.cerrado         = false;
  doc.revertidoPorId  = userId ?? null;
  doc.revertidoEn     = new Date();
  await doc.save();
  return doc;
}

module.exports = { findAll, findById, findByEjercicioPeriodo, create, remove, cerrar, revertirCierre };
