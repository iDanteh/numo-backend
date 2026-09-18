'use strict';

/**
 * visor/repositories/cierre-mes-historico.repository.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Acceso a datos del historial de reportes de Cierre de Mes.
 */

const { CierreMesHistorico, User } = require('../../shared/models/postgres');

/** Lista todos los cierres, sin el binario del archivo (solo metadata). */
async function findAll() {
  return CierreMesHistorico.findAll({
    attributes: { exclude: ['fileData'] },
    include: [{
      model:      User,
      as:         'cerradoPor',
      attributes: ['nombre', 'email'],
    }],
    order: [['createdAt', 'DESC']],
  });
}

/** Trae un cierre completo (incluye el binario) para redescargarlo. */
async function findById(id) {
  return CierreMesHistorico.findByPk(id);
}

async function create(data) {
  return CierreMesHistorico.create(data);
}

module.exports = { findAll, findById, create };
