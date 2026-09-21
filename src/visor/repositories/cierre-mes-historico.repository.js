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
    include: [
      { model: User, as: 'cerradoPor',   attributes: ['nombre', 'email'] },
      { model: User, as: 'revertidoPor', attributes: ['nombre', 'email'] },
    ],
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

/**
 * Marca como revertido el cierre vigente de un período: el más reciente de
 * ese `periodoFiscalId` que todavía no tenía `revertidoPorId` (un período
 * puede cerrarse/revertirse varias veces, y cada cierre queda como su propia
 * fila — nunca se toca un cierre ya marcado como revertido antes).
 */
async function marcarRevertido(periodoFiscalId, userId) {
  const ultimo = await CierreMesHistorico.findOne({
    where: { periodoFiscalId, revertidoPorId: null },
    order: [['createdAt', 'DESC']],
  });
  if (!ultimo) return null;
  ultimo.revertidoPorId = userId ?? null;
  ultimo.revertidoEn    = new Date();
  await ultimo.save();
  return ultimo;
}

module.exports = { findAll, findById, create, marcarRevertido };
