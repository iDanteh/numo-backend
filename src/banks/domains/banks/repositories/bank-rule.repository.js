'use strict';

/**
 * banks/repositories/bank-rule.repository.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Acceso a datos de reglas de categorización y bloqueo en PostgreSQL.
 * Reemplaza el acceso directo a BankRule (Mongoose).
 */

const { BankRule }  = require('../../../../shared/models/postgres');
const { sequelize } = require('../../../../config/database.postgres');

/**
 * Lista todas las reglas de un banco ordenadas por prioridad.
 * @param {string} banco
 * @param {{ accion?: string }} [opts] — filtro opcional por acción
 */
async function listByBanco(banco, { accion } = {}) {
  const where = { banco };
  if (accion) where.accion = accion;
  return BankRule.findAll({
    where,
    order: [['orden', 'ASC'], ['createdAt', 'ASC']],
  });
}

/**
 * Devuelve solo las reglas de bloqueo de identificación para un banco.
 * Usada en bank.service.updateStatus().
 */
async function findBlockingRules(banco) {
  return BankRule.findAll({
    where:  { banco, accion: 'bloquear_identificacion' },
    order:  [['orden', 'ASC']],
  });
}

async function findById(id) {
  return BankRule.findByPk(id);
}

async function create(banco, data) {
  return BankRule.create({
    banco,
    nombre:         String(data.nombre).trim(),
    condiciones:    data.condiciones.map(c => ({
      campo:    c.campo,
      operador: c.operador,
      valor:    String(c.valor).trim(),
    })),
    logica:         data.logica         || 'Y',
    accion:         data.accion         || 'categorizar',
    mensajeBloqueo: data.mensajeBloqueo ? String(data.mensajeBloqueo).trim() : null,
    orden:          Number(data.orden)  || 0,
  });
}

async function update(id, data) {
  const rule = await BankRule.findByPk(id);
  if (!rule) return null;
  await rule.update({
    nombre:         String(data.nombre).trim(),
    condiciones:    data.condiciones.map(c => ({
      campo:    c.campo,
      operador: c.operador,
      valor:    String(c.valor).trim(),
    })),
    logica:         data.logica         ?? rule.logica,
    accion:         data.accion         ?? rule.accion,
    mensajeBloqueo: data.mensajeBloqueo !== undefined
      ? (data.mensajeBloqueo ? String(data.mensajeBloqueo).trim() : null)
      : rule.mensajeBloqueo,
    ...(data.orden !== undefined && { orden: Number(data.orden) }),
  });
  return rule;
}

async function remove(id) {
  const rule = await BankRule.findByPk(id);
  if (!rule) return null;
  await rule.destroy();
  return { deleted: true };
}

/**
 * Reordena reglas en una transacción atómica.
 * @param {number[]} ids — array de IDs en el nuevo orden deseado
 */
async function reorder(ids) {
  const t = await sequelize.transaction();
  try {
    await Promise.all(
      ids.map((id, idx) => BankRule.update({ orden: idx }, { where: { id }, transaction: t })),
    );
    await t.commit();
    return { ok: true };
  } catch (err) {
    await t.rollback();
    throw err;
  }
}

module.exports = { listByBanco, findBlockingRules, findById, create, update, remove, reorder };
