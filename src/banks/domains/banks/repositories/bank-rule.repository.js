'use strict';

/**
 * banks/repositories/bank-rule.repository.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Acceso a datos de reglas de categorización y bloqueo en PostgreSQL.
 * Reemplaza el acceso directo a BankRule (Mongoose).
 */

const { Op }        = require('sequelize');
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

/**
 * Nombres únicos de reglas 'categorizar' — el catálogo de categorías definidas,
 * exista o no todavía un movimiento con esa categoría asignada.
 * @param {string[]} [bancos] — opcional; si se omite, trae de todos los bancos.
 */
async function listNombresCategorizar(bancos) {
  const where = { accion: 'categorizar' };
  if (bancos?.length) where.banco = { [Op.in]: bancos };
  const rules = await BankRule.findAll({ where, attributes: ['nombre'] });
  return [...new Set(rules.map(r => r.nombre))];
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
    estadoDestino:  data.estadoDestino  || null,
    ocultarRoles:   Array.isArray(data.ocultarRoles) ? data.ocultarRoles : [],
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
    estadoDestino:  data.estadoDestino !== undefined
      ? (data.estadoDestino || null)
      : rule.estadoDestino,
    ocultarRoles:   Array.isArray(data.ocultarRoles) ? data.ocultarRoles : rule.ocultarRoles,
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

module.exports = { listByBanco, findBlockingRules, findById, listNombresCategorizar, create, update, remove, reorder };
