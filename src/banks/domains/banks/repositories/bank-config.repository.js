'use strict';

/**
 * banks/repositories/bank-config.repository.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Acceso a datos de configuración de bancos en PostgreSQL.
 * Un registro por banco — el upsert garantiza idempotencia.
 */

const { BankConfig } = require('../../../../shared/models/postgres');

/**
 * Devuelve la configuración de un banco o null si no existe.
 */
async function findByBanco(banco) {
  return BankConfig.findOne({ where: { banco } });
}

/**
 * Inserta o actualiza la configuración del banco.
 * Usa upsert sobre la columna única `banco`.
 * @returns {BankConfig}
 */
async function upsert(banco, fields) {
  const [record] = await BankConfig.upsert(
    { banco, ...fields },
    { returning: true },
  );
  return record;
}

/**
 * Devuelve todas las configuraciones indexadas por nombre de banco.
 * Usado en bank.service.getCards() para hacer el join en aplicación.
 * @returns {Map<string, BankConfig>}
 */
async function findAllAsMap() {
  const configs = await BankConfig.findAll();
  return new Map(configs.map((c) => [c.banco, c]));
}

module.exports = { findByBanco, upsert, findAllAsMap };
