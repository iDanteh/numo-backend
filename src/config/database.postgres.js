'use strict';

/**
 * config/database.postgres.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Instancia Sequelize para PostgreSQL.
 *
 * Exporta `sequelize` para que los modelos lo importen directamente,
 * y `connectPostgres` / `disconnectPostgres` para el bootstrap de la app.
 *
 * En desarrollo usa `alter: true` para sincronizar el schema sin destruir datos.
 * En producción usa migraciones Sequelize (este archivo NUNCA hace force: true).
 */

const { Sequelize } = require('sequelize');
const config         = require('./env');
const { logger }     = require('../shared/utils/logger');

const sequelize = new Sequelize(config.postgres.uri, {
  dialect: 'postgres',
  logging: (msg) => logger.debug(`[PG] ${msg}`),
  pool: {
    max:     10,
    min:     0,
    acquire: 30_000,
    idle:    10_000,
  },
  define: {
    timestamps:     true,   // createdAt / updatedAt gestionados por Sequelize
    underscored:    true,   // camelCase en JS → snake_case en BD
    freezeTableName: false,
  },
});

/**
 * Inicializa la conexión y sincroniza los modelos.
 * Los modelos se importan desde shared/models/postgres/index.js
 * DESPUÉS de que sequelize ya esté exportado (evita dependencia circular).
 */
const connectPostgres = async () => {
  await sequelize.authenticate();
  logger.info('PostgreSQL conectado');

  // pgcrypto: usado por Configuraciones Globales (shared/services/global-config.service.js)
  // para cifrar/descifrar secretos (pgp_sym_encrypt/pgp_sym_decrypt). Debe existir ANTES de
  // que syncModels() cree global_configs (columna valor_cifrado es bytea, no depende de la
  // extensión para el tipo en sí, pero las queries de lectura/escritura sí la necesitan).
  await sequelize.query('CREATE EXTENSION IF NOT EXISTS pgcrypto');

  // Importación deferida: los modelos ya importan `sequelize` desde este módulo
  const { syncModels } = require('../shared/models/postgres');
  await syncModels();
};

const disconnectPostgres = async () => {
  await sequelize.close();
  logger.info('PostgreSQL desconectado correctamente');
};

module.exports = { sequelize, connectPostgres, disconnectPostgres };
