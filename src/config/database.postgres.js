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
const { Client }     = require('pg');
const config         = require('./env');
const { logger }     = require('../shared/utils/logger');

// Clave fija arbitraria para el advisory lock de syncModels() — cualquier entero de 64 bits
// sirve, con tal de que sea único dentro de la app (no colisiona con ningún otro
// pg_advisory_lock que use este mismo Postgres). No tiene relación con ningún dato real.
const SYNC_MODELS_LOCK_KEY = 847234651;

/**
 * Serializa `syncModels()` entre los N workers del cluster con un advisory lock de sesión
 * (2026-11-26, bug real encontrado en el primer despliegue con cluster en producción —
 * ver stack trace guardado en memoria/arquitectura: `Poliza.sync()` → `showIndex` tiró
 * `TypeError: Cannot read properties of null (reading '1')` porque 2+ workers preguntaban
 * al catálogo de Postgres al mismo tiempo mientras otro lo alteraba).
 *
 * `connectDB()`/`connectPostgres()` corren en TODOS los workers (cada uno necesita su propia
 * conexión real, ver comentario en app.js) — pero `syncModels()` (los ~800 ALTER/CREATE TABLE
 * de shared/models/postgres/index.js) nunca fue pensado para correr N veces en paralelo contra
 * el mismo Postgres. Mismo patrón que ya usa `reportes-por-facturar.service.js` para el mismo
 * tipo de problema (candado real cross-proceso, no solo in-memory), pero acá con
 * `pg_advisory_lock` de SESIÓN (no `_xact_lock`): `syncModels()` no es una sola transacción,
 * son cientos de statements independientes — envolverla entera en una transacción obligaría a
 * reescribir cada llamada para pasarle `{transaction}`, mucho más invasivo que esto.
 *
 * Requiere una conexión `pg` DEDICADA (no el pool de Sequelize): un lock de sesión se libera
 * con `pg_advisory_unlock` en la MISMA conexión que lo tomó — si acquire/release salieran por
 * conexiones distintas del pool de Sequelize, Postgres respondería "you don't own this lock".
 * El primer worker que llega hace la migración real; los demás quedan bloqueados en
 * `pg_advisory_lock` hasta que termina, y al entrar después no hacen nada (todo el archivo es
 * idempotente — `IF NOT EXISTS`/`force:false`), sin volver a correr la carrera.
 */
async function _conSyncLock(fn) {
  const client = new Client({ connectionString: config.postgres.uri });
  await client.connect();
  try {
    await client.query('SELECT pg_advisory_lock($1)', [SYNC_MODELS_LOCK_KEY]);
    await fn();
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [SYNC_MODELS_LOCK_KEY]).catch((e) => {
      logger.warn(`[syncModels] No se pudo liberar el advisory lock explícitamente (se libera solo al cerrar la conexión igual): ${e.message}`);
    });
    await client.end();
  }
}

const sequelize = new Sequelize(config.postgres.uri, {
  dialect: 'postgres',
  logging: (msg) => logger.debug(`[PG] ${msg}`),
  pool: {
    // POSTGRES_POOL_MAX (2026-11-10) — override SOLO para medir en stress-test (ver
    // stress-test/docker-compose.stress.yml): sin la variable, el default sigue siendo 10, cero
    // cambio de comportamiento en producción. Hallazgo real: con 10 fijo, un pico de tráfico en
    // CUALQUIER feature satura el pool compartido por TODA la app, no solo quien generó el pico.
    max:     Number(process.env.POSTGRES_POOL_MAX) || 10,
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
  await _conSyncLock(syncModels);
};

const disconnectPostgres = async () => {
  await sequelize.close();
  logger.info('PostgreSQL desconectado correctamente');
};

module.exports = {
  sequelize, connectPostgres, disconnectPostgres,
  // Exportado solo para el test dedicado (database.postgres.conSyncLock.test.js) — nunca se
  // usa fuera de este archivo en código real.
  _conSyncLock,
};
