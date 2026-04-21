'use strict';

/**
 * config/database.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Punto único para gestionar AMBAS conexiones de base de datos:
 *   • MongoDB  → documentos flexibles (movimientos bancarios, CFDIs, SAT, logs)
 *   • PostgreSQL → datos relacionales y estructurados (usuarios, catálogos, entidades)
 *
 * Ambas conexiones se inician en paralelo al arrancar el servidor.
 */

const { connectMongo,    disconnectMongo    } = require('./database.mongo');
const { connectPostgres, disconnectPostgres } = require('./database.postgres');

/**
 * Abre ambas conexiones concurrentemente.
 * Lanza excepción si cualquiera de las dos falla.
 */
const connectDB = async () => {
  await Promise.all([
    connectMongo(),
    connectPostgres(),
  ]);
};

/**
 * Cierra ambas conexiones concurrentemente.
 */
const disconnectDB = async () => {
  await Promise.all([
    disconnectMongo(),
    disconnectPostgres(),
  ]);
};

module.exports = { connectDB, disconnectDB };
