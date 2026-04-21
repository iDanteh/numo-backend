'use strict';

/**
 * config/database.mongo.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Conexión a MongoDB mediante Mongoose.
 * Extraído de database.js para soportar la arquitectura mixta Mongo + Postgres.
 */

const mongoose  = require('mongoose');
const config    = require('./env');
const { logger } = require('../shared/utils/logger');

const connectMongo = async () => {
  const uri = config.db.uri;

  mongoose.connection.on('connected',    () => logger.info('MongoDB conectado'));
  mongoose.connection.on('error',   (err) => logger.error('MongoDB error:', err));
  mongoose.connection.on('disconnected', () => logger.warn('MongoDB desconectado'));

  await mongoose.connect(uri, {
    maxPoolSize:              10,
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS:          45000,
  });
};

const disconnectMongo = async () => {
  await mongoose.connection.close();
  logger.info('MongoDB desconectado correctamente');
};

module.exports = { connectMongo, disconnectMongo };
