'use strict';

// db-health.js — chequeo de salud de Mongo/Postgres, EXTRAÍDO (2026-09-24) porque
// app.js (GET /health) y system-monitor.service.js (getSnapshot) reimplementaban la
// misma lógica de forma independiente — con dos copias, un cambio de criterio en una
// (ej. otro readyState válido, otro método de ping) podía divergir en silencio de
// la otra sin que nada lo detectara.

const mongoose = require('mongoose');
const { sequelize } = require('../../config/database.postgres');

/** true si Mongoose tiene la conexión activa (readyState 1 = connected). */
function checkMongoOk() {
  return mongoose.connection.readyState === 1;
}

/** true si Postgres responde a un ping real (sequelize.authenticate()). */
async function checkPostgresOk() {
  try {
    await sequelize.authenticate();
    return true;
  } catch {
    return false;
  }
}

module.exports = { checkMongoOk, checkPostgresOk };
