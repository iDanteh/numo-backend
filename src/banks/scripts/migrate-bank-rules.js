'use strict';

/**
 * banks/scripts/migrate-bank-rules.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Migración única: transfiere las reglas de bank_rules (MongoDB) a
 * la tabla bank_rules (PostgreSQL).
 *
 * Es seguro ejecutarlo varias veces (idempotente por nombre+banco).
 * Si una regla ya existe en PG con el mismo banco+nombre, se omite.
 *
 * Uso:
 *   node src/banks/scripts/migrate-bank-rules.js
 *
 * Variables de entorno requeridas: MONGODB_URI, POSTGRES_URI
 */

require('dotenv').config();

const mongoose  = require('mongoose');
const { BankRule: BankRulePG } = require('../../shared/models/postgres');
const { connectPostgres }      = require('../../config/database.postgres');
const { logger }               = require('../../shared/utils/logger');

// Schema mínimo para leer de Mongo sin importar el modelo deprecado
const legacySchema = new mongoose.Schema({
  banco:       String,
  nombre:      String,
  condiciones: Array,
  logica:      String,
  orden:       Number,
}, { collection: 'bank_rules', strict: false });

const LegacyBankRule = mongoose.model('LegacyBankRule', legacySchema);

async function run() {
  // 1. Conectar ambas bases
  await mongoose.connect(process.env.MONGODB_URI);
  logger.info('MongoDB conectado');

  await connectPostgres();
  logger.info('PostgreSQL conectado');

  // 2. Leer reglas de MongoDB
  const mongoRules = await LegacyBankRule.find({}).sort({ banco: 1, orden: 1 }).lean();
  logger.info(`${mongoRules.length} reglas encontradas en MongoDB`);

  let migradas  = 0;
  let omitidas  = 0;

  for (const r of mongoRules) {
    const existing = await BankRulePG.findOne({
      where: { banco: r.banco, nombre: r.nombre },
    });

    if (existing) {
      omitidas++;
      continue;
    }

    await BankRulePG.create({
      banco:          r.banco,
      nombre:         r.nombre,
      condiciones:    (r.condiciones || []).map(c => ({
        campo:    c.campo,
        operador: c.operador,
        valor:    String(c.valor ?? ''),
      })),
      logica:         r.logica         || 'Y',
      accion:         'categorizar',   // todas las reglas existentes son de categorización
      mensajeBloqueo: null,
      orden:          r.orden          ?? 0,
    });

    migradas++;
  }

  logger.info(`Migración completada: ${migradas} migradas, ${omitidas} ya existían`);

  await mongoose.disconnect();
  process.exit(0);
}

run().catch((err) => {
  logger.error('Error en migración de bank_rules:', err);
  process.exit(1);
});
