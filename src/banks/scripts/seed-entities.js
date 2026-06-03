'use strict';

/**
 * banks/scripts/seed-entities.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Siembra las entidades fiscales conocidas en PostgreSQL.
 *
 * - Seguro de ejecutar múltiples veces (idempotente).
 * - Usa upsert con conflicto en 'rfc' → solo actualiza campos no sensibles.
 * - El campo `fiel` NO se toca aquí; se registra vía la UI de Cierre de Día.
 *
 * Uso:
 *   node src/banks/scripts/seed-entities.js
 */

require('dotenv').config();

const { Entity } = require('../../shared/models/postgres');

const ENTITIES = [
  {
    rfc:             'CCO011113663',
    nombre:          'CAR COMERCIALIZADORA',
    tipo:            'moral',
    isOwn:           false,
    isActive:        true,
    esIntercompania: false,
    regimenFiscal:   null,
    domicilioFiscal: {},
    syncConfig: {
      autoSync:      true,
      syncFrequency: 'daily',
      lastSync:      null,
      nextSync:      null,
      syncEmitidos:  true,
      syncRecibidos: true,
    },
  },
];

async function seedEntities() {
  let creados = 0;
  let omitidos = 0;

  for (const data of ENTITIES) {
    const [, created] = await Entity.upsert(data, { conflictFields: ['rfc'] });
    if (created) {
      console.log(`[seed-entities] Creada → ${data.rfc} (${data.nombre})`);
      creados++;
    } else {
      console.log(`[seed-entities] Ya existe → ${data.rfc} (${data.nombre}). Sin cambios sensibles.`);
      omitidos++;
    }
  }

  console.log(`[seed-entities] Listo. Creadas: ${creados}, ya existían: ${omitidos}.`);
}

// ── Ejecución directa: node src/banks/scripts/seed-entities.js ───────────────
if (require.main === module) {
  const { connectPostgres, disconnectPostgres } = require('../../config/database.postgres');

  connectPostgres()
    .then(async () => {
      await seedEntities();
      await disconnectPostgres();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[seed-entities] Error:', err.message);
      process.exit(1);
    });
}

module.exports = seedEntities;
