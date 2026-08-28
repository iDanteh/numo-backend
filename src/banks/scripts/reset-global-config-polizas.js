'use strict';

/**
 * banks/scripts/reset-global-config-polizas.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Borra por completo la sección `polizas` de Configuraciones Globales que
 * administra seed-global-config-polizas.js — para arrancar de cero antes de
 * volver a sembrar.
 *
 * Borra SOLO esta clave — nunca un TRUNCATE general de la tabla, mismo
 * criterio que reset-global-config-banks.js. El borrado de `ConfigSection`
 * cascadea (FK `onDelete: CASCADE`) a sus `GlobalConfig` y de ahí a sus
 * `ConfigAuditLog` — no hace falta borrarlos aparte.
 *
 * Por defecto corre en modo DRY-RUN. Para borrar de verdad hay que pasar
 * --confirm explícito.
 *
 * Uso:
 *   node src/banks/scripts/reset-global-config-polizas.js            (dry-run)
 *   node src/banks/scripts/reset-global-config-polizas.js --confirm  (borra de verdad)
 *   npm run reset:polizas              (dry-run)
 *   npm run reset:polizas -- --confirm (borra de verdad)
 *
 * Después de correrlo con --confirm, volvé a sembrar:
 *   npm run seed:polizas
 */

require('dotenv').config();

const { ConfigSection } = require('../../shared/models/postgres');

const CLAVES = ['polizas'];

async function resetGlobalConfigPolizas({ confirm = false } = {}) {
  const secciones = await ConfigSection.findAll({ where: { clave: CLAVES } });

  if (secciones.length === 0) {
    console.log('[reset-polizas] No hay sección sembrada todavía — nada que borrar.');
    return;
  }

  console.log(`[reset-polizas] ${confirm ? 'Borrando' : 'Se borrarían'} ${secciones.length} sección(es):`);
  for (const s of secciones) {
    console.log(`  - ${s.clave} (id=${s.id}) "${s.nombre}"`);
  }

  if (!confirm) {
    console.log('\n[reset-polizas] Dry-run — no se borró nada. Volvé a correr con --confirm para borrar de verdad.');
    return;
  }

  await ConfigSection.destroy({ where: { clave: CLAVES } });
  console.log(`[reset-polizas] Listo — ${secciones.length} sección(es) borrada(s) (cascada a sus valores/historial). Corré 'npm run seed:polizas' para sembrar de nuevo.`);
}

// ── Ejecución directa: node src/banks/scripts/reset-global-config-polizas.js ─
if (require.main === module) {
  const confirm = process.argv.includes('--confirm');
  const { connectPostgres, disconnectPostgres } = require('../../config/database.postgres');

  connectPostgres()
    .then(async () => {
      await resetGlobalConfigPolizas({ confirm });
      await disconnectPostgres();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[reset-polizas] Error:', err.message);
      process.exit(1);
    });
}

module.exports = resetGlobalConfigPolizas;
