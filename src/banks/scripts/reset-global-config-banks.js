'use strict';

/**
 * banks/scripts/reset-global-config-banks.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Borra por completo las secciones de Configuraciones Globales que administra
 * seed-global-config-banks.js — para arrancar de cero antes de volver a
 * sembrar, en vez de arrastrar filas/módulos afectados de corridas viejas.
 *
 * La lista de abajo incluye TANTO la estructura actual (3 secciones: bancos,
 * kore, solicitudes) COMO la estructura vieja de 5 secciones más finas
 * (erp-caja, kore-formaspago, kore-caja, kore-webhooks, erp-fact, consolidadas
 * 2026-08-25) — así un solo `--confirm` deja limpio cualquier ambiente sin
 * importar en qué punto de la migración esté: uno que ya sembró con la
 * estructura vieja (ej. local) y uno que todavía no sembró nada (test/prod).
 *
 * Borra SOLO estas claves — nunca un TRUNCATE general de la tabla, para no
 * arrastrarse por delante ninguna sección que no sea de este dominio (ej. si
 * en el futuro se migra otra parte del .env con su propia sección). El
 * borrado de `ConfigSection` cascadea (FK `onDelete: CASCADE`, ver
 * shared/models/postgres/index.js) a sus `GlobalConfig` y de ahí a sus
 * `ConfigAuditLog` — no hace falta borrarlos aparte.
 *
 * Por defecto corre en modo DRY-RUN (solo muestra qué borraría). Para borrar
 * de verdad hay que pasar --confirm explícito — mismo criterio de seguridad
 * que ya usan los demás scripts de un solo uso de esta carpeta.
 *
 * Uso:
 *   node src/banks/scripts/reset-global-config-banks.js            (dry-run)
 *   node src/banks/scripts/reset-global-config-banks.js --confirm  (borra de verdad)
 *   npm run reset:banks              (dry-run)
 *   npm run reset:banks -- --confirm (borra de verdad)
 *
 * Después de correrlo con --confirm, volvé a sembrar:
 *   npm run seed:banks
 */

require('dotenv').config();

const { ConfigSection } = require('../../shared/models/postgres');

const CLAVES = [
  // Estructura actual (2026-08-25 en adelante)
  'bancos', 'kore', 'solicitudes',
  // Estructura vieja (5 secciones, antes de consolidar) — se limpia igual por
  // si el ambiente todavía tiene datos sembrados con esos nombres.
  'erp-caja', 'kore-formaspago', 'kore-caja', 'kore-webhooks', 'erp-fact',
];

async function resetGlobalConfigBanks({ confirm = false } = {}) {
  const secciones = await ConfigSection.findAll({ where: { clave: CLAVES } });

  if (secciones.length === 0) {
    console.log('[reset-banks] No hay ninguna de las 5 secciones sembrada todavía — nada que borrar.');
    return;
  }

  console.log(`[reset-banks] ${confirm ? 'Borrando' : 'Se borrarían'} ${secciones.length} sección(es):`);
  for (const s of secciones) {
    console.log(`  - ${s.clave} (id=${s.id}) "${s.nombre}"`);
  }

  if (!confirm) {
    console.log('\n[reset-banks] Dry-run — no se borró nada. Volvé a correr con --confirm para borrar de verdad.');
    return;
  }

  await ConfigSection.destroy({ where: { clave: CLAVES } });
  console.log(`[reset-banks] Listo — ${secciones.length} sección(es) borrada(s) (cascada a sus valores/historial). Corré 'npm run seed:banks' para sembrar de nuevo.`);
}

// ── Ejecución directa: node src/banks/scripts/reset-global-config-banks.js ───
if (require.main === module) {
  const confirm = process.argv.includes('--confirm');
  const { connectPostgres, disconnectPostgres } = require('../../config/database.postgres');

  connectPostgres()
    .then(async () => {
      await resetGlobalConfigBanks({ confirm });
      await disconnectPostgres();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[reset-banks] Error:', err.message);
      process.exit(1);
    });
}

module.exports = resetGlobalConfigBanks;
