'use strict';

/**
 * system-monitor/seed-global-config-system-monitor.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Seed de la sección 'system-monitor' de Configuraciones Globales (ver
 * shared/services/global-config.service.js) — Panel de Tráfico del Sistema.
 *
 * Solo da de alta la SECCIÓN (para que aparezca en la UI de administración,
 * 100% genérica — config-admin.component.ts no necesita ningún cambio). La
 * clave 'emails-alerta' NO se siembra con un valor: no hay ninguna variable de
 * .env de la que migrarla (es un dato nuevo, no un reemplazo), así que el admin
 * la declara él mismo desde la UI (mismo criterio ya usado para
 * FICHAS_IMAGEN_FOLDER_ID/COMPROBANTES_IMAGEN_FOLDER_ID en seed-global-config-banks.js,
 * ver banks/scripts/seed-global-config-banks.js). Mientras no se configure,
 * system-monitor-alerta.job.js loguea un warning y omite el envío — nunca tira la app.
 *
 * Uso (correr UNA VEZ por ambiente — idempotente, no pisa una sección ya creada):
 *   node src/system-monitor/seed-global-config-system-monitor.js
 */

require('dotenv').config();

const { ConfigSection } = require('../shared/models/postgres');

async function _asegurarSeccion(clave, { nombre, descripcion, modulos }) {
  const [section, creada] = await ConfigSection.findOrCreate({
    where:    { clave },
    defaults: { nombre, descripcion, modulosAfectados: modulos },
  });
  console.log(`[seed-system-monitor] Sección '${clave}' ${creada ? 'creada' : 'ya existía'} (id=${section.id}).`);
  return section;
}

async function seedSystemMonitor() {
  await _asegurarSeccion('system-monitor', {
    nombre: 'Panel de Tráfico del Sistema',
    descripcion: 'Configuración del panel admin-only de tráfico/salud del sistema '
      + '(requests, errores, Mongo/Postgres, CPU/memoria) y sus alertas por correo.',
    modulos: [
      'emails-alerta — CSV de correos que reciben aviso cuando el sistema pasa a '
      + 'degradado/caído, recordatorio cada 15min mientras se mantiene así, y aviso '
      + 'al recuperarse. Vacío/sin configurar = alertas deshabilitadas '
      + '(system-monitor-alerta.job.js solo loguea un warning, nunca falla).',
    ],
  });
  console.log('[seed-system-monitor] Listo — sección "system-monitor" disponible en Configuraciones Globales.');
}

// ── Ejecución directa: node src/system-monitor/seed-global-config-system-monitor.js ──
if (require.main === module) {
  const { connectPostgres, disconnectPostgres } = require('../config/database.postgres');

  connectPostgres()
    .then(async () => {
      await seedSystemMonitor();
      await disconnectPostgres();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[seed-system-monitor] Error:', err.message);
      process.exit(1);
    });
}

module.exports = seedSystemMonitor;
