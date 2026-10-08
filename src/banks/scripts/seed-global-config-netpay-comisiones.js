'use strict';

/**
 * banks/scripts/seed-global-config-netpay-comisiones.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Seed de las secciones 'netpay-comisiones-tarjetas' y 'netpay-comisiones-sucursales'
 * de Configuraciones Globales (ver shared/services/global-config.service.js) — pedido
 * explícito del usuario, 2026-10-07: "tener dentro de la Central de Configuración, una
 * nueva sección llamada Netpay".
 *
 * 2 secciones en vez de 1 (fix 2026-10-07, pedido explícito del usuario): la primera
 * versión usaba una sola sección 'netpay-comisiones' con tarjetas y sucursales
 * mezcladas (57 registros juntos en una sola lista) — separarlas en 2 secciones usa
 * el agrupamiento por sección que la UI YA provee (el panel lateral de
 * config-admin.component.ts), sin tocar el frontend.
 *
 * Solo da de alta las SECCIONES (para que aparezcan en la UI de administración,
 * 100% genérica — config-admin.component.ts no necesita ningún cambio). NO
 * siembra ningún valor: las claves se pueblan solas, una por categoría de
 * comisión detectada, cada vez que se carga un reporte Netpay (ver
 * netpay-comision-sync.service.js#sincronizarComisiones, enganchado en
 * netpay-reporte.service.js#_crearYEvaluar) — mismo criterio ya usado para
 * 'emails-alerta' en system-monitor/seed-global-config-system-monitor.js.
 *
 * Para los reportes YA cargados antes de correr este seed, ver
 * scripts/backfill-netpay-comisiones-config.js (backfill retroactivo). Si en este
 * ambiente ya existía la sección única vieja 'netpay-comisiones' (versión anterior
 * al fix de agrupación), hay que limpiarla primero con
 * scripts/reset-netpay-comisiones-config.js antes de correr este seed + el backfill.
 *
 * Uso (correr UNA VEZ por ambiente — idempotente, no pisa una sección ya creada):
 *   node src/banks/scripts/seed-global-config-netpay-comisiones.js
 */

require('dotenv').config();

const { ConfigSection } = require('../../shared/models/postgres');

async function _asegurarSeccion(clave, { nombre, descripcion, modulos }) {
  const [section, creada] = await ConfigSection.findOrCreate({
    where:    { clave },
    defaults: { nombre, descripcion, modulosAfectados: modulos },
  });
  console.log(`[seed-netpay-comisiones] Sección '${clave}' ${creada ? 'creada' : 'ya existía'} (id=${section.id}).`);
  return section;
}

async function seedNetpayComisiones() {
  await _asegurarSeccion('netpay-comisiones-tarjetas', {
    nombre: 'Netpay — Tarjetas',
    descripcion: 'Comisión base que Netpay aplica por banco emisor + tipo de tarjeta '
      + '(crédito/débito), detectada automáticamente desde cada reporte Netpay cargado. Se '
      + 'actualiza sola — no hace falta editar a mano salvo para corregir un dato erróneo.',
    modulos: [
      'Se actualiza automáticamente al cargar un reporte Netpay '
      + '(netpay-comision-sync.service.js). Claves con formato "tarjeta-<banco>-<tipo>". '
      + 'Ver el historial de cada clave para los cambios de tasa a lo largo del tiempo.',
    ],
  });

  await _asegurarSeccion('netpay-comisiones-sucursales', {
    nombre: 'Netpay — Sucursales',
    descripcion: 'Comisión base que Netpay aplica por sucursal (agrupada por nombre real del '
      + 'local — una sucursal puede tener varias terminales/storeId distintos), detectada '
      + 'automáticamente desde cada reporte Netpay cargado. Se actualiza sola — no hace falta '
      + 'editar a mano salvo para corregir un dato erróneo.',
    modulos: [
      'Se actualiza automáticamente al cargar un reporte Netpay '
      + '(netpay-comision-sync.service.js). Claves con formato "sucursal-<nombre>" — '
      + 'agrupadas por NOMBRE de sucursal, no por storeId (una sucursal física puede tener '
      + 'varias terminales). Ver el historial de cada clave para los cambios de tasa a lo '
      + 'largo del tiempo.',
    ],
  });

  console.log('[seed-netpay-comisiones] Listo — secciones "netpay-comisiones-tarjetas" y "netpay-comisiones-sucursales" disponibles en Configuraciones Globales.');
}

// ── Ejecución directa: node src/banks/scripts/seed-global-config-netpay-comisiones.js ──
if (require.main === module) {
  const { connectPostgres, disconnectPostgres } = require('../../config/database.postgres');

  connectPostgres()
    .then(async () => {
      await seedNetpayComisiones();
      await disconnectPostgres();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[seed-netpay-comisiones] Error:', err.message);
      process.exit(1);
    });
}

module.exports = seedNetpayComisiones;
