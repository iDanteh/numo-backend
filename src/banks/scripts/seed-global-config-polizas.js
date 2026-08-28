'use strict';

/**
 * banks/scripts/seed-global-config-polizas.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Seed, UNA VEZ por ambiente, de la sección `polizas` de Configuraciones
 * Globales (ver shared/services/global-config.service.js) — EXCLUSIVA de
 * Pólizas (cobros-sucursal-puente.service.js / cfdi-poliza-generator.service.js
 * vía erp-sync.service.js#_cajaBaseUrlPolizas/_tokenPolizas).
 *
 * Migrada 2026-08-28: antes `polizas` se dejaba fuera de Configuraciones
 * Globales por decisión explícita del usuario ("no quiero nada de pólizas por
 * ahora") — esa decisión ya no aplica ("no funciona nada si no está ahí").
 * Sigue el mismo patrón que seed-global-config-banks.js: nunca comparte
 * sección con `bancos` (esa es solo Bancos/Cobro/Reversiones).
 *
 *   polizas → CAJA_BASE_URL (Kore Cajas — desgloses de cobro de almacén y
 *             saldos a favor, por serie/folio o por centro+rango de fechas),
 *             TOKEN (secreto)
 *
 * Lee SIEMPRE del .env de ESTE ambiente. Para las variables que ya tenían un
 * fallback hardcodeado en el código, el seed reproduce EXACTAMENTE ese mismo
 * fallback si la variable no está en este .env — el seed nunca "adivina" un
 * valor distinto al que este ambiente ya usa hoy.
 *
 * Idempotente — correrlo de nuevo actualiza los valores (upsert por
 * sectionClave+clave).
 *
 * Uso (correr UNA VEZ por ambiente — cada uno con su propio .env/Postgres):
 *   node src/banks/scripts/seed-global-config-polizas.js
 *   npm run seed:polizas
 */

require('dotenv').config();

const svc = require('../../shared/services/global-config.service');
const { ConfigSection } = require('../../shared/models/postgres');

async function _asegurarSeccion(clave, { nombre, descripcion, modulos }) {
  const [section, creada] = await ConfigSection.findOrCreate({
    where:    { clave },
    defaults: { nombre, descripcion, modulosAfectados: modulos },
  });
  console.log(`[seed-polizas] Sección '${clave}' ${creada ? 'creada' : 'ya existía'} (id=${section.id}).`);

  if (!creada) {
    const actuales  = section.modulosAfectados || [];
    const faltantes = modulos.filter(m => !actuales.includes(m));
    const modulosFinal = [...actuales, ...faltantes];
    const cambioNombre = section.nombre !== nombre || section.descripcion !== descripcion;
    if (cambioNombre || faltantes.length > 0) {
      await svc.updateSection(section.id, { nombre, descripcion, modulosAfectados: modulosFinal });
      if (faltantes.length > 0) console.log(`[seed-polizas] '${clave}': agregados ${faltantes.length} módulo(s) afectado(s) nuevo(s).`);
      if (cambioNombre) console.log(`[seed-polizas] '${clave}': nombre/descripción actualizados.`);
    }
  }
  return section;
}

function _origen(envVar, usoFallback) {
  return usoFallback
    ? `(${envVar} no estaba definida en este .env — se usó el fallback que ya tenía el código)`
    : `(tomado de ${envVar} en este .env)`;
}

async function _sembrarClave(fallos, sectionClave, clave, fn) {
  try {
    await fn();
  } catch (err) {
    console.error(`[seed-polizas] ✖ ${sectionClave}.${clave}: ${err.message}`);
    fallos.push(`${sectionClave}.${clave}`);
  }
}

async function seedPolizas() {
  const fallos = [];

  await _asegurarSeccion('polizas', {
    nombre:      'Pólizas',
    descripcion: 'URL y token de Kore Cajas consumidos exclusivamente por el generador de pólizas (desgloses de cobro de almacén y saldos a favor entre sucursales).',
    modulos: [
      'CAJA_BASE_URL — Kore Cajas: /desgloses-cobro/almacen y /desgloses-cobro/saldos-favor, por serie/folio (obtenerDesglosesCobroAlmacen/obtenerSaldosFavor) y por centro+rango de fechas (obtenerDesglosesCobroAlmacenPorCentro/obtenerSaldosFavorPorCentro) — usadas por cobros-sucursal-puente.service.js y cfdi-poliza-generator.service.js',
      'TOKEN — Autenticación contra Kore Cajas para las mismas consultas de desgloses de cobro',
    ],
  });

  // CAJA_BASE_URL (antes ERP_CAJA_BASE_URL leído directo del .env)
  await _sembrarClave(fallos, 'polizas', 'CAJA_BASE_URL', async () => {
    const envVal  = process.env.ERP_CAJA_BASE_URL?.trim();
    if (!envVal) throw new Error('ERP_CAJA_BASE_URL no está definida en este .env.');

    await svc.setValue('polizas', 'CAJA_BASE_URL', envVal, {
      esSecreto: false, tipo: 'url',
      descripcion: 'Kore Cajas — desgloses de cobro de almacén y saldos a favor, exclusivo de Pólizas. Debe apuntar a https://app.cajas.tubosyconexiones.mx en producción.',
      usuarioNombre: 'seed-script',
    });
    console.log(`[seed-polizas] polizas.CAJA_BASE_URL = ${envVal} ${_origen('ERP_CAJA_BASE_URL', false)}`);
  });

  // TOKEN (antes ERP_TOKEN leído directo del .env, copia independiente del de `bancos`)
  await _sembrarClave(fallos, 'polizas', 'TOKEN', async () => {
    const token = process.env.ERP_TOKEN?.trim();
    if (!token) throw new Error('ERP_TOKEN no está definida en este .env.');

    await svc.setValue('polizas', 'TOKEN', token, {
      esSecreto: true, tipo: 'texto',
      descripcion: 'Token del ERP/Kore Cajas para autenticar las consultas de desgloses de cobro exclusivas de Pólizas. Mismo valor físico que .env ERP_TOKEN — copia independiente de bancos.TOKEN durante la migración.',
      usuarioNombre: 'seed-script',
    });
    console.log('[seed-polizas] polizas.TOKEN         = ••••••••  (secreto)');
  });

  if (fallos.length > 0) {
    throw new Error(`No se pudieron sembrar ${fallos.length} clave(s): ${fallos.join(', ')} — revisá el .env de este ambiente.`);
  }
  console.log('[seed-polizas] Listo — polizas sembrada correctamente.');
}

// ── Ejecución directa: node src/banks/scripts/seed-global-config-polizas.js ──
if (require.main === module) {
  const { connectPostgres, disconnectPostgres } = require('../../config/database.postgres');

  connectPostgres()
    .then(async () => {
      await seedPolizas();
      await disconnectPostgres();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[seed-polizas] Error:', err.message);
      process.exit(1);
    });
}

module.exports = seedPolizas;
