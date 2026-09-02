'use strict';

/**
 * banks/scripts/seed-global-config-banks.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Seed consolidado, UNA VEZ por ambiente, de las 3 secciones de Configuraciones
 * Globales (ver shared/services/global-config.service.js) que usan Bancos +
 * Solicitudes de Cobro + Reversiones. Pólizas queda fuera de este seed.
 *
 * Secciones sembradas (consolidado 2026-08-25 — antes eran 5 secciones más
 * finas: erp-caja/kore-formaspago/kore-caja/kore-webhooks/erp-fact; se
 * agruparon en 3 para que el admin vea menos secciones sin perder rastro
 * puntual de qué hace cada clave — ver `modulosAfectados` de cada una, con
 * formato "CLAVE — descripción" por entrada):
 *
 *   bancos      → CUENTAS_PENDIENTES_URL, TOKEN (secreto), DATE_WINDOW_DAYS,
 *                 FORMASPAGO_BASE_URL, FACT_BASE_URL
 *   kore        → AUTH_URL, SERVICIO, CAJA_URL, CAJA_BASE_URL
 *   solicitudes → API_KEY (secreto)
 *
 * El catálogo de formas de pago (FORMASPAGO_BASE_URL) vive en `bancos`, no en
 * `kore` — decisión explícita del usuario: "es donde se aplican los cobros".
 *
 * Lee SIEMPRE del .env de ESTE ambiente — nunca copiar un valor entre ambientes
 * (el incidente original con ERP_CAJA_BASE_TEST_URL, 2026-08-24, fue justamente
 * por mezclar producción/test). Para las variables que ya tenían un fallback
 * hardcodeado en el código, el seed reproduce EXACTAMENTE ese mismo fallback si
 * la variable no está en este .env — así el seed nunca "adivina" un valor
 * distinto al que este ambiente ya usa hoy.
 *
 * Cada CLAVE se siembra de forma independiente dentro de su sección: si a UNA
 * le falta una variable requerida en este .env (ej. ERP_FACT_BASE_URL), se
 * loguea el error puntual y se sigue con el resto de las claves de esa misma
 * sección — no aborta toda la sección por una sola clave faltante. Al final el
 * script sale con código 1 si alguna clave falló, para que quede claro en
 * CI/consola sin bloquear las que sí pudieron sembrarse.
 *
 * Idempotente — correrlo de nuevo actualiza los valores (upsert por
 * sectionClave+clave).
 *
 * NOTA sobre BASE_URL de Kore Cajas para almacén/saldos-favor: es de uso
 * EXCLUSIVO de Pólizas (cobros-sucursal-puente.service.js/
 * cfdi-poliza-generator.service.js) — nunca se sembró acá, erp-sync.service.js
 * la lee directo de process.env.ERP_CAJA_BASE_URL.
 *
 * Si tu ambiente todavía tiene datos de la estructura vieja (5 secciones:
 * erp-caja/kore-formaspago/kore-caja/kore-webhooks/erp-fact), corré primero
 * `npm run reset:banks -- --confirm` para depurarlos antes de sembrar de nuevo
 * con esta estructura de 3 secciones.
 *
 * Uso (correr UNA VEZ por ambiente — cada uno con su propio .env/Postgres):
 *   node src/banks/scripts/seed-global-config-banks.js
 *   npm run seed:banks
 */

require('dotenv').config();

const svc = require('../../shared/services/global-config.service');
const { ConfigSection } = require('../../shared/models/postgres');

// Asegura que una sección exista (findOrCreate) y que nombre/descripcion/
// modulosAfectados queden EXACTAMENTE como este seed los define — el seed es la
// fuente de verdad para estos 3 campos. modulosAfectados es unión (no pierde
// entradas que un admin haya agregado a mano desde la UI), pero nombre/
// descripcion se SOBRESCRIBEN siempre.
async function _asegurarSeccion(clave, { nombre, descripcion, modulos }) {
  const [section, creada] = await ConfigSection.findOrCreate({
    where:    { clave },
    defaults: { nombre, descripcion, modulosAfectados: modulos },
  });
  console.log(`[seed-banks] Sección '${clave}' ${creada ? 'creada' : 'ya existía'} (id=${section.id}).`);

  if (!creada) {
    const actuales  = section.modulosAfectados || [];
    const faltantes = modulos.filter(m => !actuales.includes(m));
    const modulosFinal = [...actuales, ...faltantes];
    const cambioNombre = section.nombre !== nombre || section.descripcion !== descripcion;
    if (cambioNombre || faltantes.length > 0) {
      await svc.updateSection(section.id, { nombre, descripcion, modulosAfectados: modulosFinal });
      if (faltantes.length > 0) console.log(`[seed-banks] '${clave}': agregados ${faltantes.length} módulo(s) afectado(s) nuevo(s).`);
      if (cambioNombre) console.log(`[seed-banks] '${clave}': nombre/descripción actualizados.`);
    }
  }
  return section;
}

function _origen(envVar, usoFallback) {
  return usoFallback
    ? `(${envVar} no estaba definida en este .env — se usó el fallback que ya tenía el código)`
    : `(tomado de ${envVar} en este .env)`;
}

// Sembrado de UNA clave, aislado: si falla, se loguea puntual y se acumula en
// `fallos` — nunca detiene el resto de las claves de la misma sección.
async function _sembrarClave(fallos, sectionClave, clave, fn) {
  try {
    await fn();
  } catch (err) {
    console.error(`[seed-banks] ✖ ${sectionClave}.${clave}: ${err.message}`);
    fallos.push(`${sectionClave}.${clave}`);
  }
}

// ── bancos ────────────────────────────────────────────────────────────────────
// EXCLUSIVA de Bancos/Solicitudes de Cobro/Reversiones. Consolida lo que antes
// eran 3 secciones (erp-caja, kore-formaspago, erp-fact).
async function seedBancos(fallos) {
  await _asegurarSeccion('bancos', {
    nombre:      'Bancos',
    descripcion: 'URLs, token y tuning del ERP/Kore consumidos por el módulo de Bancos — sync de Cuentas Pendientes, motor de coincidencia automática, catálogo de formas de pago (donde se aplican los cobros) y reporte de facturación.',
    modulos: [
      'CUENTAS_PENDIENTES_URL — Sync de Cuentas Pendientes: solicitudes de cobro, reversiones, bank-sync (respeta el ambiente)',
      'TOKEN — Autenticación contra el ERP para sincronizar cuentas pendientes y generar el reporte de CFDIs con Pagos',
      'DATE_WINDOW_DAYS — Motor de coincidencia automática ERP↔movimientos bancarios (ventana de fecha, ±días)',
      'FORMASPAGO_BASE_URL — Catálogo de bancos/formas de pago: cobro manual (GET /cobros/bancos, /formas-pago) y aplicar cobro automático desde una Solicitud de Cobro (resolver BancoID)',
      'FACT_BASE_URL — Reporte "CFDIs con Pagos" (pagos-banco)',
      'NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS — Filtro de transferencias entre cajas (JSON array de strings): solo se sincronizan transferencias cuyo nombreTipoTransferencia esté en esta lista. Vacío/[] = sin filtro (se sincroniza todo).',
      'NOMBRE_CAJA_DESTINO_PERMITIDAS — Filtro de transferencias entre cajas (JSON array de strings): solo se sincronizan transferencias cuyo nombreCajaDestino esté en esta lista. Vacío/[] = sin filtro (se sincroniza todo).',
      'TRANSFERENCIAS_DATE_WINDOW_DAYS — Ventana de fecha (± días) del matching de transferencias entre cajas contra Depósito en efectivo. Distinta de DATE_WINDOW_DAYS (esa es del motor ERP↔CxC).',
    ],
  });

  // CUENTAS_PENDIENTES_URL (antes erp-caja.CUENTAS_PENDIENTES_URL)
  await _sembrarClave(fallos, 'bancos', 'CUENTAS_PENDIENTES_URL', async () => {
    const testUrl = process.env.ERP_CAJA_BASE_TEST_URL?.trim();
    const baseUrl = process.env.ERP_CAJA_BASE_URL?.trim();
    const cuentasPendientesUrl = testUrl || baseUrl;
    if (!cuentasPendientesUrl) throw new Error('Ni ERP_CAJA_BASE_TEST_URL ni ERP_CAJA_BASE_URL están definidas en este .env.');

    await svc.setValue('bancos', 'CUENTAS_PENDIENTES_URL', cuentasPendientesUrl, {
      esSecreto: false, tipo: 'url',
      descripcion: 'Kore Cajas — /cuentas-pendientes (solicitudes de cobro, reversiones, bank-sync).',
      usuarioNombre: 'seed-script',
    });
    console.log(
      `[seed-banks] bancos.CUENTAS_PENDIENTES_URL = ${cuentasPendientesUrl} ` +
      (testUrl ? '(tomado de ERP_CAJA_BASE_TEST_URL)' : '(igual a ERP_CAJA_BASE_URL — sin override de test en este .env)'),
    );
  });

  // TOKEN (antes erp-caja.TOKEN)
  await _sembrarClave(fallos, 'bancos', 'TOKEN', async () => {
    const token = process.env.ERP_TOKEN?.trim();
    if (!token) throw new Error('ERP_TOKEN no está definida en este .env.');

    await svc.setValue('bancos', 'TOKEN', token, {
      esSecreto: true, tipo: 'texto',
      descripcion: 'Token del ERP para autenticar erp-sync.service.js/erp.routes.js (sync de Cuentas Pendientes y reporte de facturación). Mismo valor físico que .env ERP_TOKEN (visor/services/erp.service.js sigue leyendo el .env directo).',
      usuarioNombre: 'seed-script',
    });
    console.log('[seed-banks] bancos.TOKEN                  = ••••••••  (secreto)');
  });

  // DATE_WINDOW_DAYS (antes erp-caja.DATE_WINDOW_DAYS)
  await _sembrarClave(fallos, 'bancos', 'DATE_WINDOW_DAYS', async () => {
    const diasStr = process.env.ERP_DATE_WINDOW_DAYS?.trim();
    const dias    = diasStr || '30';

    await svc.setValue('bancos', 'DATE_WINDOW_DAYS', dias, {
      esSecreto: false, tipo: 'numero',
      descripcion: 'Ventana de fecha (± días) del motor de coincidencia automática ERP↔movimientos bancarios.',
      usuarioNombre: 'seed-script',
    });
    console.log(`[seed-banks] bancos.DATE_WINDOW_DAYS       = ${dias} ${_origen('ERP_DATE_WINDOW_DAYS', !diasStr)}`);
  });

  // FORMASPAGO_BASE_URL (antes kore-formaspago.BASE_URL)
  await _sembrarClave(fallos, 'bancos', 'FORMASPAGO_BASE_URL', async () => {
    const FALLBACK = 'https://test.formaspagos.koreingenieria.com';
    const envVal   = process.env.KORE_FORMASPAGO_BASE_URL?.trim();
    const baseUrl  = envVal || FALLBACK;

    await svc.setValue('bancos', 'FORMASPAGO_BASE_URL', baseUrl, {
      esSecreto: false, tipo: 'url',
      descripcion: 'Kore — catálogo de bancos/formas de pago (listarBancos/listarFormasPago), donde se aplican los cobros.',
      usuarioNombre: 'seed-script',
    });
    console.log(`[seed-banks] bancos.FORMASPAGO_BASE_URL     = ${baseUrl} ${_origen('KORE_FORMASPAGO_BASE_URL', !envVal)}`);
  });

  // FACT_BASE_URL (antes erp-fact.BASE_URL)
  await _sembrarClave(fallos, 'bancos', 'FACT_BASE_URL', async () => {
    const baseUrl = process.env.ERP_FACT_BASE_URL?.trim();
    if (!baseUrl) throw new Error('ERP_FACT_BASE_URL no está definida en este .env.');

    await svc.setValue('bancos', 'FACT_BASE_URL', baseUrl, {
      esSecreto: false, tipo: 'url',
      descripcion: 'ERP — facturación, usada por el reporte "CFDIs con Pagos" (GET /api/erp/reporte).',
      usuarioNombre: 'seed-script',
    });
    console.log(`[seed-banks] bancos.FACT_BASE_URL          = ${baseUrl} (tomado de ERP_FACT_BASE_URL en este .env)`);
  });

  // NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS / NOMBRE_CAJA_DESTINO_PERMITIDAS (Fase B, matching
  // de transferencias entre cajas) — a diferencia de las claves de arriba, NO tienen un valor
  // "correcto" derivable del .env (son listas de negocio que el usuario define desde la UI).
  // Por eso, a diferencia del resto de este script, solo se siembran si la fila TODAVÍA NO
  // EXISTE — un re-run de este seed nunca debe pisar un filtro que un admin ya configuró.
  for (const clave of ['NOMBRE_TIPO_TRANSFERENCIA_PERMITIDOS', 'NOMBRE_CAJA_DESTINO_PERMITIDAS']) {
    // eslint-disable-next-line no-await-in-loop
    await _sembrarClave(fallos, 'bancos', clave, async () => {
      const yaExiste = await svc.getValue('bancos', clave).then(() => true).catch(() => false);
      if (yaExiste) {
        console.log(`[seed-banks] bancos.${clave} ya existe — no se pisa (puede tener un filtro ya configurado).`);
        return;
      }
      await svc.setValue('bancos', clave, '[]', {
        esSecreto: false, tipo: 'lista',
        descripcion: 'JSON array de strings — filtro de transferencias entre cajas (Fase B). Vacío = sin filtro, se sincroniza todo.',
        usuarioNombre: 'seed-script',
      });
      console.log(`[seed-banks] bancos.${clave} = [] (default, sin filtro — configurar desde la UI de Configuraciones Globales)`);
    });
  }

  // TRANSFERENCIAS_DATE_WINDOW_DAYS (Fase C) — mismo criterio "solo si no existe": es un
  // valor de tuning que un admin puede ajustar, un re-run del seed no debe resetearlo.
  await _sembrarClave(fallos, 'bancos', 'TRANSFERENCIAS_DATE_WINDOW_DAYS', async () => {
    const yaExiste = await svc.getValue('bancos', 'TRANSFERENCIAS_DATE_WINDOW_DAYS').then(() => true).catch(() => false);
    if (yaExiste) {
      console.log('[seed-banks] bancos.TRANSFERENCIAS_DATE_WINDOW_DAYS ya existe — no se pisa.');
      return;
    }
    await svc.setValue('bancos', 'TRANSFERENCIAS_DATE_WINDOW_DAYS', '5', {
      esSecreto: false, tipo: 'numero',
      descripcion: 'Ventana de fecha (± días) del matching de transferencias entre cajas contra Depósito en efectivo. Default de arranque, ajustable desde la UI.',
      usuarioNombre: 'seed-script',
    });
    console.log('[seed-banks] bancos.TRANSFERENCIAS_DATE_WINDOW_DAYS = 5 (default de arranque — ajustable desde la UI)');
  });
}

// ── kore ──────────────────────────────────────────────────────────────────────
// EXCLUSIVA de Bancos/Solicitudes de Cobro — auth, sesión de caja y aplicación
// de cobros contra Kore (antes sección `kore-caja`).
async function seedKore(fallos) {
  await _asegurarSeccion('kore', {
    nombre:      'Kore',
    descripcion: 'Autenticación, sesión de caja y aplicación de cobros contra Kore — consumido por kore-caja.service.js.',
    modulos: [
      'AUTH_URL — Autenticación de usuario contra Kore: intercambia el sub de Auth0 por un token Kore (obtenerTokenKore)',
      'SERVICIO — Identificador de servicio Kore usado en esa misma autenticación de usuario',
      'CAJA_URL — Sesión de caja activa del cajero, verificada antes de aplicar cualquier cobro (obtenerSesionCaja)',
      'CAJA_BASE_URL — Aplicar cobro manual (panel de cobros), aplicar cobro de una Solicitud de Cobro ya aprobada, actualizar estatus de revisión contable ante Kore, y consultar saldo en vivo de una CxC',
    ],
  });

  const FALLBACKS = {
    AUTH_URL:      'https://app.login.tubosyconexiones.mx/logink/tokenKore',
    SERVICIO:      '6491faf156358100016565e5',
    CAJA_URL:      'https://test.cajas.koreingenieria.com/index',
    CAJA_BASE_URL: 'https://test.cajas.koreingenieria.com',
  };
  const ENV_VARS = {
    AUTH_URL:      'KORE_AUTH_URL',
    SERVICIO:      'KORE_SERVICIO',
    CAJA_URL:      'KORE_CAJA_URL',
    CAJA_BASE_URL: 'KORE_CAJA_BASE_URL',
  };
  const DESCRIPCIONES = {
    AUTH_URL:      'Kore — autenticación de usuario (intercambio Auth0 sub → token Kore). Apunta a producción en TODOS los ambientes.',
    SERVICIO:      'Kore — identificador de servicio usado en la autenticación de usuario.',
    CAJA_URL:      'Kore — sesión de caja activa del cajero.',
    CAJA_BASE_URL: 'Kore — aplicar cobro (manual/solicitud), actualizar estatus de revisión contable, consultar saldo en vivo de una CxC.',
  };

  for (const clave of Object.keys(ENV_VARS)) {
    const envVar = ENV_VARS[clave];
    // eslint-disable-next-line no-await-in-loop
    await _sembrarClave(fallos, 'kore', clave, async () => {
      const envVal = process.env[envVar]?.trim();
      const valor  = envVal || FALLBACKS[clave];
      const tipo   = clave === 'SERVICIO' ? 'texto' : 'url';
      await svc.setValue('kore', clave, valor, {
        esSecreto: false, tipo,
        descripcion: DESCRIPCIONES[clave],
        usuarioNombre: 'seed-script',
      });
      console.log(`[seed-banks] kore.${clave} = ${valor} ${_origen(envVar, !envVal)}`);
    });
  }
}

// ── solicitudes ───────────────────────────────────────────────────────────────
// EXCLUSIVA de Solicitudes de Cobro/Reversiones — autenticación de las llamadas
// que Kore hace HACIA Numo (antes sección `kore-webhooks`).
async function seedSolicitudes(fallos) {
  await _asegurarSeccion('solicitudes', {
    nombre:      'Solicitudes de Cobro',
    descripcion: 'API key compartida para autenticar llamadas server-to-server que Kore hace HACIA Numo (sin JWT/Auth0).',
    modulos: [
      'API_KEY — Autentica llamadas server-to-server que Kore hace hacia Numo: el webhook de reversión de CxC (POST /api/erp/cxc-reversiones) y los endpoints de Solicitudes de Cobro que el ERP llama directamente (crear, consultar por id, cancelar)',
    ],
  });

  await _sembrarClave(fallos, 'solicitudes', 'API_KEY', async () => {
    const apiKey = process.env.KORE_API_KEY?.trim();
    if (!apiKey) throw new Error('KORE_API_KEY no está definida en este .env.');

    await svc.setValue('solicitudes', 'API_KEY', apiKey, {
      esSecreto: true, tipo: 'texto',
      descripcion: 'API key server-to-server que Kore manda en el header X-Api-Key.',
      usuarioNombre: 'seed-script',
    });
    console.log('[seed-banks] solicitudes.API_KEY = ••••••••  (secreto)');
  });
}

async function seedBanks() {
  const fallos = [];

  await seedBancos(fallos);
  await seedKore(fallos);
  await seedSolicitudes(fallos);

  if (fallos.length > 0) {
    throw new Error(`No se pudieron sembrar ${fallos.length} clave(s): ${fallos.join(', ')} — revisá el .env de este ambiente.`);
  }
  console.log('[seed-banks] Listo — bancos/kore/solicitudes sembradas correctamente.');
}

// ── Ejecución directa: node src/banks/scripts/seed-global-config-banks.js ────
if (require.main === module) {
  const { connectPostgres, disconnectPostgres } = require('../../config/database.postgres');

  connectPostgres()
    .then(async () => {
      await seedBanks();
      await disconnectPostgres();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[seed-banks] Error:', err.message);
      process.exit(1);
    });
}

module.exports = seedBanks;
