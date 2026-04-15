'use strict';

/**
 * config/env.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Punto único de acceso a variables de entorno.
 *
 * • Carga .env mediante dotenv (solo en desarrollo; en producción las vars
 *   deben inyectarse directamente por el sistema operativo / orquestador).
 * • Valida que las variables críticas existan; si falta alguna el proceso
 *   termina inmediatamente con un mensaje claro — nunca arranca a medias.
 * • Exporta un objeto `config` inmutable y estructurado.
 *   → Nunca uses process.env directamente en el resto del código.
 */

require('dotenv').config();

// ── Variables requeridas ──────────────────────────────────────────────────────
// La aplicación NO puede arrancar sin estas. No tienen valores por defecto
// seguros y su ausencia provocaría fallos en tiempo de ejecución o brechas
// de seguridad.
const REQUIRED = [
  'JWT_SECRET',          // Firma tokens de sesión
  'CREDS_MASTER_KEY',    // Cifra credenciales e.firma del SAT
  'ERP_BASE_URL',        // URL base del ERP externo
  'ERP_TOKEN',           // Token de autenticación del ERP
];

const missing = REQUIRED.filter((key) => !process.env[key]?.trim());

if (missing.length > 0) {
  console.error('\n╔══════════════════════════════════════════════════════════╗');
  console.error('║  ERROR DE CONFIGURACIÓN — El servidor no puede iniciar   ║');
  console.error('╚══════════════════════════════════════════════════════════╝\n');
  console.error('Faltan las siguientes variables de entorno requeridas:\n');
  missing.forEach((key) => console.error(`  ✖  ${key}`));
  console.error('\nSolución:');
  console.error('  1. Copia .env.example a .env');
  console.error('  2. Define el valor de cada variable marcada con ✖');
  console.error('  3. Reinicia el servidor\n');
  process.exit(1);
}

// ── Objeto de configuración centralizado ─────────────────────────────────────
// Object.freeze() garantiza que nadie modifique los valores en tiempo de
// ejecución de forma accidental.
const config = Object.freeze({

  /** Entorno de ejecución: development | test | production */
  env: process.env.NODE_ENV || 'development',

  /** Puerto en que escucha Express */
  port: parseInt(process.env.PORT, 10) || 3000,

  /** Base de datos MongoDB */
  db: Object.freeze({
    uri: process.env.MONGODB_URI || 'mongodb://localhost:27017/cfdi_comparator',
  }),

  /** CORS */
  cors: Object.freeze({
    origin: process.env.CORS_ORIGIN || 'http://localhost:4200',
  }),

  /** Autenticación JWT */
  jwt: Object.freeze({
    secret:    process.env.JWT_SECRET,
    expiresIn: process.env.JWT_EXPIRES_IN || '24h',
  }),

  /** Integración con el ERP externo */
  erp: Object.freeze({
    baseUrl: process.env.ERP_BASE_URL?.trim(),
    // Normaliza el token: garantiza el prefijo "Bearer " independientemente
    // de si el .env lo incluye o no.
    token: (() => {
      const t = process.env.ERP_TOKEN?.trim() ?? '';
      return t.startsWith('Bearer ') ? t : `Bearer ${t}`;
    })(),
  }),

  /** SAT — servicios web y configuración de descarga masiva */
  sat: Object.freeze({
    autenticacion:  process.env.SAT_DESCARGA_MASIVA_AUTENTICACION,
    solicitud:      process.env.SAT_DESCARGA_MASIVA_SOLICITUD,
    verifica:       process.env.SAT_DESCARGA_MASIVA_VERIFICA,
    descarga:       process.env.SAT_DESCARGA_MASIVA_DESCARGA,
    credsMasterKey: process.env.CREDS_MASTER_KEY,
    cronHora:       process.env.CRON_HORA || '0 1 * * *',
  }),

  /** Rate limiting */
  rateLimit: Object.freeze({
    windowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS, 10) || 15 * 60 * 1000,
    max:      parseInt(process.env.RATE_LIMIT_MAX, 10) || 600,
  }),

  /** Logging (Winston) */
  log: Object.freeze({
    level: process.env.LOG_LEVEL || 'info',
    file:  process.env.LOG_FILE  || 'logs/app.log',
  }),

  /** Google Drive — integración via Service Account */
  google: Object.freeze({
    serviceAccountKey: process.env.GOOGLE_SERVICE_ACCOUNT_KEY   || null,
    driveRootFolderId: process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID  || null,
    driveErpFolderId:  process.env.GOOGLE_DRIVE_ERP_FOLDER_ID   || null,
  }),

});

module.exports = config;
