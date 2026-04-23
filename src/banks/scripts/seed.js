'use strict';

/**
 * banks/scripts/seed.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Genera el usuario administrador por defecto en PostgreSQL.
 *
 * - Seguro de ejecutar múltiples veces (idempotente).
 * - Si ya existe un usuario con rol 'admin' no hace nada.
 * - El auth0Sub se asigna como placeholder 'seed:<email>' hasta que el
 *   usuario haga su primer login con Auth0; en ese momento findOrCreate()
 *   reemplaza el placeholder con el sub real.
 *
 * Variables de entorno:
 *   SEED_ADMIN_EMAIL   — email del admin a pre-registrar
 *   SEED_ADMIN_NOMBRE  — nombre del admin (opcional)
 *   SEED_USER2_EMAIL   — email del segundo usuario a pre-registrar (opcional)
 *   SEED_USER2_NOMBRE  — nombre del segundo usuario (opcional)
 *   SEED_USER2_ROLE    — rol del segundo usuario (default: 'tienda')
 *   SEED_USER3_EMAIL   — email del tercer usuario a pre-registrar (opcional)
 *   SEED_USER3_NOMBRE  — nombre del tercer usuario (opcional)
 *   SEED_USER3_ROLE    — rol del tercer usuario (default: 'tienda')
 */

require('dotenv').config();

const { User } = require('../../shared/models/postgres');
const { ROLES, PERMISSIONS } = require('../../shared/config/rbac');

const PERM_META = {
  'banks:read':         { label: 'Ver movimientos bancarios',       module: 'Bancos' },
  'banks:import':       { label: 'Importar movimientos',            module: 'Bancos' },
  'banks:update':       { label: 'Editar movimientos',              module: 'Bancos' },
  'banks:config':       { label: 'Configurar bancos',               module: 'Bancos' },
  'banks:rules':        { label: 'Reglas de clasificación',         module: 'Bancos' },
  'account-plan:read':  { label: 'Ver catálogo contable',           module: 'Contabilidad' },
  'account-plan:write': { label: 'Editar catálogo contable',        module: 'Contabilidad' },
  'collections:read':   { label: 'Ver solicitudes de cobranza',     module: 'Cobranza' },
  'collections:write':  { label: 'Gestionar cobranza',              module: 'Cobranza' },
  'erp:manage':         { label: 'Integración ERP',                 module: 'ERP' },
  'visor:read':         { label: 'Ver CFDIs',                       module: 'Visor' },
  'visor:write':        { label: 'Gestionar CFDIs',                 module: 'Visor' },
  'visor:sat':          { label: 'Descarga SAT',                    module: 'Visor' },
  'visor:reports':      { label: 'Reportes CFDI',                   module: 'Visor' },
  'drive:read':         { label: 'Google Drive (leer)',             module: 'Drive' },
  'drive:import':       { label: 'Google Drive (importar)',         module: 'Drive' },
  'entities:read':      { label: 'Ver entidades fiscales',          module: 'Entidades' },
  'entities:write':     { label: 'Gestionar entidades fiscales',    module: 'Entidades' },
  'users:manage':       { label: 'Administrar usuarios y roles',    module: 'Administración' },
};

async function seedRbac() {
  const { Role, Permission } = require('../../shared/models/postgres');
  const [permCount, roleCount] = await Promise.all([Permission.count(), Role.count()]);

  if (permCount === 0) {
    const perms = Object.values(PERMISSIONS).map(key => ({
      key,
      label:  PERM_META[key]?.label  ?? key,
      module: PERM_META[key]?.module ?? 'General',
    }));
    await Permission.bulkCreate(perms, { ignoreDuplicates: true });
    console.log(`[seed] ${perms.length} permisos sembrados.`);
  }

  if (roleCount === 0) {
    const roles = Object.entries(ROLES).map(([value, { label, permissions }]) => ({
      value, label, permissions, isSystem: true,
    }));
    await Role.bulkCreate(roles, { ignoreDuplicates: true });
    console.log(`[seed] ${roles.length} roles sembrados.`);
  }
}

async function seedUser({ email, nombre, role }) {
  const existing = await User.findOne({ where: { email } });
  if (existing) {
    console.log(`[seed] Usuario ya existe (${email}). Sin cambios.`);
    return;
  }
  await User.create({
    auth0Sub: `seed:${email}`,
    nombre:   nombre ?? '',
    email,
    role,
    isActive: true,
  });
  console.log(`[seed] Usuario creado → ${email} (${role})`);
  console.log(`[seed] Cuando ese email inicie sesión con Auth0, quedará vinculado automáticamente.`);
}

async function seed() {
  const adminEmail  = process.env.SEED_ADMIN_EMAIL;
  const adminNombre = process.env.SEED_ADMIN_NOMBRE;

  // Siempre sembrar roles y permisos — el sistema no funciona sin esto.
  await seedRbac();

  // La creación del usuario admin es opcional: solo si se proporcionó el email.
  if (!adminEmail) {
    console.log('[seed] SEED_ADMIN_EMAIL no definido — omitiendo creación de admin.');
    console.log('[seed] El primer usuario que inicie sesión con Auth0 obtendrá rol "tienda".');
  } else {
    const existingAdmin = await User.findOne({ where: { role: 'admin' } });
    if (existingAdmin) {
      console.log(`[seed] Admin ya existe (${existingAdmin.email || existingAdmin.auth0Sub}). Sin cambios.`);
    } else {
      await seedUser({ email: adminEmail, nombre: adminNombre, role: 'admin' });
    }
  }

  // Usuarios adicionales — opcionales, idempotentes por email.
  const extraUsers = [
    { email: process.env.SEED_USER2_EMAIL, nombre: process.env.SEED_USER2_NOMBRE, role: process.env.SEED_USER2_ROLE || 'tienda' },
    { email: process.env.SEED_USER3_EMAIL, nombre: process.env.SEED_USER3_NOMBRE, role: process.env.SEED_USER3_ROLE || 'tienda' },
  ];

  for (const user of extraUsers) {
    if (!user.email) continue;
    await seedUser(user);
  }
}

// ── Ejecución directa: node src/banks/scripts/seed.js ────────────────────────
if (require.main === module) {
  const { connectPostgres, disconnectPostgres } = require('../../config/database.postgres');

  connectPostgres()
    .then(async () => {
      await seed();
      await disconnectPostgres();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[seed] Error:', err.message);
      process.exit(1);
    });
}

module.exports = seed;
