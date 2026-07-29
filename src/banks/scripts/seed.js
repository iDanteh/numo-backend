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
  'banks:update':        { label: 'Actualizar movimientos (status, ERP, auxiliar)', module: 'Bancos' },
  'banks:movement:edit': { label: 'Editar datos del movimiento',    module: 'Bancos' },
  'banks:movement:categoria': { label: 'Recategorizar movimientos', module: 'Bancos' },
  'banks:config':        { label: 'Configurar bancos',              module: 'Bancos' },
  'banks:rules':        { label: 'Reglas de clasificación',         module: 'Bancos' },
  'banks:ficha':        { label: 'Registrar/eliminar fichas',       module: 'Bancos' },
  'banks:admin':        { label: 'Operaciones admin de bancos',     module: 'Bancos' },
  'banks:export':       { label: 'Exportar movimientos a Excel',    module: 'Bancos' },
  'banks:export:all':   { label: 'Exportar movimientos de cualquier usuario', module: 'Bancos' },
  'banks:erp:link':     { label: 'Vincular CxC del ERP directamente',        module: 'Bancos' },
  'banks:cobro':        { label: 'Aplicar cobro bancario',                   module: 'Bancos' },
  'banks:erp:unlink':   { label: 'Desvincular CxC del ERP',                 module: 'Bancos' },
  'account-plan:read':  { label: 'Ver catálogo contable',           module: 'Contabilidad' },
  'account-plan:write': { label: 'Editar catálogo contable',        module: 'Contabilidad' },
  'polizas:read':       { label: 'Ver pólizas contables',           module: 'Contabilidad' },
  'polizas:write':      { label: 'Crear y editar pólizas',          module: 'Contabilidad' },
  'polizas:admin':      { label: 'Administrar pólizas contabilizadas', module: 'Contabilidad' },
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
  'entities:write':     { label: 'Gestionar entidades fiscales (heredado, incluye programación de jobs)', module: 'Entidades' },
  'entities:edit':      { label: 'Editar entidades fiscales',       module: 'Entidades' },
  'entities:message':   { label: 'Enviar alerta de credenciales SAT', module: 'Entidades' },
  'users:manage':       { label: 'Administrar usuarios y roles',    module: 'Administración' },
};

async function seedRbac() {
  const { Role, Permission } = require('../../shared/models/postgres');

  // ── Permisos: agregar los nuevos, los existentes no se modifican ──────────
  // ignoreDuplicates usa ON CONFLICT DO NOTHING → idempotente en cada reinicio.
  const perms = Object.values(PERMISSIONS).map(key => ({
    key,
    label:  PERM_META[key]?.label  ?? key,
    module: PERM_META[key]?.module ?? 'General',
  }));
  const permsBefore = await Permission.count();
  // updateOnDuplicate: actualiza label y module si ya existe el key.
  // Esto corrige permisos que fueron sembrados antes de tener entrada en PERM_META
  // y quedaron con module='General' o label=key (fallback).
  await Permission.bulkCreate(perms, { updateOnDuplicate: ['label', 'module'] });
  const permsAfter = await Permission.count();
  const nuevosPerms = permsAfter - permsBefore;
  if (nuevosPerms > 0) {
    console.log(`[seed] ${nuevosPerms} permiso(s) nuevo(s) registrado(s) (total: ${permsAfter}).`);
  } else {
    console.log(`[seed] Catálogo de permisos: ${permsAfter} permisos (sin cambios, labels/módulos sincronizados).`);
  }

  // ── Roles del sistema: crear si no existen, nunca sobreescribir permisos ─────
  // En el primer arranque se crean con los permisos de rbac.js — o con los del
  // snapshot en disco (roles.snapshot.json) si hay uno más reciente, por si
  // esta base viene de un restore viejo y el snapshot ya tiene los permisos
  // que se editaron después desde la UI (ver role.service.js:_guardarSnapshot,
  // confirmado con el usuario 2026-07-28).
  // En reinicios normales (el rol YA existe) solo se sincroniza label e
  // isSystem, preservando cualquier cambio manual hecho a los permisos.
  const { leerSnapshot } = require('../domains/users/role.service');
  const snapshotPorValue = new Map(leerSnapshot().map(r => [r.value, r]));

  let createdRoles = 0;
  for (const [value, { label, permissions }] of Object.entries(ROLES)) {
    const snap = snapshotPorValue.get(value);
    snapshotPorValue.delete(value);   // lo que queda al final son roles personalizados
    const [, created] = await Role.findOrCreate({
      where:    { value },
      defaults: { label: snap?.label ?? label, permissions: snap?.permissions ?? permissions, isSystem: true },
    });
    if (!created) {
      // Sincronizar solo label e isSystem — nunca tocar permissions
      await Role.update({ label, isSystem: true }, { where: { value } });
    } else {
      createdRoles++;
    }
  }

  // Roles personalizados (creados desde la UI, no existen en rbac.js) que el
  // snapshot recuerda pero esta base no tiene — se restauran tal cual estaban.
  for (const [value, snap] of snapshotPorValue) {
    const [, created] = await Role.findOrCreate({
      where:    { value },
      defaults: { label: snap.label, permissions: snap.permissions, isSystem: false },
    });
    if (created) {
      createdRoles++;
      console.log(`[seed] Rol personalizado restaurado desde snapshot: ${value}`);
    }
  }

  if (createdRoles > 0) {
    console.log(`[seed] ${createdRoles} rol(es) creados (de sistema y/o restaurados desde snapshot).`);
  }
  console.log(`[seed] ${Object.keys(ROLES).length} rol(es) del sistema verificados (permisos preservados).`);
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

  // Siempre sembrar entidades fiscales conocidas — idempotente.
  const seedEntities = require('./seed-entities');
  await seedEntities();

  // Siempre sembrar catálogo de cuentas y centros de costo — idempotente.
  const seedAccountPlan = require('./seed-account-plan');
  await seedAccountPlan();

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
