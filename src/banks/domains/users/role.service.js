'use strict';

const fs   = require('fs');
const path = require('path');
const { NotFoundError, ConflictError, BadRequestError } = require('../../../shared/errors/AppError');
const { invalidate } = require('../../../shared/services/rbac-store');
const { getIo }      = require('../../shared/socket');

// Importación deferida para evitar dependencia circular durante el bootstrap
function db() {
  return require('../../../shared/models/postgres');
}

// ── Respaldo en disco de roles (sistema + personalizados) ──────────────────────
// rbac.js solo trae los roles "de fábrica"; un rol personalizado creado desde
// la UI (o un cambio de permisos a uno de sistema) vive SOLO en Postgres. Si
// alguna vez se restaura la base desde una copia vieja (ya pasó en este
// proyecto — ver memoria de sesión sobre topología BD local/producción), ese
// cambio se pierde sin que sea culpa de seedRbac(). Este snapshot es la red de
// seguridad: se reescribe en cada create/update/delete de rol, y seedRbac()
// lo usa como fuente de verdad al arrancar — así un restore viejo se
// autocorrige con lo último que se guardó aquí, en vez de quedarse en lo que
// trajo la copia. Confirmado con el usuario 2026-07-28.
const SNAPSHOT_PATH = path.join(__dirname, '../../../shared/config/roles.snapshot.json');

async function _guardarSnapshot() {
  const { Role } = db();
  const roles = await Role.findAll({ raw: true, order: [['value', 'ASC']] });
  const data = roles.map(r => ({ value: r.value, label: r.label, permissions: r.permissions, isSystem: r.isSystem }));
  try {
    fs.writeFileSync(SNAPSHOT_PATH, JSON.stringify(data, null, 2) + '\n', 'utf8');
  } catch (err) {
    // No debe tumbar la operación (crear/editar rol) por un problema de disco
    // (ej. filesystem read-only en algunos despliegues) — solo se pierde la
    // red de seguridad, no el cambio real en la base.
    // eslint-disable-next-line no-console
    console.warn('[role.service] No se pudo escribir roles.snapshot.json:', err.message);
  }
}

/** Lee el snapshot en disco, o [] si no existe todavía (primera vez). */
function leerSnapshot() {
  try {
    return JSON.parse(fs.readFileSync(SNAPSHOT_PATH, 'utf8'));
  } catch {
    return [];
  }
}

// ── Roles ─────────────────────────────────────────────────────────────────────

async function listRoles() {
  const { Role } = db();
  return Role.findAll({ order: [['value', 'ASC']] });
}

async function getRoleByValue(value) {
  const { Role } = db();
  return Role.findByPk(value, { raw: true });
}

async function createRole({ value, label, permissions }) {
  const { Role } = db();
  if (!value || !/^[a-z][a-z0-9_-]*$/.test(value)) {
    throw new BadRequestError(
      'El identificador solo puede contener minúsculas, números, guiones y guiones bajos, y debe empezar con letra.',
    );
  }
  const exists = await Role.findByPk(value);
  if (exists) throw new ConflictError(`El rol '${value}' ya existe.`);
  const role = await Role.create({ value, label, permissions: permissions ?? [], isSystem: false });
  invalidate();
  await _guardarSnapshot();
  const io = getIo();
  if (io) io.emit('role:definition:updated', { role: value });
  return role;
}

async function updateRole(value, updates) {
  const { Role, User } = db();
  const role = await Role.findByPk(value);
  if (!role) throw new NotFoundError(`Rol '${value}' no encontrado.`);

  if (updates.label       !== undefined) role.label       = updates.label;
  if (updates.permissions !== undefined) {
    role.permissions = updates.permissions;
    // Forzar el dirty flag: Sequelize puede no detectar la reasignación de un
    // array JSONB como cambio si la referencia es distinta pero el contenido igual.
    role.changed('permissions', true);
  }

  await role.save();
  invalidate();
  await _guardarSnapshot();

  const io = getIo();
  if (io) {
    if (updates.permissions !== undefined) {
      // Notifica a los usuarios con ese rol para que actualicen sus permisos en sesión
      const affected = await User.findAll({
        where: { role: value },
        attributes: ['auth0Sub'],
        raw: true,
      });
      for (const u of affected) {
        const sub = u.auth0Sub ?? u.auth0_sub;
        if (sub) {
          io.to(`user:${sub}`).emit('role:updated', { role: value, permissions: role.permissions });
        }
      }
    }

    // Broadcast siempre (label o permissions) para refrescar la vista de gestión
    io.emit('role:definition:updated', { role: value });
  }

  return role;
}

async function deleteRole(value) {
  const { Role, User } = db();
  const role = await Role.findByPk(value);
  if (!role) throw new NotFoundError(`Rol '${value}' no encontrado.`);
  if (role.isSystem) {
    throw new BadRequestError('No se pueden eliminar roles del sistema. Puedes editar sus permisos.');
  }
  const count = await User.count({ where: { role: value } });
  if (count > 0) {
    throw new ConflictError(
      `No se puede eliminar: ${count} usuario(s) tienen este rol. Reasígnales otro rol primero.`,
    );
  }
  await role.destroy();
  invalidate();
  await _guardarSnapshot();
  const io = getIo();
  if (io) io.emit('role:definition:updated', { role: value, deleted: true });
}

// ── Permisos ──────────────────────────────────────────────────────────────────

async function listPermissions() {
  const { Permission } = db();
  return Permission.findAll({ order: [['module', 'ASC'], ['key', 'ASC']] });
}

async function createPermission({ key, label, module: mod }) {
  const { Permission } = db();
  if (!key || !/^[a-z][a-z0-9_-]*:[a-z][a-z0-9_-]*$/.test(key)) {
    throw new BadRequestError(
      'La clave debe tener el formato módulo:acción (p. ej. ventas:read). Solo minúsculas, números y guiones.',
    );
  }
  const exists = await Permission.findByPk(key);
  if (exists) throw new ConflictError(`El permiso '${key}' ya existe.`);
  return Permission.create({ key, label, module: mod ?? 'General' });
}

async function deletePermission(key) {
  const { Permission, Role } = db();
  const perm = await Permission.findByPk(key);
  if (!perm) throw new NotFoundError(`Permiso '${key}' no encontrado.`);
  const roles = await Role.findAll({ raw: true });
  const inUse = roles.filter(r => Array.isArray(r.permissions) && r.permissions.includes(key));
  if (inUse.length > 0) {
    throw new ConflictError(
      `El permiso está asignado a: ${inUse.map(r => r.label).join(', ')}. Quítalo de esos roles primero.`,
    );
  }
  await perm.destroy();
}

module.exports = {
  listRoles, getRoleByValue, createRole, updateRole, deleteRole,
  listPermissions, createPermission, deletePermission,
  leerSnapshot,
};
