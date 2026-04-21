'use strict';

const { NotFoundError, ConflictError, BadRequestError } = require('../../../shared/errors/AppError');
const { invalidate } = require('../../../shared/services/rbac-store');

// Importación deferida para evitar dependencia circular durante el bootstrap
function db() {
  return require('../../../shared/models/postgres');
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
  return role;
}

async function updateRole(value, updates) {
  const { Role } = db();
  const role = await Role.findByPk(value);
  if (!role) throw new NotFoundError(`Rol '${value}' no encontrado.`);
  if (updates.label       !== undefined) role.label       = updates.label;
  if (updates.permissions !== undefined) role.permissions = updates.permissions;
  await role.save();
  invalidate();
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
};
