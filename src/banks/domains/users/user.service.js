'use strict';

/**
 * users/user.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Lógica de negocio para gestión de usuarios.
 * Delegada a PostgreSQL mediante user.repository.js.
 *
 * Flujo findOrCreate (Auth0):
 *   1. Buscar por auth0Sub → actualizar lastLogin si existe.
 *   2. Si no existe, intentar reclamar un registro pre-sembrado (seed:<email>).
 *   3. Si tampoco, crear de forma atómica con findOrCreate de Sequelize.
 */

const userRepo  = require('./repositories/user.repository');
const { NotFoundError, BadRequestError } = require('../../../shared/errors/AppError');
const { getIo }    = require('../../shared/socket');
const rbacStore    = require('../../../shared/services/rbac-store');

/**
 * Resuelve (o crea) el usuario a partir de los claims del JWT de Auth0.
 * Maneja race conditions mediante la restricción UNIQUE de auth0_sub en Postgres.
 */
async function findOrCreate({ auth0Sub, nombre, email }) {
  // 1. Búsqueda rápida por sub
  let user = await userRepo.findByAuth0Sub(auth0Sub);

  if (user) {
    return userRepo.touchLogin(user.id, nombre);
  }

  // 2. Intentar reclamar un usuario pre-sembrado con ese email
  if (email) {
    const claimed = await userRepo.claimSeedUser(email, auth0Sub, nombre);
    if (claimed) return claimed;
  }

  // 3. Crear nuevo de forma atómica (UNIQUE sobre auth0Sub absorbe la race condition)
  const { user: newUser } = await userRepo.findOrCreate({ auth0Sub, nombre, email });
  return newUser;
}

async function listUsers() {
  return userRepo.findAll();
}

async function updateRole(id, role) {
  if (!(await rbacStore.roleExists(role))) {
    throw new BadRequestError(`Rol inválido: '${role}'`);
  }
  const user = await userRepo.updateRole(id, role);
  if (!user) throw new NotFoundError('Usuario');

  // Notificar al usuario por socket con el nuevo rol Y sus permisos,
  // para evitar un round-trip HTTP adicional en el cliente.
  const io = getIo();
  if (io && user.auth0Sub) {
    // Use DB permissions (rbac-store) so custom role edits are reflected correctly
    const permissions = await rbacStore.getPermissions(user.role);
    io.to(`user:${user.auth0Sub}`).emit('role:updated', { role: user.role, permissions });
  }

  return user;
}

async function toggleActive(id) {
  const user = await userRepo.findById(id);
  if (!user) throw new NotFoundError('Usuario');
  const updated = await userRepo.updateActive(id, !user.isActive);
  return updated;
}

// Reemplaza la lista COMPLETA de empresas fijas del usuario (no es
// add/remove incremental — el caller manda el array final deseado).
async function updateEmpresas(id, empresaRfcs) {
  const { Entity } = require('../../../shared/models/postgres');
  const rfcs = [...new Set((empresaRfcs ?? []).map(r => String(r).toUpperCase().trim()).filter(Boolean))];

  let empresas = [];
  if (rfcs.length) {
    const { Op } = require('sequelize');
    const rows = await Entity.findAll({ where: { rfc: { [Op.in]: rfcs } }, attributes: ['rfc', 'nombre'], raw: true });
    const faltantes = rfcs.filter(rfc => !rows.some(r => r.rfc === rfc));
    if (faltantes.length) throw new BadRequestError(`No existe ninguna empresa con RFC: ${faltantes.join(', ')}.`);
    empresas = rows.map(r => ({ rfc: r.rfc, nombre: r.nombre }));
  }

  const user = await userRepo.updateEmpresas(id, rfcs);
  if (!user) throw new NotFoundError('Usuario');

  // Notifica al usuario en sesión con sus empresas asignadas — mismo evento
  // que usa updateRole() para que el frontend no necesite dos listeners.
  const io = getIo();
  if (io && user.auth0Sub) {
    const permissions = await rbacStore.getPermissions(user.role);
    io.to(`user:${user.auth0Sub}`).emit('role:updated', { role: user.role, permissions, empresas });
  }

  return user;
}

module.exports = { findOrCreate, listUsers, updateRole, toggleActive, updateEmpresas };
