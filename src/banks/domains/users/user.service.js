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
const { getIo }  = require('../../shared/socket');
const { ROLES }  = require('../../../shared/config/rbac');

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
  if (!ROLES[role]) {
    throw new BadRequestError(`Rol inválido. Opciones: ${Object.keys(ROLES).join(', ')}`);
  }
  const user = await userRepo.updateRole(id, role);
  if (!user) throw new NotFoundError('Usuario');

  // Notificar al usuario por socket si está conectado
  const io = getIo();
  if (io && user.auth0Sub) {
    io.to(`user:${user.auth0Sub}`).emit('role:updated', { role: user.role });
  }

  return user;
}

async function toggleActive(id) {
  const user = await userRepo.findById(id);
  if (!user) throw new NotFoundError('Usuario');
  const updated = await userRepo.updateActive(id, !user.isActive);
  return updated;
}

module.exports = { findOrCreate, listUsers, updateRole, toggleActive };
