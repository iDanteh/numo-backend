'use strict';

/**
 * users/repositories/user.repository.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Acceso a datos de usuarios en PostgreSQL.
 *
 * Responsabilidad única: persistencia — sin lógica de negocio.
 * El servicio (user.service.js) es quien decide qué llamar y en qué orden.
 */

const { User } = require('../../../../shared/models/postgres');

/**
 * Busca un usuario por su auth0Sub.
 */
async function findByAuth0Sub(auth0Sub) {
  return User.findOne({ where: { auth0Sub } });
}

/**
 * Busca un usuario pre-sembrado (placeholder auth0Sub = 'seed:<email>').
 * Si existe, lo actualiza con el sub real de Auth0 de forma atómica.
 * Devuelve el registro actualizado o null si no había ninguno que reclamar.
 */
async function claimSeedUser(email, auth0Sub, nombre) {
  const [count] = await User.update(
    {
      auth0Sub,
      lastLogin: new Date(),
      ...(nombre ? { nombre } : {}),
    },
    { where: { email, auth0Sub: `seed:${email}` } },
  );
  if (!count) return null;
  return User.findOne({ where: { auth0Sub } });
}

/**
 * Crea un usuario o lo devuelve si ya existe (por auth0Sub).
 * Sequelize.findOrCreate es atómico en Postgres gracias al UNIQUE sobre auth0Sub.
 * @returns {{ user: User, created: boolean }}
 */
async function findOrCreate({ auth0Sub, nombre, email }) {
  const [user, created] = await User.findOrCreate({
    where:    { auth0Sub },
    defaults: { auth0Sub, nombre, email, role: 'tienda' },
  });
  return { user, created };
}

/**
 * Actualiza lastLogin (y nombre si se proporciona).
 * Devuelve el usuario refresco desde la BD.
 */
async function touchLogin(id, nombre) {
  const update = { lastLogin: new Date() };
  if (nombre) update.nombre = nombre;
  await User.update(update, { where: { id } });
  return User.findByPk(id);
}

async function findAll() {
  return User.findAll({ order: [['created_at', 'DESC']] });
}

async function findById(id) {
  return User.findByPk(id);
}

async function updateRole(id, role) {
  await User.update({ role }, { where: { id } });
  return User.findByPk(id);
}

async function updateActive(id, isActive) {
  await User.update({ isActive }, { where: { id } });
  return User.findByPk(id);
}

async function create(data) {
  return User.create(data);
}

module.exports = {
  findByAuth0Sub,
  claimSeedUser,
  findOrCreate,
  touchLogin,
  findAll,
  findById,
  updateRole,
  updateActive,
  create,
};
