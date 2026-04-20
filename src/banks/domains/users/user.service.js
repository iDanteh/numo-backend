'use strict';

const User = require('./User.model');
const { NotFoundError, BadRequestError } = require('../../shared/errors/AppError');
const { getIo } = require('../../shared/socket');

const ROLES_VALIDOS = ['admin', 'contabilidad', 'cobranza', 'tienda'];

/**
 * Busca al usuario por auth0Sub; si no existe lo crea con rol 'tienda'.
 * Actualiza nombre, email y lastLogin en cada llamada.
 * Usa operaciones atómicas para evitar la condición de carrera que
 * produce E11000 cuando dos peticiones simultáneas intentan crear el mismo usuario.
 */
async function findOrCreate({ auth0Sub, nombre, email }) {
  // 1. Búsqueda normal por sub de Auth0
  let user = await User.findOne({ auth0Sub });

  // 2. Si no existe por sub, intenta reclamar un registro pre-sembrado
  //    de forma atómica (evita race condition en el save del sub).
  if (!user && email) {
    user = await User.findOneAndUpdate(
      { email, auth0Sub: `seed:${email}` },
      { $set: { auth0Sub, lastLogin: new Date(), ...(nombre && { nombre }) } },
      { new: true },
    );
  }

  if (!user) {
    // 3. Crear nuevo usuario; en caso de race condition (E11000) releer el doc.
    try {
      user = await User.create({ auth0Sub, nombre, email, role: 'tienda' });
    } catch (err) {
      if (err.code === 11000) {
        user = await User.findOne({ auth0Sub });
      } else {
        throw err;
      }
    }
  } else if (!user.auth0Sub || user.auth0Sub === `seed:${email}`) {
    // Ya fue actualizado por el findOneAndUpdate; nada más que hacer.
  } else {
    user.lastLogin = new Date();
    if (nombre) user.nombre = nombre;
    if (email && !user.email) user.email = email;
    await user.save();
  }

  return user;
}

async function listUsers() {
  return User.find().sort({ createdAt: -1 }).lean();
}

async function updateRole(id, role) {
  if (!ROLES_VALIDOS.includes(role)) {
    throw new BadRequestError(`Rol inválido. Opciones: ${ROLES_VALIDOS.join(', ')}`);
  }
  const user = await User.findByIdAndUpdate(id, { role }, { new: true });
  if (!user) throw new NotFoundError('Usuario');

  // Notificar al usuario en tiempo real si está conectado por socket
  const io = getIo();
  if (io && user.auth0Sub) {
    io.to(`user:${user.auth0Sub}`).emit('role:updated', { role: user.role });
  }

  return user;
}

async function toggleActive(id) {
  const user = await User.findById(id);
  if (!user) throw new NotFoundError('Usuario');
  user.isActive = !user.isActive;
  await user.save();
  return user;
}

module.exports = { findOrCreate, listUsers, updateRole, toggleActive };
