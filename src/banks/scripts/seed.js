'use strict';

/**
 * seed.js — Genera el usuario administrador por defecto.
 *
 * - Seguro de ejecutar múltiples veces (idempotente).
 * - Si ya existe un usuario con rol 'admin' no hace nada.
 * - El auth0Sub se asigna como placeholder 'seed:<email>' hasta que el
 *   usuario haga su primer login con Auth0; en ese momento findOrCreate()
 *   reemplaza el placeholder con el sub real.
 *
 * Variables de entorno:
 *   SEED_ADMIN_EMAIL  — email del admin a pre-registrar
 *   SEED_ADMIN_NOMBRE — nombre del admin (opcional)
 */

require('dotenv').config();

const mongoose = require('mongoose');
const User     = require('../domains/users/User.model');

async function seed() {
  const adminEmail  = process.env.SEED_ADMIN_EMAIL;
  const adminNombre = process.env.SEED_ADMIN_NOMBRE;

  const existing = await User.findOne({ role: 'admin' });

  if (existing) {
    console.log(`[seed] Admin ya existe (${existing.email || existing.auth0Sub}). Sin cambios.`);
    return;
  }

  await User.create({
    auth0Sub:  `seed:${adminEmail}`,
    nombre:    adminNombre,
    email:     adminEmail,
    role:      'admin',
    isActive:  true,
  });

  console.log(`[seed] Usuario admin creado → ${adminEmail}`);
  console.log(`[seed] Cuando ese email inicie sesión con Auth0, quedará vinculado automáticamente.`);
}

// ── Ejecución directa: node src/scripts/seed.js ───────────────────────────
if (require.main === module) {
  mongoose.connect(process.env.MONGODB_URI)
    .then(async () => {
      await seed();
      await mongoose.disconnect();
      process.exit(0);
    })
    .catch((err) => {
      console.error('[seed] Error:', err.message);
      process.exit(1);
    });
}

module.exports = seed;
