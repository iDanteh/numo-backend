require('dotenv').config();
const mongoose = require('mongoose');
const User = require('../models/User');

const SEED_USERS = [
  {
    name: 'Administrador',
    email: 'admin@cfdi.local',
    password: 'Admin1234!',
    role: 'admin',
  },
  {
    name: 'Contador',
    email: 'contador@cfdi.local',
    password: 'Contador1234!',
    role: 'contador',
  },
];

const run = async () => {
  const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017/cfdi_comparator';
  await mongoose.connect(uri);
  console.log('MongoDB conectado:', uri);

  for (const data of SEED_USERS) {
    const exists = await User.findOne({ email: data.email });
    if (exists) {
      console.log(`  ⚠  Ya existe: ${data.email} — omitido`);
      continue;
    }
    await User.create(data);
    console.log(`  ✓  Usuario creado: ${data.email}  (rol: ${data.role})`);
  }

  console.log('\nCredenciales de acceso:');
  SEED_USERS.forEach(u => {
    console.log(`  Email: ${u.email}   Password: ${u.password}   Rol: ${u.role}`);
  });

  await mongoose.disconnect();
  process.exit(0);
};

run().catch(err => {
  console.error('Error en seed:', err.message);
  process.exit(1);
});
