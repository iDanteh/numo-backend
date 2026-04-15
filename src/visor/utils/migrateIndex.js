/**
 * migrateIndex.js — Elimina el índice uuid_1 (único simple) y lo reemplaza
 * por uuid_1_source_1 (único compuesto). Ejecutar UNA sola vez.
 * Uso: node src/utils/migrateIndex.js
 */
require('dotenv').config();
const mongoose = require('mongoose');

async function run() {
  await mongoose.connect(process.env.MONGODB_URI);
  const db = mongoose.connection.db;
  const col = db.collection('cfdis');

  // Listar índices actuales
  const indexes = await col.indexes();
  console.log('Índices actuales:', indexes.map(i => i.name));

  // Eliminar índice simple si existe
  if (indexes.find(i => i.name === 'uuid_1')) {
    await col.dropIndex('uuid_1');
    console.log('✓ Eliminado índice uuid_1');
  } else {
    console.log('— Índice uuid_1 no encontrado (ya migrado)');
  }

  // Crear índice compuesto
  await col.createIndex({ uuid: 1, source: 1 }, { unique: true });
  console.log('✓ Creado índice uuid_1_source_1 (único compuesto)');

  await mongoose.disconnect();
  console.log('Migración completada.');
}

run().catch(err => { console.error(err); process.exit(1); });
