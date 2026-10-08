'use strict';

/**
 * scripts/reset-netpay-comisiones-config.js — netpay-comisiones-config (2026-10-07, pedido
 * explícito del usuario — fix de agrupación por sucursal): borra la sección VIEJA única
 * 'netpay-comisiones' (y sus GlobalConfig/ConfigAuditLog) ya reemplazada por las 2 secciones
 * nuevas ('netpay-comisiones-tarjetas'/'netpay-comisiones-sucursales', ver
 * seed-global-config-netpay-comisiones.js). La versión vieja agrupaba sucursales por storeId
 * (una terminal, no el local físico) — wipe+rebuild en vez de migrar: son datos de un solo día,
 * sin historial real de cambios todavía, así que no vale la pena consolidar a mano los
 * duplicados (ej. 2 storeId distintos para la misma sucursal "AV FERROCARRIL 802"). Después de
 * correr este script, correr el seed actualizado + scripts/backfill-netpay-comisiones-config.js
 * --apply para repoblar limpio con la agrupación correcta.
 *
 * NO toca Mongo — solo Postgres (Configuraciones Globales).
 *
 * Modos (mutuamente excluyentes; --dry-run es el default):
 *
 *   node scripts/reset-netpay-comisiones-config.js [--dry-run]
 *     Cuenta cuántos GlobalConfig y ConfigAuditLog hay bajo la sección vieja. Cero borrados.
 *
 *   node scripts/reset-netpay-comisiones-config.js --apply
 *     Borra, en una sola transacción (todo-o-nada): primero los ConfigAuditLog de esos
 *     configs, después los GlobalConfig de la sección, y por último la ConfigSection misma.
 *
 * Idempotente: si la sección vieja 'netpay-comisiones' no existe (ya se corrió, o este
 * ambiente arrancó directo con las 2 secciones nuevas), informa "nada que limpiar" y termina
 * sin error.
 */

require('dotenv').config();

const { ConfigSection, GlobalConfig, ConfigAuditLog } = require('../src/shared/models/postgres');
const { sequelize, connectPostgres, disconnectPostgres } = require('../src/config/database.postgres');

const CLAVE_SECCION_VIEJA = 'netpay-comisiones';

const MODOS = ['--dry-run', '--apply'];

function parseArgs(argv) {
  const flags = (argv ?? []).filter(a => a.startsWith('--'));
  const desconocidos = flags.filter(f => !MODOS.includes(f));
  if (desconocidos.length > 0) {
    throw new Error(`Flag(s) no reconocido(s): ${desconocidos.join(', ')}. Use --dry-run o --apply.`);
  }
  const modosPedidos = flags.filter(f => MODOS.includes(f));
  if (modosPedidos.length > 1) {
    throw new Error(`Solo se permite un modo a la vez, recibidos: ${modosPedidos.join(', ')}.`);
  }
  return { modo: modosPedidos[0] === '--apply' ? 'apply' : 'dry-run' };
}

async function _buscarSeccionVieja() {
  return ConfigSection.findOne({ where: { clave: CLAVE_SECCION_VIEJA } });
}

async function runDryRun() {
  const seccion = await _buscarSeccionVieja();
  if (!seccion) {
    console.log(`── Reset sección vieja '${CLAVE_SECCION_VIEJA}' ──`);
    console.log('No existe — nada que limpiar (ya se corrió, o este ambiente arrancó directo con las 2 secciones nuevas).');
    return { existeSeccion: false, totalConfigs: 0, totalAudits: 0 };
  }

  const configs = await GlobalConfig.findAll({ where: { sectionId: seccion.id }, attributes: ['id'] });
  const configIds = configs.map(c => c.id);
  const totalAudits = configIds.length === 0
    ? 0
    : await ConfigAuditLog.count({ where: { configId: configIds } });

  console.log(`── Reset sección vieja '${CLAVE_SECCION_VIEJA}' (id=${seccion.id}) ──`);
  console.log(`GlobalConfig a borrar: ${configs.length}`);
  console.log(`ConfigAuditLog a borrar: ${totalAudits}`);
  console.log('Correr con --apply para borrar (transacción todo-o-nada), y después el seed + backfill actualizados.');
  return { existeSeccion: true, totalConfigs: configs.length, totalAudits };
}

async function runApply() {
  const seccion = await _buscarSeccionVieja();
  if (!seccion) {
    console.log(`── Reset sección vieja '${CLAVE_SECCION_VIEJA}' ──`);
    console.log('No existe — nada que limpiar.');
    return { existeSeccion: false, configsBorrados: 0, auditsBorrados: 0 };
  }

  return sequelize.transaction(async (t) => {
    const configs = await GlobalConfig.findAll({ where: { sectionId: seccion.id }, attributes: ['id'], transaction: t });
    const configIds = configs.map(c => c.id);

    const auditsBorrados = configIds.length === 0
      ? 0
      : await ConfigAuditLog.destroy({ where: { configId: configIds }, transaction: t });
    const configsBorrados = await GlobalConfig.destroy({ where: { sectionId: seccion.id }, transaction: t });
    await seccion.destroy({ transaction: t });

    console.log(`── Reset sección vieja '${CLAVE_SECCION_VIEJA}' ──`);
    console.log(`ConfigAuditLog borrados: ${auditsBorrados}`);
    console.log(`GlobalConfig borrados: ${configsBorrados}`);
    console.log('ConfigSection vieja borrada.');
    return { existeSeccion: true, configsBorrados, auditsBorrados };
  });
}

async function main(argv = process.argv.slice(2)) {
  const { modo } = parseArgs(argv);
  await connectPostgres();
  console.log(`Conectado a Postgres. Modo: --${modo}.`);
  try {
    if (modo === 'dry-run') return await runDryRun();
    return await runApply();
  } finally {
    await disconnectPostgres();
  }
}

if (require.main === module) {
  main().catch(err => {
    console.error('Error:', err.message);
    process.exit(1);
  });
}

module.exports = {
  parseArgs,
  _buscarSeccionVieja,
  runDryRun,
  runApply,
  main,
  CLAVE_SECCION_VIEJA,
};
