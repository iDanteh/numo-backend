'use strict';

/**
 * scripts/backfill-netpay-comisiones-config.js — netpay-comisiones-config (2026-10-07,
 * pedido explícito del usuario): sincroniza retroactivamente los reportes Netpay YA cargados
 * antes de que existiera netpay-comision-sync.service.js, hacia Configuraciones Globales
 * (sección 'netpay-comisiones'). Sin este backfill, los reportes históricos quedarían fuera
 * de la sección nueva hasta que alguien subiera un reporte más — y aun así, eso solo cubriría
 * la tasa MÁS RECIENTE vista, perdiendo el historial real de cambios anteriores.
 *
 * Modos (mutuamente excluyentes; --dry-run es el default):
 *
 *   node scripts/backfill-netpay-comisiones-config.js [--dry-run]
 *     Reporta cuántos NetpayReporte hay y cuántas categorías (tarjeta/sucursal) distintas se
 *     detectarían en total. Cero escrituras.
 *
 *   node scripts/backfill-netpay-comisiones-config.js --apply
 *     Recorre TODOS los NetpayReporte (sin filtrar por estatus/eliminado — la comisión es un
 *     hecho factual, mismo criterio que netpay-comision.service.js), ordenados por
 *     fechaMovimiento ASCENDENTE (para que el historial de ConfigAuditLog quede en el orden
 *     cronológico REAL en que se vieron las tasas, no el orden arbitrario de Mongo), y llama
 *     sincronizarComisiones() para cada uno, SECUENCIAL — nunca en paralelo: el guard de
 *     "valor sin cambio" dentro de sincronizarComisiones necesita comparar contra el estado
 *     recién actualizado por el reporte anterior, no uno viejo leído en paralelo.
 *
 * Único script de esta carpeta que abre conexión a Mongo (reportes) Y a Postgres
 * (Configuraciones Globales, vía global-config.service.js) a la vez.
 */

const mongoose = require('mongoose');
const NetpayReporte = require('../src/banks/domains/erp/NetpayReporte.model');
const { sincronizarComisiones, _categoriasDeReporte } = require('../src/banks/domains/erp/netpay-comision-sync.service');
const { connectPostgres, disconnectPostgres } = require('../src/config/database.postgres');

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/cfdi_comparator';

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

// Orden cronológico REAL (fechaMovimiento ascendente) — crítico para que el historial de
// ConfigAuditLog refleje cuándo cambió cada tasa de verdad, no el orden arbitrario de Mongo.
async function _reportesEnOrden() {
  return NetpayReporte.find({}).sort({ fechaMovimiento: 1 }).lean();
}

function _contarCategorias(reportes) {
  const claves = new Set();
  for (const reporte of reportes) {
    for (const clave of _categoriasDeReporte(reporte).keys()) claves.add(clave);
  }
  return claves.size;
}

async function runDryRun() {
  const reportes = await _reportesEnOrden();
  const totalCategorias = _contarCategorias(reportes);
  console.log('── Resumen backfill comisiones Netpay → Configuraciones Globales ──');
  console.log(`Reportes Netpay totales: ${reportes.length}`);
  console.log(`Categorías distintas (tarjeta/sucursal) detectadas: ${totalCategorias}`);
  if (reportes.length === 0) {
    console.log('Nada que hacer.');
  } else {
    console.log('Correr con --apply para sincronizar estos reportes contra Configuraciones Globales (orden cronológico).');
  }
  return { totalReportes: reportes.length, totalCategorias };
}

async function runApply() {
  const reportes = await _reportesEnOrden();
  console.log('── Resumen backfill comisiones Netpay → Configuraciones Globales ──');
  console.log(`Reportes Netpay a sincronizar (orden cronológico): ${reportes.length}`);

  let procesados = 0;
  for (const reporte of reportes) {
    // eslint-disable-next-line no-await-in-loop
    await sincronizarComisiones(reporte);
    procesados += 1;
    if (procesados % 10 === 0 || procesados === reportes.length) {
      console.log(`  ${procesados}/${reportes.length} reportes procesados...`);
    }
  }

  console.log(`Listo — ${procesados} reporte(s) sincronizados.`);
  return { procesados };
}

async function main(argv = process.argv.slice(2)) {
  const { modo } = parseArgs(argv);
  await mongoose.connect(MONGODB_URI);
  await connectPostgres();
  console.log(`Conectado a MongoDB y Postgres. Modo: --${modo}.`);
  try {
    if (modo === 'dry-run') return await runDryRun();
    return await runApply();
  } finally {
    await mongoose.disconnect();
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
  _reportesEnOrden,
  _contarCategorias,
  runDryRun,
  runApply,
  main,
};
