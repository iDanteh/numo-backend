'use strict';

/**
 * scripts/migrate-netpay-v2.js — netpay-matching-v2 (Phase 4, design.md "Migration
 * Classification Rule" + "Migration / Rollout"). CLI wrapper over the pure classifier
 * (netpay-migracion.service.js, PR2) — this file owns I/O only (load, report, write), zero
 * classification logic lives here.
 *
 * Modes (mutually exclusive; --dry-run is the default when no flag is given):
 *
 *   node scripts/migrate-netpay-v2.js [--dry-run]
 *     Report-only. Zero writes. Prints how many NetpayMatch/NetpayReporte docs would map to
 *     each new state (and how many are already migrated / unmapped).
 *
 *   node scripts/migrate-netpay-v2.js --apply
 *     Drops the OLD {terminalID,dia} unique index on netpay_matches BEFORE writing
 *     (design.md: "It must run first, otherwise the second bucket's insert fails with
 *     E11000" — the NEW {terminalID,dia,bucket} index is already declared on the schema
 *     since PR1, this only removes the old one), then writes estatusMatch/estatus/
 *     motivoDiscrepancia/vinculo/estatusLegacy for every classified doc via one bulkWrite
 *     per collection. Aborts the whole run (zero writes at all, index included) if ANY
 *     record is unmapped.
 *
 *   node scripts/migrate-netpay-v2.js --revert
 *     Restores estatusMatch/estatus from estatusLegacy for every doc that has one (i.e. was
 *     migrated by --apply) and clears estatusLegacy. REFUSES to run (zero writes) if ANY doc
 *     with estatusLegacy:null holds a link (NetpayMatch.movementIdsConfirmados non-empty, or
 *     NetpayReporte.movementIdConfirmado set) — design.md: those are genuine v2-only records
 *     with no legacy state to restore to.
 *
 * This is a one-shot operational script, same pattern as every other file under scripts/
 * (crear-indices.js, migrate-severity.js, etc.) — it only opens its own mongoose connection
 * when run directly (`require.main === module`), so unit tests can require() its
 * pure/composable pieces without ever touching Mongo. ABSOLUTELY no test in this file (see
 * migrate-netpay-v2.test.js) connects to any live or production database — task 4.2 (running
 * --dry-run against a real synced snapshot) is explicitly reserved for the user.
 */

const mongoose = require('mongoose');
const NetpayMatch = require('../src/banks/domains/erp/NetpayMatch.model');
const NetpayReporte = require('../src/banks/domains/erp/NetpayReporte.model');
const BankMovement = require('../src/banks/domains/banks/BankMovement.model');
const {
  clasificarNetpayMatch, clasificarNetpayReporte,
} = require('../src/banks/domains/erp/netpay-migracion.service');

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/cfdi_comparator';

// Índice viejo que --apply debe dropear ANTES de escribir (design.md "Data Model": "The
// migration drops the old index. It must run first, otherwise the second bucket's insert
// fails with E11000") — el índice nuevo {terminalID,dia,bucket} ya lo declara el schema de
// NetpayMatch.model.js desde PR1, esto solo limpia el viejo.
const NOMBRE_COLECCION_MATCHES = 'netpay_matches';
const INDICE_VIEJO_SPEC = { terminalID: 1, dia: 1 };

// Estados v2 (idéntico enum en ambas colecciones, ver NetpayMatch.model.js/NetpayReporte.model.js)
// — un doc que ya tiene uno de estos valores no es legacy, no se reclasifica (permite correr
// --dry-run más de una vez de forma idempotente e informativa, sin falsos "no mapeado").
const ESTADOS_V2 = new Set([
  'confirmado_automatico',
  'pendiente_por_marca',
  'discrepancia',
  'resuelto_por_reporte',
  'rechazado',
  'resuelto_manual',
]);

const MODOS = ['--dry-run', '--apply', '--revert'];

function parseArgs(argv) {
  const flags = (argv ?? []).filter(a => a.startsWith('--'));
  const desconocidos = flags.filter(f => !MODOS.includes(f));
  if (desconocidos.length > 0) {
    throw new Error(`Flag(s) no reconocido(s): ${desconocidos.join(', ')}. Use --dry-run, --apply o --revert.`);
  }
  const modosPedidos = flags.filter(f => MODOS.includes(f));
  if (modosPedidos.length > 1) {
    throw new Error(`Solo se permite un modo a la vez, recibidos: ${modosPedidos.join(', ')}.`);
  }
  if (modosPedidos[0] === '--apply') return { modo: 'apply' };
  if (modosPedidos[0] === '--revert') return { modo: 'revert' };
  return { modo: 'dry-run' };
}

// Carga en UN solo Map<String(_id), BankMovement lean> todos los BankMovement referenciados
// por los docs legacy (movementIdsConfirmados de NetpayMatch + movementIdConfirmado de
// NetpayReporte) — una sola consulta para toda la corrida (mismo criterio de optimización
// que buscarCandidatosBatch/el extinto obtenerBandejaNetpay).
async function _cargarMovimientosPorId(ids) {
  const unicos = [...new Set((ids ?? []).filter(Boolean).map(String))];
  if (unicos.length === 0) return new Map();
  const movimientos = await BankMovement.find({ _id: { $in: unicos } }).lean();
  return new Map(movimientos.map(m => [String(m._id), m]));
}

// Clasifica todos los NetpayMatch: separa en yaMigrados (estatusMatch ya es un valor v2 — no
// se toca), mapeados (clasificados sin error por netpay-migracion.service.js) y noMapeados
// (el clasificador arrojó — estado desconocido/corrupto).
async function clasificarMatches(docs, movimientosPorId) {
  const yaMigrados = [];
  const mapeados = [];
  const noMapeados = [];
  for (const doc of docs) {
    if (ESTADOS_V2.has(doc.estatusMatch)) { yaMigrados.push(doc); continue; }
    try {
      const nuevo = clasificarNetpayMatch(doc, movimientosPorId);
      mapeados.push({ doc, nuevo });
    } catch (err) {
      noMapeados.push({ doc, error: err.message });
    }
  }
  return { yaMigrados, mapeados, noMapeados };
}

async function clasificarReportes(docs, movimientosPorId) {
  const yaMigrados = [];
  const mapeados = [];
  const noMapeados = [];
  for (const doc of docs) {
    if (ESTADOS_V2.has(doc.estatus)) { yaMigrados.push(doc); continue; }
    const movimiento = doc.movementIdConfirmado
      ? (movimientosPorId.get(String(doc.movementIdConfirmado)) ?? null)
      : null;
    try {
      const nuevo = clasificarNetpayReporte(doc, movimiento);
      mapeados.push({ doc, nuevo });
    } catch (err) {
      noMapeados.push({ doc, error: err.message });
    }
  }
  return { yaMigrados, mapeados, noMapeados };
}

// Reúne y clasifica ambas colecciones en un solo pase — usado tanto por --dry-run como por
// --apply (mismas clasificaciones; --apply solo agrega la escritura).
async function clasificarTodo() {
  const [matchesDocs, reportesDocs] = await Promise.all([
    NetpayMatch.find({}).lean(),
    NetpayReporte.find({}).lean(),
  ]);

  const idsReferenciados = [
    ...matchesDocs.flatMap(d => d.movementIdsConfirmados ?? []),
    ...reportesDocs.map(d => d.movementIdConfirmado).filter(Boolean),
  ];
  const movimientosPorId = idsReferenciados.length > 0
    ? await _cargarMovimientosPorId(idsReferenciados)
    : new Map();

  const matches = await clasificarMatches(matchesDocs, movimientosPorId);
  const reportes = await clasificarReportes(reportesDocs, movimientosPorId);
  return { matches, reportes };
}

function _resumenLinea(nombre, resultado) {
  const total = resultado.yaMigrados.length + resultado.mapeados.length + resultado.noMapeados.length;
  return `${nombre}: total=${total} yaMigrados=${resultado.yaMigrados.length} `
    + `mapeados=${resultado.mapeados.length} noMapeados=${resultado.noMapeados.length}`;
}

function imprimirResumen({ matches, reportes }) {
  console.log('── Resumen de clasificación (netpay-migracion.service.js) ──');
  console.log(_resumenLinea('NetpayMatch', matches));
  console.log(_resumenLinea('NetpayReporte', reportes));
  for (const { doc, error } of [...matches.noMapeados, ...reportes.noMapeados]) {
    console.log(`  NO MAPEADO  _id=${doc._id}  error=${error}`);
  }
  const totalNoMapeados = matches.noMapeados.length + reportes.noMapeados.length;
  if (totalNoMapeados === 0) {
    console.log('100% mapeado, 0 no mapeados.');
  } else {
    console.log(`${totalNoMapeados} registro(s) NO mapeado(s) — revisar antes de --apply.`);
  }
  return { totalNoMapeados };
}

async function runDryRun() {
  const resultado = await clasificarTodo();
  return imprimirResumen(resultado);
}

// Dropea el índice viejo {terminalID,dia} de netpay_matches si existe. Nunca revienta si ya
// fue dropeado (permite reintentar --apply después de un fallo a mitad de camino sin que
// esta parte sea el problema).
async function _dropearIndiceViejo() {
  const coleccion = mongoose.connection.db.collection(NOMBRE_COLECCION_MATCHES);
  const indices = await coleccion.indexes();
  const viejo = indices.find(
    i => JSON.stringify(i.key) === JSON.stringify(INDICE_VIEJO_SPEC) && i.unique,
  );
  if (!viejo) {
    console.log(`Índice viejo {terminalID,dia} no existe en ${NOMBRE_COLECCION_MATCHES} (ya dropeado o nunca existió) — nada que hacer.`);
    return { dropeado: false };
  }
  await coleccion.dropIndex(viejo.name);
  console.log(`Índice viejo dropeado: ${viejo.name}`);
  return { dropeado: true };
}

function _bulkOpsMatches(mapeados) {
  return mapeados.map(({ doc, nuevo }) => ({
    updateOne: { filter: { _id: doc._id }, update: { $set: nuevo } },
  }));
}

function _bulkOpsReportes(mapeados) {
  return mapeados.map(({ doc, nuevo }) => {
    // bucket:'general' no existe en el schema de NetpayReporte (concepto exclusivo de
    // NetpayMatch, ver design.md "Data Model") — Mongoose lo ignoraría en modo strict de
    // todos modos, pero se excluye acá explícitamente para no depender de ese comportamiento
    // implícito.
    const { bucket, ...campos } = nuevo;
    return { updateOne: { filter: { _id: doc._id }, update: { $set: campos } } };
  });
}

async function runApply() {
  const resultado = await clasificarTodo();
  const { totalNoMapeados } = imprimirResumen(resultado);
  if (totalNoMapeados > 0) {
    throw new Error(`--apply abortado: ${totalNoMapeados} registro(s) no mapeado(s). Corrija los datos o el clasificador antes de reintentar.`);
  }

  await _dropearIndiceViejo();

  const opsMatches = _bulkOpsMatches(resultado.matches.mapeados);
  const opsReportes = _bulkOpsReportes(resultado.reportes.mapeados);

  const resultados = { matches: { modificados: 0 }, reportes: { modificados: 0 } };
  if (opsMatches.length > 0) {
    const r = await NetpayMatch.bulkWrite(opsMatches);
    resultados.matches.modificados = r.modifiedCount ?? 0;
  }
  if (opsReportes.length > 0) {
    const r = await NetpayReporte.bulkWrite(opsReportes);
    resultados.reportes.modificados = r.modifiedCount ?? 0;
  }
  console.log(`NetpayMatch actualizados: ${resultados.matches.modificados}`);
  console.log(`NetpayReporte actualizados: ${resultados.reportes.modificados}`);
  return resultados;
}

// --revert: rechaza TODA la corrida (cero escrituras) si algún doc con estatusLegacy:null
// tiene un link real — design.md "Migration Classification Rule": "It refuses to run if any
// doc with estatusLegacy:null holds a link." Esos documentos son actividad v2 genuina (creada
// después del corte, con backend nuevo) sin ningún estado legacy al que revertir.
function _tieneLinkMatch(doc) {
  return (doc.movementIdsConfirmados ?? []).length > 0;
}

function _tieneLinkReporte(doc) {
  return doc.movementIdConfirmado != null;
}

async function _verificarGuardaRevert(matchesDocs, reportesDocs) {
  const matchesBloqueantes = matchesDocs.filter(d => d.estatusLegacy == null && _tieneLinkMatch(d));
  const reportesBloqueantes = reportesDocs.filter(d => d.estatusLegacy == null && _tieneLinkReporte(d));
  const total = matchesBloqueantes.length + reportesBloqueantes.length;
  if (total > 0) {
    throw new Error(
      `--revert abortado: ${total} documento(s) con estatusLegacy:null y un link activo `
      + `(${matchesBloqueantes.length} NetpayMatch, ${reportesBloqueantes.length} NetpayReporte) `
      + '— son actividad v2 genuina sin estado legacy al que revertir.',
    );
  }
}

function _bulkOpsRevertMatches(docs) {
  return docs
    .filter(d => d.estatusLegacy != null)
    .map(d => ({
      updateOne: {
        filter: { _id: d._id },
        update: { $set: { estatusMatch: d.estatusLegacy }, $unset: { estatusLegacy: '' } },
      },
    }));
}

function _bulkOpsRevertReportes(docs) {
  return docs
    .filter(d => d.estatusLegacy != null)
    .map(d => ({
      updateOne: {
        filter: { _id: d._id },
        update: { $set: { estatus: d.estatusLegacy }, $unset: { estatusLegacy: '' } },
      },
    }));
}

async function runRevert() {
  const [matchesDocs, reportesDocs] = await Promise.all([
    NetpayMatch.find({}).lean(),
    NetpayReporte.find({}).lean(),
  ]);

  await _verificarGuardaRevert(matchesDocs, reportesDocs);

  const opsMatches = _bulkOpsRevertMatches(matchesDocs);
  const opsReportes = _bulkOpsRevertReportes(reportesDocs);

  const resultados = { matches: { modificados: 0 }, reportes: { modificados: 0 } };
  if (opsMatches.length > 0) {
    const r = await NetpayMatch.bulkWrite(opsMatches);
    resultados.matches.modificados = r.modifiedCount ?? 0;
  }
  if (opsReportes.length > 0) {
    const r = await NetpayReporte.bulkWrite(opsReportes);
    resultados.reportes.modificados = r.modifiedCount ?? 0;
  }
  console.log(`--revert: NetpayMatch restaurados: ${resultados.matches.modificados}`);
  console.log(`--revert: NetpayReporte restaurados: ${resultados.reportes.modificados}`);
  return resultados;
}

async function main(argv = process.argv.slice(2)) {
  const { modo } = parseArgs(argv);
  await mongoose.connect(MONGODB_URI);
  console.log(`Conectado a MongoDB. Modo: --${modo}.`);
  try {
    if (modo === 'dry-run') return await runDryRun();
    if (modo === 'apply') return await runApply();
    return await runRevert();
  } finally {
    await mongoose.disconnect();
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
  clasificarMatches,
  clasificarReportes,
  clasificarTodo,
  imprimirResumen,
  runDryRun,
  runApply,
  runRevert,
  main,
  _cargarMovimientosPorId,
  _dropearIndiceViejo,
  _bulkOpsMatches,
  _bulkOpsReportes,
  _tieneLinkMatch,
  _tieneLinkReporte,
  _verificarGuardaRevert,
  _bulkOpsRevertMatches,
  _bulkOpsRevertReportes,
  ESTADOS_V2,
  INDICE_VIEJO_SPEC,
  NOMBRE_COLECCION_MATCHES,
};
