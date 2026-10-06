'use strict';

/**
 * scripts/backfill-netpay-kore-cache.js (2026-10-06, pedido explícito del usuario).
 *
 * `NetpayReporte.folios[].koreCache` (serie/folio, folio fiscal, cuenta — ver
 * NetpayReporte.model.js) ya se persiste desde el primer commit de este feature, PERO solo se
 * llena cuando alguien abre el folio a mano en el panel, o automáticamente antes de exportar el
 * Excel de ESE reporte puntual (netpay-reporte.service.js#consultarFoliosPendientes, "Fix 2a").
 * Un reporte ya identificado (vinculado a un BankMovement) cuyo Excel nunca se exportó, o se
 * exportó ANTES de ese fix (2026-09-25), puede seguir con `koreCache:null` en sus folios.
 *
 * Este script recorre TODOS los NetpayReporte con `movementIdConfirmado` seteado (= ya
 * identificados contra un movimiento bancario real, sin importar el estatus exacto v2) y, para
 * cada uno con folios pendientes, reusa `consultarFoliosPendientes()` tal cual — mismo backoff
 * ante 429, misma concurrencia de 5 en lotes con pausa de 400ms entre lotes, mismo criterio de
 * fallo parcial (un folio que no resuelve no aborta el resto). Los reportes se procesan
 * SECUENCIALMENTE entre sí (uno a la vez) para no multiplicar la concurrencia ya calibrada
 * dentro de cada reporte y terminar saturando a Kore entre muchos reportes en simultáneo.
 *
 * Reportes soft-eliminados (`eliminado:true`) SÍ se incluyen a propósito: el soft-delete oculta
 * el reporte de las listas pero el vínculo con el movimiento sigue siendo válido (nunca se
 * revierte un match ya resuelto al ocultar, ver NetpayReporte.model.js#eliminado) y puede
 * restaurarse — dejar sus folios sin caché sería un hueco de datos esperando a que alguien lo
 * restaure.
 *
 * Folios que Kore ya no tiene disponibles (confirmado empíricamente: consultarFolioKore lanza
 * "puede que la transacción ya no esté disponible" cuando Kore no devuelve cuenta — no hay
 * retención documentada en código) quedan `koreCache:null` para siempre; no hay forma de
 * recuperarlos y el script no reintenta indefinidamente — cada corrida simplemente vuelve a
 * intentar lo que siga pendiente.
 *
 * Mismo patrón operacional que migrate-netpay-v2.js (abre su propia conexión mongoose solo si
 * se corre directo, --dry-run es el default, cuenta/reporta sin escribir nada contra Kore).
 * A diferencia de ese script, acá NO hay --revert: esto solo completa un dato informativo que
 * ya estaba vacío, no hay ningún estado previo al que "revertir".
 */

const mongoose = require('mongoose');
const NetpayReporte = require('../src/banks/domains/erp/NetpayReporte.model');
const { consultarFoliosPendientes } = require('../src/banks/domains/erp/netpay-reporte.service');

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

// Reportes ya identificados contra un movimiento bancario real — sin filtrar por `estatus` a
// propósito (movementIdConfirmado ES el hecho relevante, no un estatus puntual; cubre
// confirmado_automatico/resuelto_por_reporte/resuelto_manual por igual) ni por `eliminado` (ver
// JSDoc arriba). Solo trae los campos que este script necesita — los reportes pueden tener
// decenas de folios, no hace falta traer el resto del documento.
async function _reportesConMovimientoIdentificado() {
  return NetpayReporte.find({ movementIdConfirmado: { $ne: null } })
    .select('folios movementIdConfirmado')
    .lean();
}

function _folioPendientes(reporte) {
  return (reporte.folios ?? []).filter(f => f.referencia && !f.koreCache?.cuenta);
}

// Separa los reportes en "completos" (ningún folio pendiente, nada que hacer) y "afectados"
// (al menos 1 folio sin koreCache.cuenta) — mismo criterio de idempotencia que
// migrate-netpay-v2.js: correr --dry-run varias veces nunca da un resultado distinto si nada
// cambió en Mongo/Kore entre medio.
function _clasificar(reportes) {
  const completos = [];
  const afectados = [];
  for (const reporte of reportes) {
    const pendientes = _folioPendientes(reporte);
    if (pendientes.length === 0) completos.push(reporte);
    else afectados.push({ reporte, pendientes });
  }
  return { completos, afectados };
}

function imprimirResumenDryRun({ completos, afectados }) {
  const totalFoliosPendientes = afectados.reduce((sum, a) => sum + a.pendientes.length, 0);
  console.log('── Resumen backfill koreCache de Netpay (reportes ya identificados) ──');
  console.log(`Reportes con movimiento identificado: ${completos.length + afectados.length}`);
  console.log(`  ya completos (sin folios pendientes): ${completos.length}`);
  console.log(`  con folios pendientes: ${afectados.length}`);
  console.log(`  total de folios pendientes a consultar en Kore: ${totalFoliosPendientes}`);
  if (afectados.length === 0) {
    console.log('Nada que hacer.');
  } else {
    console.log(`Correr con --apply para consultar esos ${totalFoliosPendientes} folio(s) contra Kore.`);
  }
  return { totalReportesAfectados: afectados.length, totalFoliosPendientes };
}

async function runDryRun() {
  const reportes = await _reportesConMovimientoIdentificado();
  return imprimirResumenDryRun(_clasificar(reportes));
}

const NO_DISPONIBLE_REGEX = /ya no esté disponible/;

async function runApply() {
  const reportes = await _reportesConMovimientoIdentificado();
  const { afectados } = _clasificar(reportes);
  const { totalFoliosPendientes } = imprimirResumenDryRun(_clasificar(reportes));

  if (afectados.length === 0) return { totalResueltos: 0, fallos: [] };

  let totalResueltos = 0;
  const fallos = [];

  for (const { reporte, pendientes } of afectados) {
    const resultado = await consultarFoliosPendientes(reporte._id);
    totalResueltos += resultado.consultados;
    for (const fallo of resultado.fallos) {
      fallos.push({ reporteId: String(reporte._id), ...fallo });
    }
    console.log(
      `  reporte=${reporte._id}  pendientes=${pendientes.length}  `
      + `resueltos=${resultado.consultados}  fallos=${resultado.fallos.length}`,
    );
  }

  const noDisponibles = fallos.filter(f => NO_DISPONIBLE_REGEX.test(f.error));
  const otrosErrores  = fallos.filter(f => !NO_DISPONIBLE_REGEX.test(f.error));

  console.log('── Resultado final ──');
  console.log(`Folios resueltos: ${totalResueltos} / ${totalFoliosPendientes}`);
  console.log(`Folios ya no disponibles en Kore (permanente, no reintentable): ${noDisponibles.length}`);
  console.log(`Folios con otro error (red/429 agotado — reintentable corriendo de nuevo): ${otrosErrores.length}`);
  for (const f of otrosErrores) {
    console.log(`  OTRO ERROR  reporte=${f.reporteId}  folio=${f.referencia}  error=${f.error}`);
  }

  return { totalResueltos, fallos };
}

async function main(argv = process.argv.slice(2)) {
  const { modo } = parseArgs(argv);
  await mongoose.connect(MONGODB_URI);
  console.log(`Conectado a MongoDB. Modo: --${modo}.`);
  try {
    if (modo === 'dry-run') return await runDryRun();
    return await runApply();
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
  _reportesConMovimientoIdentificado,
  _folioPendientes,
  _clasificar,
  imprimirResumenDryRun,
  runDryRun,
  runApply,
  main,
};
