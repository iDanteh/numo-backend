'use strict';

// netpay-reporte-revert.service.js — netpay-matching-v2 (design.md "Unlink hooks"):
// reversión de un NetpayReporte cuando el usuario desvincula el erpId sintético
// NETPAYRPT-<claveRastreo> desde "IDs ERP" (Bancos). Mismo mecanismo de hook que
// netpay-match-revert.service.js (registrado en bank.service.js#registerErpUnlinkHook).
//
// CAMBIO DE COMPORTAMIENTO v1→v2: el reporte YA NO vuelve a 'pendiente' (ese valor no
// existe en el enum v2, ver NetpayReporte.model.js#estatus) — pasa a discrepancia/revertido
// y NUNCA se vuelve a auto-resolver (netpay-reporte.service.js#evaluarReporte no se
// invoca automáticamente sobre un reporte ya revertido; un "Reevaluar" manual explícito
// puede volver a intentarlo, mismo criterio que un bucket revertido en
// netpay-evaluacion.service.js — la diferencia es que ESE nunca se reevalúa ni manualmente
// vía evaluarRango, mientras el reporte SÍ tiene una acción explícita "Reevaluar" en el API
// table de design.md).
//
// design.md: "Every bucket it closed goes to discrepancia/reporte_revertido" — cada
// NetpayMatch que este reporte había cerrado (snapshot.reporteIdOrigen === este reporte,
// estatusMatch:'resuelto_por_reporte') también se revierte.
//
// Prefijo DISTINTO ('NETPAYRPT-') del matching automático ('NETPAY-', ver
// netpay-match-revert.service.js) — nunca colisionan: 'NETPAYRPT-...'.startsWith('NETPAY-')
// es false (el séptimo carácter es 'R', no '-').

const NetpayReporte = require('./NetpayReporte.model');
const NetpayMatch = require('./NetpayMatch.model');
const { registerErpUnlinkHook } = require('../banks/bank.service');
const { emitToAll } = require('../../shared/socket');

const PREFIJO_ERP_ID_REPORTE = 'NETPAYRPT-';

async function _revertirBucketsCerrados(reporteId, session) {
  const buckets = await NetpayMatch.find({
    'snapshot.reporteIdOrigen': reporteId, estatusMatch: 'resuelto_por_reporte',
  }).lean();

  for (const bucket of buckets) {
    const filtro = { _id: bucket._id, estatusMatch: 'resuelto_por_reporte' };
    const update = { $set: { estatusMatch: 'discrepancia', motivoDiscrepancia: 'reporte_revertido' } };
    // eslint-disable-next-line no-await-in-loop
    if (session) await NetpayMatch.findOneAndUpdate(filtro, update, { session });
    // eslint-disable-next-line no-await-in-loop
    else await NetpayMatch.findOneAndUpdate(filtro, update);
  }
}

async function _revertirPorDesvinculacion({ erpId, movementId, session }) {
  if (!erpId || !erpId.startsWith(PREFIJO_ERP_ID_REPORTE)) return;

  const query = NetpayReporte.findOne({ movementIdConfirmado: movementId, estatus: 'resuelto_por_reporte' });
  const reporte = await (session ? query.session(session) : query);
  // No existe (ej. ya se había revertido antes, o es un erpId huérfano) — no hay nada que
  // revertir.
  if (!reporte) return;

  reporte.estatus = 'discrepancia';
  reporte.motivoDiscrepancia = 'revertido';
  reporte.revertido = { en: new Date(), movementIds: [movementId] };
  await reporte.save(session ? { session } : undefined);

  await _revertirBucketsCerrados(String(reporte._id), session);

  // Señal cross-banco (mismo criterio que netpay-match-revert.service.js): al revertir, el
  // movimiento deja de calificar para "pendientes de ficha" — refrescar la bandeja igual
  // que al confirmar.
  emitToAll('bank:ficha-pendiente:changed', { movementId });
}

function init() {
  registerErpUnlinkHook(_revertirPorDesvinculacion);
}

module.exports = { init, _revertirPorDesvinculacion };
