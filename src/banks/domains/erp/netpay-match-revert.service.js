'use strict';

// netpay-match-revert.service.js — netpay-matching-v2 (design.md "Revert (unlink)"):
// reversión del match de un bucket Netpay cuando el usuario desvincula el erpId sintético
// NETPAY-<terminalID>-<YYYY-MM-DD>[-<bucket>] desde "IDs ERP" (Bancos). Mismo mecanismo de
// hook que v1 (registrado en bank.service.js#registerErpUnlinkHook).
//
// CAMBIO DE COMPORTAMIENTO v1→v2 (design.md, decisión explícita, "Rejected: Delete the doc
// (today's behavior)... Deleting would let the next evaluation re-link the same wrong
// movement"): el documento YA NO se borra — pasa a discrepancia/revertido y NUNCA se
// vuelve a auto-confirmar (netpay-evaluacion.service.js#_debeReevaluarse lo salta a
// propósito por motivoDiscrepancia:'revertido'). Un grupo "pendiente" real (nunca
// evaluado) sigue siendo la ausencia de documento — pero un bucket YA revertido conserva
// su historia como discrepancia auditable, no desaparece.
//
// También resetea cualquier NetpayReporte cuyo vinculo sea 'corroborado' sobre el MISMO
// movimiento (design.md: "resets any corroborado report on M the same way") — un reporte
// corroborado nunca creó su propio erpLink (NETPAYRPT-), solo confirmó que un bucket YA
// confirmado_automatico compartía su monto; al revertir ESE bucket, la corroboración deja
// de tener sentido y también debe volver a discrepancia.

const NetpayMatch = require('./NetpayMatch.model');
const NetpayReporte = require('./NetpayReporte.model');
const { registerErpUnlinkHook } = require('../banks/bank.service');
const { emitToAll } = require('../../shared/socket');

const PREFIJO_ERP_ID_SINTETICO = 'NETPAY-';

// erpId real de Kore, o de otro origen (NETPAYRPT-, CAJA-, etc.) -> retorna de inmediato,
// sin ninguna query. NETPAYRPT- nunca matchea este prefijo (ver comentario de diseño en
// netpay-reporte-revert.service.js: 'NETPAYRPT-...'.startsWith('NETPAY-') es false).
async function _revertirPorDesvinculacion({ erpId, movementId, session }) {
  if (!erpId || !erpId.startsWith(PREFIJO_ERP_ID_SINTETICO)) return;

  const matchQuery = NetpayMatch.findOne({
    movementIdsConfirmados: movementId,
    estatusMatch: { $in: ['confirmado_automatico', 'resuelto_manual'] },
  });
  const match = await (session ? matchQuery.session(session) : matchQuery);
  // No existe (ej. ya se había revertido antes, o es un erpId NETPAY- huérfano) — no es
  // un error, no hay nada que revertir.
  if (!match) return;

  match.estatusMatch = 'discrepancia';
  match.motivoDiscrepancia = 'revertido';
  match.revertido = { en: new Date(), movementIds: [movementId] };
  await match.save(session ? { session } : undefined);

  const reporteQuery = NetpayReporte.findOne({
    movementIdConfirmado: movementId, vinculo: 'corroborado', estatus: 'resuelto_por_reporte',
  });
  const reporteCorroborado = await (session ? reporteQuery.session(session) : reporteQuery);
  if (reporteCorroborado) {
    reporteCorroborado.estatus = 'discrepancia';
    reporteCorroborado.motivoDiscrepancia = 'reporte_revertido';
    reporteCorroborado.revertido = { en: new Date(), movementIds: [movementId] };
    await reporteCorroborado.save(session ? { session } : undefined);
  }

  // Señal cross-banco (mismo criterio que v1): al revertir, el movimiento se desvincula
  // (queda sin erpLinks de netpay-matching) — deja de calificar para "pendientes de
  // ficha", así que la bandeja debe refrescarse igual que al confirmar.
  emitToAll('bank:ficha-pendiente:changed', { movementId });
}

function init() {
  registerErpUnlinkHook(_revertirPorDesvinculacion);
}

module.exports = { init, _revertirPorDesvinculacion };
