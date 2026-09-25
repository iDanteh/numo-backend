'use strict';

// netpay-reporte-revert.service.js — reversión de un NetpayReporte cuando el usuario
// desvincula el erpId sintético NETPAYRPT-<claveRastreo> desde "IDs ERP" (Bancos). Mismo
// mecanismo de hook que netpay-match-revert.service.js (registrado en
// bank.service.js#registerErpUnlinkHook), pero a diferencia de NetpayMatch (que se BORRA al
// revertir, porque un grupo pendiente nunca tiene documento propio), acá el documento se
// CONSERVA — solo vuelve a estatus 'pendiente' — porque el detalle de folios YA parseado es
// costoso de recuperar (habría que resubir el Excel) y el reporte en sí sigue siendo válido
// para volver a conciliar contra otro BankMovement.
//
// Prefijo DISTINTO ('NETPAYRPT-') del matching automático ('NETPAY-', ver
// netpay-match-revert.service.js) — nunca colisionan: 'NETPAYRPT-...'.startsWith('NETPAY-')
// es false (el séptimo carácter es 'R', no '-').

const NetpayReporte = require('./NetpayReporte.model');
const { registerErpUnlinkHook } = require('../banks/bank.service');
const { emitToAll } = require('../../shared/socket');

const PREFIJO_ERP_ID_REPORTE = 'NETPAYRPT-';

async function _revertirPorDesvinculacion({ erpId, movementId, session }) {
  if (!erpId || !erpId.startsWith(PREFIJO_ERP_ID_REPORTE)) return;

  const query = NetpayReporte.findOne({ movementIdConfirmado: movementId, estatus: 'confirmado' });
  const reporte = await (session ? query.session(session) : query);
  // No existe (ej. ya se había revertido antes, o es un erpId huérfano) — no hay nada que
  // revertir.
  if (!reporte) return;

  reporte.estatus = 'pendiente';
  reporte.movementIdConfirmado = null;
  reporte.confirmadoPor = null;
  reporte.confirmadoEn = null;
  await reporte.save(session ? { session } : undefined);

  // Señal cross-banco (mismo criterio que netpay-match-revert.service.js): al revertir, el
  // movimiento deja de calificar para "pendientes de ficha" — refrescar la bandeja igual
  // que al confirmar.
  emitToAll('bank:ficha-pendiente:changed', { movementId });
}

function init() {
  registerErpUnlinkHook(_revertirPorDesvinculacion);
}

module.exports = { init, _revertirPorDesvinculacion };
