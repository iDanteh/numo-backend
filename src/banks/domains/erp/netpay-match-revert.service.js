'use strict';

// netpay-match-revert.service.js — reversión del match de un grupo Netpay cuando el
// usuario desvincula el erpId sintético NETPAY-<terminalID>-<YYYY-MM-DD> desde "IDs ERP"
// (Bancos). Mismo mecanismo que caja-transferencia-revert.service.js (hook registrado en
// bank.service.js#registerErpUnlinkHook, evita el require circular con bank.service.js).
//
// DECISIÓN DE DISEÑO (a diferencia de CAJA-<koreId>, que parsea el koreId del propio erpId
// para volver a encontrar la CajaTransferencia): acá NO se parsea terminalID/día de vuelta
// desde el string del erpId — un terminalID podría en teoría traer guiones y volvería
// ambigua la separación "NETPAY-<terminalID>-<YYYY-MM-DD>". En vez de eso, se busca el
// NetpayMatch directamente por `movementIdsConfirmados` (ya lo guarda confirmarMatchNetpay)
// + estatusMatch:'matcheada' — sin ambigüedad posible, sin parsear nada.
//
// A diferencia de CajaTransferencia (que vuelve a 'pendiente'), acá el documento NetpayMatch
// se BORRA por completo: un grupo "pendiente" nunca tiene documento propio (ver
// NetpayMatch.model.js) — pendiente es la ausencia de un registro resuelto para esa clave,
// así que "revertir a pendiente" es, por diseño, simplemente dejar de existir.

const NetpayMatch = require('./NetpayMatch.model');
const { registerErpUnlinkHook } = require('../banks/bank.service');
const { emitToAll } = require('../../shared/socket');

const PREFIJO_ERP_ID_SINTETICO = 'NETPAY-';

// erpId real de Kore o de otro origen (ej. CAJA-) -> retorna de inmediato, sin ninguna
// query (caso común, no le agrega overhead a un desvincular que no tiene nada que ver con
// Netpay).
async function _revertirPorDesvinculacion({ erpId, movementId, session }) {
  if (!erpId || !erpId.startsWith(PREFIJO_ERP_ID_SINTETICO)) return;

  const query = NetpayMatch.findOne({ movementIdsConfirmados: movementId, estatusMatch: 'matcheada' });
  const match = await (session ? query.session(session) : query);
  // No existe (ej. ya se había revertido antes, o es un erpId NETPAY- huérfano) — no es
  // un error, no hay nada que revertir.
  if (!match) return;

  await NetpayMatch.deleteOne({ _id: match._id }, { session });

  // Señal cross-banco (mismo criterio que caja-transferencia-revert.service.js): al
  // revertir, el movimiento se desvincula (queda sin erpLinks de netpay-matching) — deja
  // de calificar para "pendientes de ficha", así que la bandeja debe refrescarse igual que
  // al confirmar.
  emitToAll('bank:ficha-pendiente:changed', { movementId });
}

function init() {
  registerErpUnlinkHook(_revertirPorDesvinculacion);
}

module.exports = { init, _revertirPorDesvinculacion };
