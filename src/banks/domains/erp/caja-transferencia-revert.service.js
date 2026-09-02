'use strict';

// caja-transferencia-revert.service.js — reversión del match de una CajaTransferencia
// cuando el usuario desvincula el erpId sintético CAJA-<koreId> desde "IDs ERP" (Bancos).
// Pedido explícito del usuario 2026-09-02, gap identificado el 2026-09-01: sin esto,
// desvincular liberaba el BankMovement (vuelve a calificar como candidato en
// buscarCandidatos, caja-transferencia-match.service.js) pero la CajaTransferencia
// quedaba estancada en estatusMatch:'matcheada' para siempre — "resuelta" aunque su
// movimiento ya estuviera libre y pudiera terminar matcheado a OTRA transferencia.
//
// Se registra como hook en bank.service.js (registerErpUnlinkHook) — bank.service.js
// (dominio banks) nunca importa nada de acá ni sabe que caja-transferencia existe; es
// este archivo el que se registra a sí mismo al cargarse desde erp.routes.js. Esto evita
// el require circular: caja-transferencia-confirm.service.js YA importa setErpIds de
// bank.service.js, así que un require en sentido inverso (bank.service.js -> acá) crearía
// un ciclo.
//
// Split 1:2 (pedido explícito del usuario): si se desvincula UNO solo de los dos
// movimientos de una transferencia dividida, la transferencia igual vuelve COMPLETA a
// 'pendiente' — sin importar que el otro movimiento siga con su erpLink intacto. Ese
// remanente no es un problema real: confirmarMatch() (caja-transferencia-confirm.service.js)
// ya rechaza cualquier movimiento que ya tenga erpLinks, así que ese movimiento no puede
// reusarse por error en una futura confirmación.

const CajaTransferencia = require('./CajaTransferencia.model');
const { registerErpUnlinkHook } = require('../banks/bank.service');

const PREFIJO_ERP_ID_SINTETICO = 'CAJA-';

// erpId real de Kore -> retorna de inmediato, sin ninguna query (caso común, no le
// agrega overhead a un desvincular que no tiene nada que ver con transferencias-caja).
async function _revertirPorDesvinculacion({ erpId, session }) {
  if (!erpId || !erpId.startsWith(PREFIJO_ERP_ID_SINTETICO)) return;

  const koreId = erpId.slice(PREFIJO_ERP_ID_SINTETICO.length);
  const query = CajaTransferencia.findOne({ koreId, estatusMatch: 'matcheada' });
  const transferencia = await (session ? query.session(session) : query);
  // No existe o no está 'matcheada' (ej. ya se había revertido antes, o es un erpId
  // CAJA- huérfano) — no es un error, no hay nada que revertir.
  if (!transferencia) return;

  await CajaTransferencia.updateOne(
    { _id: transferencia._id },
    {
      $set: {
        estatusMatch: 'pendiente',
        confirmadoPor: null,
        confirmadoEn: null,
        movementIdsConfirmados: [],
      },
    },
    { session },
  );
}

function init() {
  registerErpUnlinkHook(_revertirPorDesvinculacion);
}

module.exports = { init, _revertirPorDesvinculacion };
