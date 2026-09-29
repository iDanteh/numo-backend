'use strict';

// netpay-match-confirm.service.js — netpay-matching-v2 (design.md "File Changes": "keeps
// _recalcularNetoEnVivo, drops confirm/discard"): confirmarMatchNetpay/descartarMatchNetpay
// (candidate picker manual, Fase D del matching Netpay↔BBVA v1) se ELIMINAN de este
// archivo — el "Approach" del proposal es explícito: "Candidate picker removed for
// Netpay". La confirmación automática 1:1 ahora vive en netpay-evaluacion.service.js
// (evaluarRango), y la resolución/rechazo manual de un bucket 'discrepancia' vive en
// netpay-resolver.service.js.
//
// _recalcularNetoEnVivo SOBREVIVE sin cambios — sigue siendo la única forma de recalcular
// el neto EN VIVO contra Kore para un terminalID+día exacto (puede haber llegado una
// transacción tardía desde la última evaluación automática). netpay-resolver.service.js la
// reusa para mostrar el neto actualizado en el detalle de un bucket 'discrepancia' antes de
// resolverlo manualmente.

const { consultarTransaccionesNetpay } = require('./netpay-transacciones.service');
const { _diaMx } = require('./netpay-match.service');

// Recalcula el neto EN VIVO para un terminalID+día exacto — acotado a ese único día
// (dateFrom/dateTo del mismo día), y se vuelve a filtrar por _diaMx en memoria por si
// Kore devolviera algo fuera de rango por algún desfase de huso horario (mismo criterio
// defensivo que el resto del dominio ERP con fechas de Kore).
//
// `dateFrom`/`dateTo` viajan pelados (YYYY-MM-DD) — es consultarTransaccionesNetpay quien
// arma el instante UTC real de inicio/fin de día en hora MX (ver _medianocheMx/_finDiaMx
// en netpay-transacciones.service.js).
async function _recalcularNetoEnVivo(terminalID, diaMarcador) {
  const diaStr = diaMarcador.toISOString().slice(0, 10);
  const { transacciones } = await consultarTransaccionesNetpay({ dateFrom: diaStr, dateTo: diaStr, terminalID });

  const delDia = transacciones.filter(t => _diaMx(t.transactionDate).getTime() === diaMarcador.getTime());
  const montoBruto = delDia.reduce((acc, t) => acc + (t.amount ?? 0), 0);
  const comision = delDia.reduce((acc, t) => acc + (t.commission ?? 0), 0);
  return { netoEsperado: montoBruto - comision, cantidadTransacciones: delDia.length };
}

module.exports = { _recalcularNetoEnVivo };
