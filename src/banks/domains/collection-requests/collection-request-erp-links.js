'use strict';

// collection-request-erp-links.js — cálculo de erpLinks[] (saldoActual/
// saldoPagado/saldoPagadoTotal) a pasar a bankService.setErpIds() al aplicar
// un cobro. Extraído de collection-request.service.js: funciones puras, sin
// llamadas a Kore ni a la base de datos (recibe los datos ya consultados).

// Puerto exacto de _esFormaBancaria()/_norm() en cobro-panel.component.ts — NO
// se basa en si la forma trae banco seleccionado (bancoKoreId), sino en el
// TEXTO de la descripción: transferencia o depósito en efectivo cuentan como
// "bancaria" (alimentan saldoPagado), cualquier otra (cheque, efectivo de
// caja, tarjeta, etc.) no, aunque sí liquide la CxC (saldoPagadoTotal).
// "Depósito en efectivo" normalmente no trae bancoKoreId (no siempre exige
// elegir banco), por eso basarse en bancoKoreId lo excluía por error.
function normFormaPago(s) {
  return (s ?? '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase().replace(/\s+/g, ' ').trim();
}

function esFormaBancaria(f) {
  if (!f) return false;
  const desc = f.formaPagoDescripcion || '';
  if (/transferencia/i.test(desc)) return true;
  return /deposito.*efectivo/.test(normFormaPago(desc));
}

// erpLinks[] a pasar a bankService.setErpIds() — mismo cálculo que
// _buildCobroSaldosErp() en cobro-panel.component.ts:
//   - saldoActual: saldo EN VIVO de Kore (antes de este cobro) menos lo pagado ahora.
//   - saldoPagado: acumulado de formas BANCARIAS (transferencia/depósito) — alimenta
//     el badge de la tabla de bancos.
//   - saldoPagadoTotal: acumulado de CUALQUIER forma — alimenta saldoErp.
// Ambos acumulados se suman sobre lo que ya tuviera el erpLink existente (por si la
// CxC ya traía pagos parciales previos en ese mismo movimiento).
// setErpIds() REEMPLAZA por completo mov.erpLinks, así que aquí también se preservan
// los links de otras CxC ajenas a esta solicitud que ya estuvieran en el movimiento.
function buildErpLinksParaCobro(cr, cuentasKore, existingLinks) {
  const round2       = (n) => Math.round(n * 100) / 100;
  const cuentaPorId   = new Map(cuentasKore.map(c => [String(c.id), c]));
  const existingPorId = new Map((existingLinks || []).map(l => [l.erpId, l]));

  function _link(cxc, pagadoBanco, pagadoTotal) {
    const cuenta    = cuentaPorId.get(cxc.erpId);
    const prevSaldo = cuenta?.saldoActual ?? cxc.total ?? 0;
    const existing  = existingPorId.get(cxc.erpId);
    return {
      erpId:            cxc.erpId,
      saldoActual:      round2(Math.max(0, prevSaldo - pagadoTotal)),
      saldoPagado:      round2((existing?.saldoPagado ?? 0) + pagadoBanco),
      saldoPagadoTotal: round2((existing?.saldoPagadoTotal ?? 0) + pagadoTotal),
      folioFiscal:      cxc.folioFiscal ?? null,
      total:            cuenta?.total ?? cxc.total ?? 0,
      serie:            cxc.serie ?? cuenta?.serie ?? null,
      folioExterno:     cxc.folioExterno ?? cuenta?.folio ?? null,
      tipoPago:         cxc.tipoPago ?? cuenta?.tipoPago ?? null,
    };
  }

  let nuevos;
  if (cr.cxcs.length === 1) {
    const totalPaid = cr.formasPago.reduce((s, f) => s + f.importe, 0);
    const bancoPaid = cr.formasPago.filter(esFormaBancaria).reduce((s, f) => s + f.importe, 0);
    nuevos = [_link(cr.cxcs[0], bancoPaid, totalPaid)];
  } else {
    const esBancaria = esFormaBancaria(cr.formasPago[0]);
    nuevos = cr.cxcs.map(cxc => {
      const paid = cxc.montoAsignado ?? 0;
      return _link(cxc, esBancaria ? paid : 0, paid);
    });
  }

  const nuevosIds   = new Set(nuevos.map(l => l.erpId));
  const preservados = (existingLinks || []).filter(l => !nuevosIds.has(l.erpId));
  return [...preservados, ...nuevos];
}

module.exports = { normFormaPago, esFormaBancaria, buildErpLinksParaCobro };
