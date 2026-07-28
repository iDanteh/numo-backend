'use strict';

// collection-request-erp-links.js — cálculo de erpLinks[] (saldoActual/
// saldoPagado/saldoPagadoTotal) a pasar a bankService.setErpIds() al aplicar
// un cobro. Extraído de collection-request.service.js: funciones puras, sin
// llamadas a Kore ni a la base de datos (recibe los datos ya consultados).

// Puerto exacto de _esFormaBancaria()/_norm() en cobro-panel.component.ts — NO
// se basa en si la forma trae banco seleccionado (bancoKoreId), sino en el
// TEXTO de la descripción: transferencia, cheque o depósito en efectivo
// cuentan como "bancaria" (alimentan saldoPagado — dropdown "CxC vinculadas"
// de la tabla de movimientos), cualquier otra (efectivo de caja, tarjeta,
// compensación, etc.) no, aunque sí liquide la CxC (saldoPagadoTotal).
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
  if (/cheque/i.test(desc)) return true;
  return /deposito.*efectivo/.test(normFormaPago(desc));
}

// Puerto exacto de esSaldoEspecial() en cobro-panel.component.ts — clasifica una
// forma de pago como saldo a favor / anticipo, los dos únicos tipos que requieren
// un id de Kore + monto específico (ver formasPago[].saldosAplicados en el modelo).
// "Compensación" se detecta en el frontend pero no maneja id/monto propio — viaja
// como forma de pago normal en DetalleFormaPago, sin entrada en saldosAFavorAUsar/
// anticipos, así que aquí no se clasifica como caso especial.
function tipoSaldoEspecial(f) {
  const n = normFormaPago(f?.formaPagoDescripcion);
  if (n.includes('saldo a favor')) return 'saldo_favor';
  if (n === 'anticipo') return 'anticipo';
  return null;
}

// Puerto exacto de _matchBancoDefault() en cobro-panel.component.ts — resuelve
// el banco de Kore que corresponde al `banco` del BankMovement identificado
// (ej. "BBVA"), para poder mandar BancoID al aplicar el cobro automático (ver
// identificar() en collection-request.service.js). A diferencia del panel
// manual, acá no hay un humano confirmando el banco en pantalla antes de
// aplicar — el usuario confirmó (2026-07-28) que igual quiere el mismo
// fallback: si no hay match, usar bancos[0] (el primero del catálogo) en vez
// de dejar el cobro sin BancoID.
function matchBancoDefault(bancos, movBanco) {
  const banco = (movBanco ?? '').toUpperCase().trim();
  if (!banco) return bancos[0] ?? null;
  return (
    bancos.find(b => b.claveBanco.toUpperCase() === banco) ??
    bancos.find(b => banco.includes(b.claveBanco.toUpperCase()) || b.claveBanco.toUpperCase().includes(banco)) ??
    bancos.find(b => b.descripcion.toUpperCase().includes(banco) || banco.includes(b.descripcion.toUpperCase())) ??
    bancos[0] ?? null
  );
}

// erpLinks[] a pasar a bankService.setErpIds() — mismo cálculo que
// _buildCobroSaldosErp() en cobro-panel.component.ts:
//   - saldoActual: saldo EN VIVO de Kore (antes de este cobro) menos lo pagado ahora.
//   - saldoPagado: acumulado de formas BANCARIAS (transferencia/cheque/depósito) —
//     alimenta el badge de la tabla de bancos.
//   - saldoPagadoTotal: acumulado de CUALQUIER forma — alimenta saldoErp.
//   - desglosePorFormaPago: bitácora de auditoría, una entrada por cada forma de pago
//     usada en ESTE cobro — igual que hace cobro-panel.component.ts.
// Los 3 acumulados/bitácora se suman sobre lo que ya tuviera el erpLink existente (por
// si la CxC ya traía pagos parciales previos en ese mismo movimiento) — nunca se
// sobreescriben.
// setErpIds() REEMPLAZA por completo mov.erpLinks, así que aquí también se preservan
// los links de otras CxC ajenas a esta solicitud que ya estuvieran en el movimiento.
function buildErpLinksParaCobro(cr, cuentasKore, existingLinks) {
  const round2       = (n) => Math.round(n * 100) / 100;
  const cuentaPorId   = new Map(cuentasKore.map(c => [String(c.id), c]));
  const existingPorId = new Map((existingLinks || []).map(l => [l.erpId, l]));
  const fechaCobro    = new Date();

  function _link(cxc, pagadoBanco, pagadoTotal, nuevoDesglose) {
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
      desglosePorFormaPago: [...(existing?.desglosePorFormaPago ?? []), ...nuevoDesglose],
    };
  }

  function _desgloseDe(formaPago, monto) {
    if (!(monto > 0)) return [];
    return [{
      formaPagoId:          formaPago.formaPagoId,
      formaPagoDescripcion: formaPago.formaPagoDescripcion,
      monto:                round2(monto),
      fecha:                fechaCobro,
    }];
  }

  let nuevos;
  if (cr.cxcs.length === 1) {
    // Modo 1 — N formas de pago, todas contra la misma (única) CxC: una entrada de
    // desglose por cada forma de pago con importe > 0.
    const totalPaid = cr.formasPago.reduce((s, f) => s + f.importe, 0);
    const bancoPaid = cr.formasPago.filter(esFormaBancaria).reduce((s, f) => s + f.importe, 0);
    const desglose  = cr.formasPago.flatMap(f => _desgloseDe(f, f.importe));
    nuevos = [_link(cr.cxcs[0], bancoPaid, totalPaid, desglose)];
  } else {
    // Modo 2 — 1 sola forma de pago global, repartida entre N CxC: cada CxC recibe UNA
    // entrada de desglose con la misma forma de pago y el monto que le tocó (montoAsignado).
    const esBancaria = esFormaBancaria(cr.formasPago[0]);
    nuevos = cr.cxcs.map(cxc => {
      const paid     = cxc.montoAsignado ?? 0;
      const desglose = _desgloseDe(cr.formasPago[0], paid);
      return _link(cxc, esBancaria ? paid : 0, paid, desglose);
    });
  }

  const nuevosIds   = new Set(nuevos.map(l => l.erpId));
  const preservados = (existingLinks || []).filter(l => !nuevosIds.has(l.erpId));
  return [...preservados, ...nuevos];
}

module.exports = { normFormaPago, esFormaBancaria, tipoSaldoEspecial, matchBancoDefault, buildErpLinksParaCobro };
