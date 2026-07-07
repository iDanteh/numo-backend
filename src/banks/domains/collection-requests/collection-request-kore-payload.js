'use strict';

// collection-request-kore-payload.js — construcción pura del payload que
// espera Kore al aplicar un cobro (Modo 1 / Modo 2). Mismo shape que
// _buildCobroPayload / _buildCobroPayloadMulti de cobro-panel.component.ts —
// ver AplicarCobroPayload / AplicarCobroPayloadMulti en
// numo-frontend/src/app/core/models/bank.model.ts. Extraído de
// collection-request.service.js: funciones puras (cr/mov/sesionId → payload),
// sin llamadas a Kore ni a la base de datos.

function buildDetalleFormaPago(f, mov) {
  const d = {
    FormaPagoID:     f.formaPagoId,
    FormaPagoNombre: f.formaPagoDescripcion,
    Monto:           f.importe,
    Recibido:        f.importe,
    Comision:        0,
    transactionID:   '',
  };
  if (f.bancoKoreId) {
    d.BancoID          = f.bancoKoreId;
    d.BancoDescripcion = f.bancoDescripcion;
  }
  if (f.referencia) {
    const datos = [{ Nombre: 'Aut', Valor: f.referencia }];
    if (mov.numeroAutorizacion) datos.push({ Nombre: 'Numo', Valor: mov.numeroAutorizacion });
    d.DatosAdicionales = datos;
  }
  return d;
}

function fechaISO(mov) {
  const d = new Date(mov.fecha ?? Date.now());
  return isNaN(d.getTime()) ? new Date().toISOString() : d.toISOString();
}

// Modo 1 — 1 CxC + N formas de pago → POST /cobros/operacion/:sesionId
function buildPayloadSingle(cr, formasPagoConRef, mov, sesionId) {
  const cxc   = cr.cxcs[0];
  const fecha = fechaISO(mov);
  return {
    anotacion:               `Pago de pedido ${cxc.serie ?? ''}-${cxc.folioExterno ?? cxc.erpId}`,
    anticipoTimbrar:         false,
    anticipos:               {},
    cantAnticipoAutomatico:  0,
    codigo:                  '',
    cuenta:                  cxc.erpId,
    datoFiscalID:            0,
    detalle: {
      DetalleFormaPago:  formasPagoConRef.map(f => buildDetalleFormaPago(f, mov)),
      Total:             cr.monto,
      autorizo:          '',
      concepto:          cr.conceptoId,
      encargado:         '',
      fecha_afectacion:  fecha,
      fecha_aplicacion:  fecha,
      fecha_real_pago:   fecha,
    },
    formaPagoAnticipoAutoID: '',
    saldosAFavorAUsar:       {},
    sesionId,
    usoCFDI:                 'G03',
  };
}

// Modo 2 — N CxC + 1 forma de pago → POST /cobros/operacion-multiple/:sesionId
function buildPayloadMulti(cr, formasPagoConRef, mov, sesionId) {
  const fecha = fechaISO(mov);
  return {
    MotivoAutorizacion:      '',
    anotacion:               '',
    anticipos:               {},
    cantAnticipoAutomatico:  0,
    cuentas:                 cr.cxcs.map(c => ({ CuentaID: c.erpId, Monto: c.montoAsignado })),
    datoFiscalID:            0,
    detalle: {
      DetalleFormaPago:  formasPagoConRef.map(f => buildDetalleFormaPago(f, mov)),
      Total:             cr.monto,
      autorizo:          '',
      concepto:          cr.conceptoId,
      encargado:         '',
      fecha_afectacion:  fecha,
      fecha_aplicacion:  fecha,
      fecha_real_pago:   fecha,
    },
    formaPagoAnticipoAutoID: '',
    idUsuarioAutoriza:       '',
    saldosAFavorAUsar:       {},
  };
}

module.exports = { buildDetalleFormaPago, fechaISO, buildPayloadSingle, buildPayloadMulti };
