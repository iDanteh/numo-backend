'use strict';

// collection-request-kore-payload.js — construcción pura del payload que
// espera Kore al aplicar un cobro (Modo 1 / Modo 2). Mismo shape que
// _buildCobroPayload / _buildCobroPayloadMulti de cobro-panel.component.ts —
// ver AplicarCobroPayload / AplicarCobroPayloadMulti en
// numo-frontend/src/app/core/models/bank.model.ts. Extraído de
// collection-request.service.js: funciones puras (cr/mov/sesionId → payload),
// sin llamadas a Kore ni a la base de datos.

const { tipoSaldoEspecial } = require('./collection-request-erp-links');

// Kore exige que `anticipos` NUNCA sea `{}` en Modo 1 (single-CxC) cuando no hay
// anticipos reales — mismo placeholder que usa cobro-panel.component.ts
// (_buildCobroPayload). Modo 2 sí acepta `{}` real (_buildCobroPayloadMulti).
const ANTICIPO_PLACEHOLDER_SINGLE = { additionalProp1: 0, additionalProp2: 0, additionalProp3: 0 };

// Arma los mapas `anticipos`/`saldosAFavorAUsar` (id de Kore → monto) que Kore
// necesita para saber DE QUÉ registro específico descontar — a partir de
// formasPago[].saldosAplicados (ver CollectionRequest.model.js). Si dos formas
// de pago usan el mismo saldo/anticipo (no debería pasar, pero por seguridad)
// los montos se suman en vez de pisarse.
function buildSaldosEspeciales(formasPago) {
  const anticipos = {};
  const saldosAFavorAUsar = {};
  for (const f of formasPago) {
    const tipo = tipoSaldoEspecial(f);
    if (!tipo) continue;
    const target = tipo === 'anticipo' ? anticipos : saldosAFavorAUsar;
    for (const s of (f.saldosAplicados || [])) {
      if (!s?.id) continue;
      target[s.id] = Math.round(((target[s.id] || 0) + Number(s.monto || 0)) * 100) / 100;
    }
  }
  return { anticipos, saldosAFavorAUsar };
}

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
  const { anticipos, saldosAFavorAUsar } = buildSaldosEspeciales(formasPagoConRef);
  return {
    anotacion:               `Pago de pedido ${cxc.serie ?? ''}-${cxc.folioExterno ?? cxc.erpId}`,
    anticipoTimbrar:         false,
    anticipos:               Object.keys(anticipos).length > 0 ? anticipos : ANTICIPO_PLACEHOLDER_SINGLE,
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
    saldosAFavorAUsar,
    sesionId,
    usoCFDI:                 'G03',
  };
}

// Modo 2 — N CxC + 1 forma de pago → POST /cobros/operacion-multiple/:sesionId
function buildPayloadMulti(cr, formasPagoConRef, mov, sesionId) {
  const fecha = fechaISO(mov);
  // Modo 2 sí acepta `anticipos: {}` real cuando no hay anticipos (a diferencia
  // de Modo 1) — mismo criterio que _buildCobroPayloadMulti() en cobro-panel.
  const { anticipos, saldosAFavorAUsar } = buildSaldosEspeciales(formasPagoConRef);
  return {
    MotivoAutorizacion:      '',
    anotacion:               '',
    anticipos,
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
    saldosAFavorAUsar,
  };
}

module.exports = { buildDetalleFormaPago, fechaISO, buildSaldosEspeciales, buildPayloadSingle, buildPayloadMulti };
