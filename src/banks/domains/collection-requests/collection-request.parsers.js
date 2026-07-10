'use strict';

// collection-request.parsers.js — validación/normalización de lo que manda el
// ERP (Kore) al crear una solicitud de cobro. Extraído de
// collection-request.service.js para separar "parseo de entrada" del resto
// del flujo (payloads de Kore, cálculo de erpLinks, CRUD).

const { BadRequestError } = require('../../shared/errors/AppError');

// Saldo(s) a favor / anticipo(s) que el ERP dice haber usado para cubrir una
// forma de pago específica — ver formasPago[].saldosAplicados en el modelo.
function parseSaldosAplicados(raw) {
  if (!Array.isArray(raw)) return [];
  return raw.map(s => ({
    id:    String(s?.id ?? '').trim(),
    monto: Number(s?.monto),
  }));
}

function parseFormasPago(raw) {
  const arr = typeof raw === 'string' ? JSON.parse(raw) : raw;
  if (!Array.isArray(arr) || arr.length === 0) {
    throw new BadRequestError('formasPago debe ser un arreglo con al menos una forma de pago');
  }
  return arr.map(f => ({
    formaPagoId:          String(f.formaPagoId ?? '').trim(),
    formaPagoDescripcion: String(f.formaPagoDescripcion ?? '').trim(),
    importe:              Number(f.importe),
    // referencia NUNCA se acepta del ERP, aunque la mande — Numo la asigna con
    // el folio del BankMovement identificado al aplicar el cobro (ver modelo).
    referencia:           null,
    bancoKoreId:          f.bancoKoreId      ? String(f.bancoKoreId).trim()      : null,
    bancoDescripcion:     f.bancoDescripcion ? String(f.bancoDescripcion).trim() : null,
    saldosAplicados:      parseSaldosAplicados(f.saldosAplicados),
  }));
}

// Modo 1 (1 CxC) o Modo 2 (N CxC + 1 sola forma de pago) — mismo shape de CxC
// en ambos casos, ver CollectionRequest.model.js.
function parseCxcs(raw) {
  const arr = typeof raw === 'string' ? JSON.parse(raw) : raw;
  if (!Array.isArray(arr) || arr.length === 0) {
    throw new BadRequestError('cxcs debe ser un arreglo con al menos una CxC');
  }
  return arr.map(c => ({
    erpId:                String(c.erpId ?? '').trim(),
    serie:                c.serie                ? String(c.serie).trim()                : null,
    folioExterno:         c.folioExterno         ? String(c.folioExterno).trim()         : null,
    folioFiscal:          c.folioFiscal          ? String(c.folioFiscal).trim()          : null,
    total:                c.total != null        ? Number(c.total)                       : null,
    tipoPago:             c.tipoPago             ? String(c.tipoPago).trim().toUpperCase() : null,
    nombrePersona:        c.nombrePersona        ? String(c.nombrePersona).trim()        : null,
    nombreTipoMovimiento: c.nombreTipoMovimiento ? String(c.nombreTipoMovimiento).trim().toUpperCase() : null,
    montoAsignado:        c.montoAsignado != null ? Number(c.montoAsignado) : null,
  }));
}

module.exports = { parseFormasPago, parseCxcs };
