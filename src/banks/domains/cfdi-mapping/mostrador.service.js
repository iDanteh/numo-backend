'use strict';

/**
 * mostrador.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Hoja "MOSTRADOR" de la póliza de Ingreso (2026-10-21, pedido del usuario con
 * su PDF "MOSTRADOR 20 OCT 2026", factura A0-261009872).
 *
 * Una factura de mostrador agrupa remisiones que se cobraron DÍAS ANTES
 * (depósitos y transferencias ya identificados en Bancos) y se factura
 * después. El generador solo busca cobros del día de la factura, así que la
 * deja como "Venta Sin Cobro" — y esa línea no sale en el export (ver
 * poliza.service.js), por lo que nada de ella aparecía en el Excel.
 *
 * Aquí se listan, por cada factura con "Venta Sin Cobro", los depósitos de
 * Bancos ligados a ella (`erpLinks[].folioFiscal` = UUID de la factura) —
 * informativo, NUNCA se contabiliza (mismo criterio que "Movimientos PPD por
 * facturar"), con estatus "POR FACTURAR".
 */

const BankMovement = require('../banks/BankMovement.model');
const { obtenerDesglosesCobroAlmacen, obtenerNombrePersonaTicket } = require('../erp/erp-sync.service');

const TIPO_ORIGEN_VENTA_SIN_COBRO = 'Venta Sin Cobro';
const ESTATUS_MOSTRADOR = 'POR FACTURAR';

const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;

// Nombre del cliente de cada remisión (`serie|folio` → nombre) vía
// `/cuentas-pendientes`, que necesita la fecha de creación de la remisión
// (sale de `/desgloses-cobro/almacen`). Si Kore falla, el nombre queda vacío.
async function _nombresPorRemision(rfc, remisiones) {
  const nombres = new Map();
  if (!remisiones.length) return nombres;
  let cuentas = [];
  try {
    cuentas = await obtenerDesglosesCobroAlmacen({ rfc, series: remisiones.map(r => r.serie), folios: remisiones.map(r => r.folio) });
  } catch {
    return nombres;
  }
  await Promise.all((cuentas ?? []).map(async c => {
    const nombre = await obtenerNombrePersonaTicket({ rfc, serie: c.serieVenta, folio: c.folioVenta, fechaCreacion: c.fechaCreacion }).catch(() => null);
    if (nombre) nombres.set(`${c.serieVenta}|${c.folioVenta}`, nombre);
  }));
  return nombres;
}

/**
 * @param {object}   p
 * @param {string}   p.rfc
 * @param {object[]} p.cfdis        CFDIs de la póliza (uuid, serie, folio, fecha, total)
 * @param {object[]} p.movimientos  movimientos generados (se usan los "Venta Sin Cobro")
 * @returns {Promise<object[]>} una entrada por factura:
 *   { uuid, serie, folio, fechaFactura, totalFactura, totalAplicado, depositos: [{ fecha, descripcion, importe, aplicado, banco, remisiones, cliente, idNumo, estatus }] }
 */
async function detectarMovimientosMostrador({ rfc, cfdis, movimientos }) {
  const uuidsSinCobro = new Set(movimientos
    .filter(m => m.tipoOrigen === TIPO_ORIGEN_VENTA_SIN_COBRO && Number(m.debe) > 0 && m.cfdiUuid)
    .map(m => m.cfdiUuid.toUpperCase()));
  if (!uuidsSinCobro.size) return [];

  const cfdiPorUuid = new Map(cfdis.filter(c => c.uuid).map(c => [c.uuid.toUpperCase(), c]));
  const variantes = [...uuidsSinCobro].flatMap(u => [u, u.toLowerCase()]);
  const bancos = await BankMovement.find({ 'erpLinks.folioFiscal': { $in: variantes }, isActive: { $ne: false } }).lean();
  if (!bancos.length) return [];

  const porFactura = new Map();
  for (const b of bancos) {
    const linksPorUuid = new Map();
    for (const l of b.erpLinks ?? []) {
      const u = (l.folioFiscal ?? '').toUpperCase();
      if (!uuidsSinCobro.has(u)) continue;
      if (!linksPorUuid.has(u)) linksPorUuid.set(u, []);
      linksPorUuid.get(u).push(l);
    }
    for (const [u, links] of linksPorUuid) {
      if (!porFactura.has(u)) porFactura.set(u, []);
      porFactura.get(u).push({
        fecha:       b.fecha ?? null,
        descripcion: b.concepto ?? '',
        importe:     _r2(b.deposito),
        aplicado:    _r2(links.reduce((s, l) => s + (Number(l.saldoPagado ?? l.saldoPagadoTotal) || 0), 0)),
        banco:       b.banco ?? '',
        remisiones:  [...new Set(links.map(l => `${l.serie ?? ''}-${l.folioExterno ?? ''}`))],
        idNumo:      b.folio ?? '',
        estatus:     ESTATUS_MOSTRADOR,
      });
    }
  }

  const remisiones = [...new Set([...porFactura.values()].flat().flatMap(d => d.remisiones))]
    .map(r => { const i = r.indexOf('-'); return { serie: r.slice(0, i), folio: r.slice(i + 1) }; })
    .filter(r => r.serie && r.folio);
  const nombres = await _nombresPorRemision(rfc, remisiones);

  const resultado = [];
  for (const [u, depositos] of porFactura) {
    const cfdi = cfdiPorUuid.get(u) ?? {};
    for (const d of depositos) {
      d.cliente = [...new Set(d.remisiones.map(r => nombres.get(r.replace('-', '|'))).filter(Boolean))].join(' / ');
    }
    depositos.sort((a, b) => new Date(a.fecha ?? 0) - new Date(b.fecha ?? 0) || String(a.idNumo).localeCompare(String(b.idNumo)));
    resultado.push({
      uuid:          u,
      serie:         cfdi.serie ?? null,
      folio:         cfdi.folio ?? null,
      fechaFactura:  cfdi.fecha ?? null,
      totalFactura:  _r2(cfdi.total),
      totalAplicado: _r2(depositos.reduce((s, d) => s + d.aplicado, 0)),
      depositos,
    });
  }
  return resultado.sort((a, b) => String(a.folio).localeCompare(String(b.folio)));
}

module.exports = { detectarMovimientosMostrador, ESTATUS_MOSTRADOR };
