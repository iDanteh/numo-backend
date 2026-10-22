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
 * facturar").
 *
 * 2026-10-22 (pedido del usuario): la hoja solo enseña (a) los depósitos de
 * facturas GLOBALES (Público en General), juntos en una tabla, y (b) los de
 * remisiones que Kore sigue teniendo como PPD "POR FACTURAR" — el resto (p.
 * ej. factura PUE a nombre del cliente, ya FACTURADO) no sale. El estatus de
 * cada depósito es el de Kore (`nombreTipoMovimiento` de su remisión).
 */

const BankMovement = require('../banks/BankMovement.model');
const { obtenerDesglosesCobroAlmacen, obtenerCuentaTicket } = require('../erp/erp-sync.service');

const TIPO_ORIGEN_VENTA_SIN_COBRO = 'Venta Sin Cobro';
const ESTATUS_POR_FACTURAR = 'POR FACTURAR';
const RFC_PUBLICO_GENERAL = 'XAXX010101000';

const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;

// Cuenta de Kore de cada remisión (`serie|folio` → { nombre, tipoPago,
// nombreTipoMovimiento }) vía `/cuentas-pendientes`, que necesita la fecha de
// creación de la remisión (sale de `/desgloses-cobro/almacen`). Si Kore falla,
// la remisión queda sin datos.
async function _cuentasPorRemision(rfc, remisiones) {
  const cuentasKore = new Map();
  if (!remisiones.length) return cuentasKore;
  let cuentas = [];
  try {
    cuentas = await obtenerDesglosesCobroAlmacen({ rfc, series: remisiones.map(r => r.serie), folios: remisiones.map(r => r.folio) });
  } catch {
    return cuentasKore;
  }
  await Promise.all((cuentas ?? []).map(async c => {
    const info = await obtenerCuentaTicket({ rfc, serie: c.serieVenta, folio: c.folioVenta, fechaCreacion: c.fechaCreacion }).catch(() => null);
    if (info) cuentasKore.set(`${c.serieVenta}|${c.folioVenta}`, info);
  }));
  return cuentasKore;
}

/**
 * @param {object}   p
 * @param {string}   p.rfc
 * @param {object[]} p.cfdis        CFDIs de la póliza (uuid, serie, folio, fecha, total)
 * @param {object[]} p.movimientos  movimientos generados (se usan los "Venta Sin Cobro")
 * @returns {Promise<object[]>} una entrada por factura:
 *   { uuid, serie, folio, fechaFactura, esGlobal, totalFactura, totalAplicado,
 *     depositos: [{ fecha, descripcion, importe, aplicado, banco, remisiones, cliente, idNumo, estatus, ppdPorFacturar }] }
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
        _tipoPagoPorRem: new Map(links.map(l => [`${l.serie ?? ''}-${l.folioExterno ?? ''}`, l.tipoPago ?? null])),
        fecha:       b.fecha ?? null,
        descripcion: b.concepto ?? '',
        importe:     _r2(b.deposito),
        aplicado:    _r2(links.reduce((s, l) => s + (Number(l.saldoPagado ?? l.saldoPagadoTotal) || 0), 0)),
        banco:       b.banco ?? '',
        remisiones:  [...new Set(links.map(l => `${l.serie ?? ''}-${l.folioExterno ?? ''}`))],
        idNumo:      b.folio ?? '',
      });
    }
  }

  const remisiones = [...new Set([...porFactura.values()].flat().flatMap(d => d.remisiones))]
    .map(r => { const i = r.indexOf('-'); return { serie: r.slice(0, i), folio: r.slice(i + 1) }; })
    .filter(r => r.serie && r.folio);
  const cuentasKore = await _cuentasPorRemision(rfc, remisiones);

  const resultado = [];
  for (const [u, depositos] of porFactura) {
    const cfdi = cfdiPorUuid.get(u) ?? {};
    for (const d of depositos) {
      const infos = d.remisiones.map(r => ({ ...cuentasKore.get(r.replace('-', '|')), tipoPagoLink: d._tipoPagoPorRem.get(r) }));
      const unicos = vals => [...new Set(vals.filter(Boolean))];
      d.cliente = unicos(infos.map(i => i.nombre)).join(' / ');
      d.estatus = unicos(infos.map(i => i.nombreTipoMovimiento)).join(' / ');
      // PPD "POR FACTURAR" en Kore (mismo criterio que `esPpdPorFacturar` del
      // generador); si Kore no regresó la cuenta, tipoPago cae al del link.
      d.ppdPorFacturar = infos.some(i => (i.tipoPago ?? i.tipoPagoLink) === 'PPD' && i.nombreTipoMovimiento === ESTATUS_POR_FACTURAR);
      delete d._tipoPagoPorRem;
    }
    depositos.sort((a, b) => new Date(a.fecha ?? 0) - new Date(b.fecha ?? 0) || String(a.idNumo).localeCompare(String(b.idNumo)));
    resultado.push({
      uuid:          u,
      serie:         cfdi.serie ?? null,
      folio:         cfdi.folio ?? null,
      fechaFactura:  cfdi.fecha ?? null,
      esGlobal:      (cfdi.receptor?.rfc ?? '').trim().toUpperCase() === RFC_PUBLICO_GENERAL,
      totalFactura:  _r2(cfdi.total),
      totalAplicado: _r2(depositos.reduce((s, d) => s + d.aplicado, 0)),
      depositos,
    });
  }
  return resultado.sort((a, b) => String(a.folio).localeCompare(String(b.folio)));
}

module.exports = { detectarMovimientosMostrador };
