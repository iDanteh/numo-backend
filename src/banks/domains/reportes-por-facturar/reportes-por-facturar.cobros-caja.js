'use strict';

/**
 * reportes-por-facturar.cobros-caja.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Renglones "EFECTIVO" y "SALDO A FAVOR" de cada factura global en la hoja
 * MOSTRADOR del reporte "Por facturar" (2026-10-29, pedido del usuario con su
 * Excel "MOSTRADOR 2026": "Kore ya lo tiene"). Son cobros de las remisiones
 * PPD "POR FACTURAR" de la global que NO son un depósito de Bancos:
 *
 *   • EFECTIVO: cobro en caja con forma EFECTIVO (clave 01, no "DEPÓSITO EN
 *     EFECTIVO"). Si ese efectivo ya se depositó y el depósito está ligado en
 *     Bancos a la factura o a la remisión (mismo importe), ya sale como
 *     depósito y no se repite.
 *   • SALDO A FAVOR: forma de pago saldo a favor / anticipo, o un abono (ABO)
 *     de un pago que NO es ningún depósito de Bancos de la factura ni de la
 *     remisión (p. ej. A0-260501745: abono de $242.06 de una transferencia de
 *     $1,500 del 15-jul que no está en la global A0-261002123). Los abonos de
 *     un mismo pago se juntan en un renglón (I0-261000245: $9,089.77 con las
 *     remisiones I0-260800407 e I0-260800408).
 *
 * Verificado contra el Excel del usuario: A0-261002123 (EFECTIVO $1,500 y
 * SALDO A FAVOR $242.06), I0-261000245 (SALDO A FAVOR $9,089.77) y
 * A0-261014203 (su efectivo de $3,899.27 ya es el depósito 55151 → no se repite).
 */

const BankMovement = require('../banks/BankMovement.model');
const { obtenerDesglosesCobroPorFactura, obtenerCuentaTicket } = require('../erp/erp-sync.service');

const ESTATUS_POR_FACTURAR = 'POR FACTURAR';
// Kore limita /cuentas-pendientes (429 con 8 a la vez en prod, 2026-10-22).
const CONCURRENCIA_CUENTAS = 2;
const TOLERANCIA = 0.05;

const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;
const _diaMx = f => new Date(new Date(f).getTime() - 6 * 3600000).toISOString().slice(0, 10);
const _fechaCorta = f => { const [y, m, d] = _diaMx(f).split('-'); return `${d}/${m}/${y.slice(2)}`; };
const _esSaldoFavor = fp => /saldo\s*a\s*favor|anticipo/i.test(fp.nombre ?? fp.nombreFormaPago ?? '');
const _esEfectivo = fp => String(fp.claveSat ?? '').trim() === '01' && !/dep[oó]sito/i.test(fp.nombre ?? fp.nombreFormaPago ?? '');
const _hayImporte = (importes, monto) => importes.some(i => Math.abs(i - monto) <= TOLERANCIA);

/**
 * Cobros de caja (efectivo / saldo a favor) de las remisiones PPD por
 * facturar de cada factura global.
 * @param {object}   p
 * @param {string}   p.rfc
 * @param {object[]} p.facturas  globales: [{ uuid, serie, folio, fecha, total }]
 * @returns {Promise<{ porFactura: object[], sinRespuesta: number }>}
 *   porFactura: [{ uuid, serie, folio, fechaFactura, totalFactura, depositos: [renglón] }]
 *   — solo las facturas que tienen algún renglón.
 */
async function cobrosCajaDeGlobales({ rfc, facturas }) {
  const porFactura = [];
  let sinRespuesta = 0;
  for (const f of facturas) {
    let cuentas;
    try {
      cuentas = await obtenerDesglosesCobroPorFactura({ rfc, serieFactura: f.serie, folioFactura: f.folio });
    } catch {
      sinRespuesta++;
      continue;
    }

    // Cobros candidatos por remisión. CBT = cobro del ticket al venderse
    // (ventas de contado) → nunca es cobranza de una remisión por facturar.
    const candidatos = [];
    for (const c of cuentas ?? []) {
      const cobros = [];
      for (const k of c.cobros ?? []) {
        if (!k.fecha || String(k.serieOrigen ?? '').trim().toUpperCase() === 'CBT') continue;
        const fps = k.formasPago ?? [];
        const monto = Math.abs(Number(k.monto) || 0);
        const pago = _r2(fps.reduce((s, fp) => s + (Number(fp.monto) || 0), 0));
        const parte = fp => (fps.length === 1 ? monto : Number(fp.monto) || 0);
        for (const fp of fps) {
          if (_esSaldoFavor(fp)) cobros.push({ tipo: 'SALDO A FAVOR', k, importe: _r2(parte(fp)), forma: fp.nombre ?? '' });
          else if (_esEfectivo(fp)) cobros.push({ tipo: 'EFECTIVO', k, importe: _r2(parte(fp)) });
        }
        // Abono que es solo una parte de un pago (no efectivo): puede venir de un saldo a favor.
        if (String(k.serieOrigen ?? '').trim().toUpperCase() === 'ABO' && !fps.some(fp => _esEfectivo(fp) || _esSaldoFavor(fp)) && pago > monto + TOLERANCIA) {
          cobros.push({ tipo: 'ABONO', k, importe: _r2(monto), pago, forma: fps.map(fp => fp.nombre).filter(Boolean).join(' + ') });
        }
      }
      if (cobros.length) candidatos.push({ c, cobros });
    }
    if (!candidatos.length) continue;

    // Solo remisiones PPD "POR FACTURAR" (como el resto de la hoja MOSTRADOR).
    const info = new Map();
    for (let i = 0; i < candidatos.length; i += CONCURRENCIA_CUENTAS) {
      await Promise.all(candidatos.slice(i, i + CONCURRENCIA_CUENTAS).map(async ({ c }) => {
        const r = await obtenerCuentaTicket({ rfc, serie: c.serieVenta, folio: c.folioVenta, fechaCreacion: c.fechaCreacion }).catch(() => null);
        if (r) info.set(c, r); else sinRespuesta++;
      }));
    }
    const porFacturar = candidatos.filter(({ c }) => info.get(c)?.tipoPago === 'PPD' && info.get(c)?.nombreTipoMovimiento === ESTATUS_POR_FACTURAR);
    if (!porFacturar.length) continue;

    // Depósitos de Bancos ligados a la factura o a esas remisiones: lo que ya
    // es un depósito no se repite como efectivo / saldo a favor.
    const uuid = String(f.uuid ?? '').toUpperCase();
    const bancos = await BankMovement.find({
      isActive: { $ne: false },
      deposito: { $gt: 0 },
      $or: [
        ...(uuid ? [{ 'erpLinks.folioFiscal': { $in: [uuid, uuid.toLowerCase()] } }] : []),
        ...porFacturar.map(({ c }) => ({ erpLinks: { $elemMatch: { serie: c.serieVenta, folioExterno: c.folioVenta } } })),
      ],
    }).select('deposito erpLinks').lean();
    const importesFactura = bancos.map(b => _r2(b.deposito));

    const renglones = [];
    const abonosPorPago = new Map();
    for (const { c, cobros } of porFacturar) {
      const rem = `${c.serieVenta}-${c.folioVenta}`;
      const cliente = info.get(c)?.nombre ?? '';
      for (const x of cobros) {
        const origen = `${x.k.serieOrigen ?? ''}-${x.k.folioOrigen ?? ''}`;
        if (x.tipo === 'EFECTIVO') {
          if (_hayImporte(importesFactura, x.importe)) continue;
          renglones.push({ fecha: x.k.fecha, descripcion: `COBRO EN EFECTIVO EN CAJA ${x.k.claveCentro ?? ''} (${origen})`.replace('  ', ' '),
            importe: x.importe, banco: 'EFECTIVO', remisiones: [rem], cliente });
        } else if (x.tipo === 'SALDO A FAVOR') {
          renglones.push({ fecha: x.k.fecha, descripcion: `${x.forma || 'SALDO A FAVOR'} APLICADO (${origen})`.toUpperCase(),
            importe: x.importe, banco: 'SALDO A FAVOR', remisiones: [rem], cliente });
        } else {
          if (_hayImporte(importesFactura, x.pago)) continue;
          // Abonos del mismo pago (mismo día y monto total) → un renglón.
          const clave = `${_diaMx(x.k.fecha)}|${x.pago}|${cliente}`;
          if (!abonosPorPago.has(clave)) {
            const r = { fecha: x.k.fecha, descripcion: `SALDO A FAVOR — DE UN PAGO DE $${x.pago.toFixed(2)} (${x.forma || 'SIN FORMA'}) DEL ${_fechaCorta(x.k.fecha)}`,
              importe: 0, banco: 'SALDO A FAVOR', remisiones: [], cliente };
            abonosPorPago.set(clave, r);
            renglones.push(r);
          }
          const r = abonosPorPago.get(clave);
          r.importe = _r2(r.importe + x.importe);
          if (!r.remisiones.includes(rem)) r.remisiones.push(rem);
        }
      }
    }
    if (!renglones.length) continue;

    porFactura.push({
      uuid, serie: f.serie ?? null, folio: f.folio ?? null, fechaFactura: f.fecha ?? null, totalFactura: _r2(f.total),
      depositos: renglones
        .map(r => ({ ...r, aplicado: r.importe, idNumo: 'S/R', estatus: ESTATUS_POR_FACTURAR, ppdPorFacturar: true, deKore: true }))
        .sort((a, b) => new Date(a.fecha) - new Date(b.fecha)),
    });
  }
  return { porFactura, sinRespuesta };
}

module.exports = { cobrosCajaDeGlobales };
