'use strict';

/**
 * reportes-por-facturar.otra-caja.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Pestaña "A0 COBRADAS EN PUERTO" del reporte "Por facturar" (2026-10-30,
 * pedido del usuario: "si las A0 se cobraron en O0, otra pestaña que diga A0
 * cobradas en Puerto con esa información"). De las remisiones que salen en el
 * reporte, los cobros que Kore registró en la caja de OTRO almacén
 * (`claveCentro` del cobro ≠ serie de la remisión) — p. ej. A0-261003322
 * cobrada en la caja O0 el 21-oct (CPF-261003893, EFECTIVO $689.44). En el
 * reporte 41 (27–29 oct) eran 37 remisiones A0, todas en O0.
 */

const { obtenerDesglosesCobroAlmacen } = require('../erp/erp-sync.service');
const { formasPagoDeCuentas } = require('../cfdi-mapping/mostrador.service');

const LOTE = 50;
const TOLERANCIA = 0.05;
const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;
const _serie = v => String(v ?? '').trim().toUpperCase();

// Todas las filas del reporte (depósitos y cobros de Kore) con sus remisiones.
function _filasDelReporte(datos) {
  return [
    ...(datos.movimientosMostrador ?? []).flatMap(f => (f.depositos ?? []).map(d => ({ ...d, factura: `${f.serie}-${f.folio}` }))),
    ...(datos.cobrosCajaGlobales ?? []).flatMap(f => (f.depositos ?? []).map(d => ({ ...d, factura: `${f.serie}-${f.folio}` }))),
    ...(datos.depositosPendientesFactura ?? []),
    ...(datos.depositosPendientesPago ?? []),
  ];
}

/**
 * @param {object} p
 * @param {string} p.rfc
 * @param {object} p.datos  datos del reporte (movimientosMostrador, cobrosCajaGlobales, pendientes…)
 * @returns {Promise<{ cobros: object[], sinRespuesta: number }>}
 *   cobros: [{ fecha, descripcion, importe, banco, remisiones, factura, cliente, idNumo, serie, caja, deKore }]
 */
async function cobrosEnOtraCaja({ rfc, datos }) {
  const filas = _filasDelReporte(datos);
  const porRemision = new Map(); // 'A0-261003322' → filas del reporte con esa remisión
  for (const d of filas) {
    for (const r of d.remisiones ?? []) {
      if (!porRemision.has(r)) porRemision.set(r, []);
      porRemision.get(r).push(d);
    }
  }
  const remisiones = [...porRemision.keys()].map(r => { const i = r.indexOf('-'); return { serie: _serie(r.slice(0, i)), folio: r.slice(i + 1).trim() }; })
    .filter(r => r.serie && r.folio);

  const cobros = [];
  let sinRespuesta = 0;
  for (let i = 0; i < remisiones.length; i += LOTE) {
    const lote = remisiones.slice(i, i + LOTE);
    let cuentas;
    try {
      cuentas = await obtenerDesglosesCobroAlmacen({ rfc, series: lote.map(r => r.serie), folios: lote.map(r => r.folio) });
    } catch {
      sinRespuesta += lote.length;
      continue;
    }
    for (const c of cuentas ?? []) {
      const serie = _serie(c.serieVenta);
      const rem = `${serie}-${String(c.folioVenta).trim()}`;
      const delReporte = porRemision.get(rem) ?? [];
      const cliente = delReporte.map(d => d.cliente).find(Boolean) ?? '';
      const factura = c.serieFactura ? `${_serie(c.serieFactura)}-${String(c.folioFactura ?? '').trim()}` : 'POR FACTURAR';
      for (const k of c.cobros ?? []) {
        const caja = _serie(k.claveCentro);
        if (!caja || caja === serie || !(Number(k.monto) < 0)) continue;
        const importe = _r2(Math.abs(Number(k.monto)));
        // ID NUMO del depósito de Bancos de esa remisión con el mismo importe (si lo hay).
        const deposito = delReporte.find(d => d.idNumo && d.idNumo !== 'S/R' && Math.abs((Number(d.importe) || 0) - importe) <= TOLERANCIA);
        cobros.push({
          fecha:       k.fecha ?? null,
          descripcion: `COBRO ${k.serieOrigen ?? ''}-${k.folioOrigen ?? ''} EN CAJA ${caja}`,
          importe,
          banco:       (k.formasPago ?? []).map(fp => fp.nombre).filter(Boolean).join(' + '),
          formasPago:  formasPagoDeCuentas([{ cobros: [k] }]),
          remisiones:  [rem],
          factura,
          cliente,
          idNumo:      deposito?.idNumo ?? 'S/R',
          serie,
          caja,
          deKore:      true,
        });
      }
    }
  }
  cobros.sort((a, b) => a.serie.localeCompare(b.serie) || a.caja.localeCompare(b.caja) || new Date(a.fecha) - new Date(b.fecha));
  return { cobros, sinRespuesta };
}

module.exports = { cobrosEnOtraCaja };
