'use strict';

/**
 * reportes-por-facturar.abonos.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Historial de abonos de cada remisión en TODAS las pestañas del reporte "Por
 * facturar" (2026-11-10, pedido del usuario: "que se muestren los abonos, la
 * fecha y el movimiento de todos los abonos hasta que quedó saldada"). Debajo
 * de cada renglón, un renglón por abono (fecha, movimiento de Kore, forma,
 * importe, saldo) y al final "SALDADA el …" o lo que todavía debe.
 *
 * Fuente: `movimientos` de /cuentas-pendientes de Kore (ver _abonosDeCuenta
 * en erp-sync.service). Se guarda en kore_remisiones: una remisión ya
 * liquidada no se vuelve a preguntar; la que aún debe, sí (puede tener abonos
 * nuevos).
 */

const { obtenerDesglosesCobroAlmacen } = require('../erp/erp-sync.service');
const { infoRemisiones } = require('../erp/kore-remisiones.service');

const LOTE = 50;
const _serie = v => String(v ?? '').trim().toUpperCase();

// Todas las filas del reporte que llevan remisiones.
function _filas(datos) {
  return [
    ...(datos.movimientosMostrador ?? []).flatMap(f => f.depositos ?? []),
    ...(datos.cobrosCajaGlobales ?? []).flatMap(f => f.depositos ?? []),
    ...(datos.depositosPendientesFactura ?? []),
    ...(datos.depositosPendientesPago ?? []),
    ...(datos.cobrosOtraCaja ?? []),
  ].filter(d => d.remisiones?.length);
}

const _partir = r => { const i = r.indexOf('-'); return { serie: _serie(r.slice(0, i)), folio: r.slice(i + 1).trim() }; };

/**
 * Agrega `historialAbonos` a cada fila del reporte (muta `datos`):
 *   [{ remision, abonos: [{ fecha, movimiento, forma, importe, saldo, idNumo }], saldo, saldadaEl }]
 * Solo las remisiones con al menos un abono.
 * @returns {Promise<{ sinRespuesta: number }>}
 */
async function agregarHistorialAbonos({ rfc, datos }) {
  const filas = _filas(datos);
  const claves = [...new Set(filas.flatMap(d => d.remisiones))];
  const remisiones = claves.map(_partir).filter(r => r.serie && r.folio);
  if (!remisiones.length) return { sinRespuesta: 0 };

  // Lo guardado y ya liquidado no se vuelve a pedir. Para preguntar a Kore
  // hace falta la fecha de creación (sale de /desgloses-cobro/almacen), así que
  // primero se ve qué falta.
  const memoria = await _memoria(rfc, remisiones);
  const faltan = remisiones.filter(r => { const g = memoria.get(`${r.serie}|${r.folio}`); return !(g?.liquidadaEl && Array.isArray(g.abonos)); });

  const fechaCreacion = new Map();
  let sinRespuesta = 0;
  for (let i = 0; i < faltan.length; i += LOTE) {
    const lote = faltan.slice(i, i + LOTE);
    try {
      const cuentas = await obtenerDesglosesCobroAlmacen({ rfc, series: lote.map(r => r.serie), folios: lote.map(r => r.folio) });
      for (const c of cuentas ?? []) fechaCreacion.set(`${_serie(c.serieVenta)}|${String(c.folioVenta).trim()}`, c.fechaCreacion);
    } catch {
      sinRespuesta += lote.length;
    }
  }

  const { info, sinRespuesta: sinCuenta } = await infoRemisiones({
    rfc,
    remisiones: remisiones.map(r => ({ ...r, fechaCreacion: fechaCreacion.get(`${r.serie}|${r.folio}`) ?? null })),
    // Se pregunta si no está liquidada o si nunca se guardaron sus abonos.
    necesitaSaldo: (k, g) => !(g?.liquidadaEl && Array.isArray(g?.abonos)) && fechaCreacion.has(k),
  });
  sinRespuesta += sinCuenta;

  const historialDe = new Map();
  for (const r of claves) {
    const { serie, folio } = _partir(r);
    const c = info.get(`${serie}|${folio}`);
    const abonos = (c?.abonos ?? []).map(a => ({
      // Sin forma de pago (RET / bonificación) → su origen en Kore (BON-324649).
      fecha: a.fecha ?? null, movimiento: a.movimiento, forma: a.forma ?? (a.origen ? `DE ${a.origen}` : null),
      importe: a.importe, saldo: a.saldo, idNumo: a.idNumo ?? null,
    }));
    if (!abonos.length) continue;
    const saldo = abonos[abonos.length - 1].saldo;
    historialDe.set(r, {
      remision:  r,
      abonos,
      saldo,
      saldadaEl: Math.abs(Number(saldo) || 0) < 0.005 ? (c.liquidadaEl ?? abonos[abonos.length - 1].fecha) : null,
    });
  }
  for (const d of filas) {
    const h = d.remisiones.map(r => historialDe.get(r)).filter(Boolean);
    if (h.length) d.historialAbonos = h;
  }
  return { sinRespuesta };
}

// kore_remisiones guardadas (con abonos y liquidadaEl), por `SERIE|FOLIO`.
async function _memoria(rfc, remisiones) {
  const KoreRemision = require('../erp/KoreRemision.model');
  const mapa = new Map();
  try {
    const gs = await KoreRemision.find({ rfc, $or: remisiones.map(r => ({ serie: r.serie, folio: r.folio })) })
      .select('serie folio liquidadaEl abonos').lean();
    for (const g of gs) mapa.set(`${_serie(g.serie)}|${String(g.folio).trim()}`, g);
  } catch {
    // Sin memoria → se pregunta todo.
  }
  return mapa;
}

module.exports = { agregarHistorialAbonos };
