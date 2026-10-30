'use strict';

/**
 * kore-remisiones.service.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Cliente, tipo de pago y estatus de remisiones de Kore con memoria permanente
 * en NUMO (colección `kore_remisiones`, 2026-10-29). Antes cada reporte "Por
 * facturar" / hoja MOSTRADOR preguntaba a /cuentas-pendientes por CADA
 * remisión (cientos por reporte → 429 de Kore, depósitos sin estatus, caso
 * 044114). Esos tres datos no cambian, así que solo se consultan la primera
 * vez; el saldo sí cambia y se pide a Kore únicamente cuando el llamador lo
 * necesita fresco (`necesitaSaldo`).
 */

const KoreRemision = require('./KoreRemision.model');
const { obtenerCuentaTicket } = require('./erp-sync.service');

// Kore limita /cuentas-pendientes (429 con 8 a la vez en prod, 2026-10-22).
const CONCURRENCIA = 2;

const _clave = (serie, folio) => `${String(serie ?? '').trim().toUpperCase()}|${String(folio ?? '').trim()}`;

/**
 * @param {object}   p
 * @param {string}   p.rfc
 * @param {object[]} p.remisiones  [{ serie, folio, fechaCreacion }] (fechaCreacion la pide Kore)
 * @param {(clave: string) => boolean} [p.necesitaSaldo]  `SERIE|FOLIO` cuyo saldo / factura
 *   hay que pedir fresco a Kore aunque ya esté guardada
 * @returns {Promise<{ info: Map<string, { nombre, tipoPago, nombreTipoMovimiento, saldoActual?, factura? }>, sinRespuesta: number, consultadas: number }>}
 *   `info` por `SERIE|FOLIO`; `saldoActual` / `factura` solo vienen si se consultó a Kore.
 */
async function infoRemisiones({ rfc, remisiones, necesitaSaldo = () => false }) {
  const info = new Map();
  const unicas = [...new Map((remisiones ?? []).filter(r => r?.serie && r?.folio).map(r => [_clave(r.serie, r.folio), r])).entries()];
  if (!rfc || !unicas.length) return { info, sinRespuesta: 0, consultadas: 0 };

  let guardadas = [];
  try {
    guardadas = await KoreRemision.find({
      rfc, $or: unicas.map(([, r]) => ({ serie: String(r.serie).trim().toUpperCase(), folio: String(r.folio).trim() })),
    }).lean();
  } catch {
    // Sin la memoria se consulta todo a Kore, como antes.
  }
  for (const g of guardadas) {
    if (!g.nombreTipoMovimiento) continue; // incompleta → se vuelve a pedir
    info.set(_clave(g.serie, g.folio), { nombre: g.nombre, tipoPago: g.tipoPago, nombreTipoMovimiento: g.nombreTipoMovimiento });
  }

  const pedir = unicas.filter(([k]) => !info.has(k) || necesitaSaldo(k));
  let sinRespuesta = 0;
  const nuevas = [];
  for (let i = 0; i < pedir.length; i += CONCURRENCIA) {
    await Promise.all(pedir.slice(i, i + CONCURRENCIA).map(async ([k, r]) => {
      const c = await obtenerCuentaTicket({ rfc, serie: r.serie, folio: r.folio, fechaCreacion: r.fechaCreacion }).catch(() => null);
      if (!c) { sinRespuesta++; return; }
      info.set(k, c);
      if (c.nombreTipoMovimiento) nuevas.push({ k, r, c });
    }));
  }
  if (nuevas.length) {
    await KoreRemision.bulkWrite(nuevas.map(({ r, c }) => ({
      updateOne: {
        filter: { rfc, serie: String(r.serie).trim().toUpperCase(), folio: String(r.folio).trim() },
        update: { $set: { nombre: c.nombre ?? null, tipoPago: c.tipoPago ?? null, nombreTipoMovimiento: c.nombreTipoMovimiento, consultadoEn: new Date() } },
        upsert: true,
      },
    })), { ordered: false }).catch(() => {});
  }
  return { info, sinRespuesta, consultadas: pedir.length };
}

module.exports = { infoRemisiones };
