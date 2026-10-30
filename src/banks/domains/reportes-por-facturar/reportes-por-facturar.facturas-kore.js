'use strict';

/**
 * reportes-por-facturar.facturas-kore.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Facturas del día directo de Kore (2026-10-30, pedido del usuario: "¿no se
 * puede solo consultar las globales del día cuando lo generemos?"). El reporte
 * lee las facturas de la copia de CFDIs de NUMO, que se llena con las
 * descargas nocturnas (SAT ~00:44, Kore ~02:52) → las facturas de HOY (y las
 * de ayer antes de esas descargas) no salían. Caso real: global A0-261014834
 * timbrada 29-oct 11:40, entró a NUMO el 30-oct 00:44 → los reportes 39/40
 * generados el 29 a las 17:08 no la traían.
 *
 * Al generar / actualizar, si el rango incluye hoy o ayer (hora México), se
 * piden a Kore las facturas de esos días y se guardan igual que la descarga
 * nocturna (`upsertFromERP`, copia ERP) — solo Ingreso de las series del
 * reporte. La descarga nocturna después solo las encuentra ya guardadas.
 */

const CFDI = require('../../../visor/models/CFDI');
const { fetchTodasLasFacturas } = require('../../../visor/services/erp.service');
const { transformarTolerante } = require('../../../visor/services/erp-transformer.service');
const { upsertFromERP } = require('../../../visor/repositories/cfdi.repository');
const { aplicarReclasificacion } = require('../../../visor/services/reclasificacionGlobal.service');
const { logger } = require('../../../shared/utils/logger');

const _hoyMx = (ahora = new Date()) => new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Mexico_City' }).format(ahora);
const _sumarDias = (dia, n) => new Date(new Date(`${dia}T00:00:00Z`).getTime() + n * 86400000).toISOString().slice(0, 10);

/**
 * Días del rango que la descarga nocturna todavía no pudo traer: hoy y ayer.
 * @returns {string[]} YYYY-MM-DD
 */
function diasRecientes(fechaInicio, fechaFin, ahora = new Date()) {
  const hoy = _hoyMx(ahora);
  return [_sumarDias(hoy, -1), hoy].filter(d => d >= fechaInicio && d <= fechaFin);
}

/**
 * Trae de Kore y guarda las facturas de Ingreso de `series` de los días
 * recientes del rango. No truena el reporte: si Kore no responde, lo regresa
 * en `error` (el reporte sale con lo que ya hay en NUMO + advertencia).
 * @returns {Promise<{ dias: string[], nuevas: number, error: string|null }>}
 */
async function traerFacturasRecientes({ series, fechaInicio, fechaFin, ahora = new Date() }) {
  const dias = diasRecientes(fechaInicio, fechaFin, ahora);
  if (!dias.length) return { dias, nuevas: 0, error: null };

  const seriesSet = new Set(series.map(s => String(s).trim().toUpperCase()));
  let nuevas = 0;
  const periodos = new Set();
  try {
    for (const dia of dias) {
      const [ejercicio, periodo] = dia.split('-').map(Number);
      // Día completo en hora de México (mismo huso que la descarga nocturna).
      const facturas = await fetchTodasLasFacturas({ fechaInicio: `${dia}T06:00:00Z`, fechaFin: `${_sumarDias(dia, 1)}T05:59:59Z` });
      for (const factura of facturas) {
        let doc;
        try {
          ({ doc } = transformarTolerante(factura, { ejercicio, periodo, uploadedBy: 'system' }));
        } catch {
          continue;
        }
        if (doc.tipoDeComprobante !== 'I' || !seriesSet.has(String(doc.serie ?? '').trim().toUpperCase())) continue;
        // Ya guardada → no se toca (la descarga nocturna la mantiene).
        if (await CFDI.exists({ uuid: doc.uuid, source: 'ERP' })) continue;
        const { isNew } = await upsertFromERP(doc);
        if (isNew) { nuevas++; periodos.add(`${ejercicio}|${periodo}`); }
      }
    }
  } catch (err) {
    logger.warn(`[ReportePorFacturar] No se pudieron traer de Kore las facturas de ${dias.join(', ')}: ${err.message}`);
    return { dias, nuevas, error: err.message };
  }

  // Igual que la descarga nocturna: con facturas nuevas, reclasificar
  // globales (si no, la nocturna ya no las ve como nuevas y no lo haría).
  for (const p of periodos) {
    const [ejercicio, periodo] = p.split('|').map(Number);
    await aplicarReclasificacion({ ejercicio, periodo, source: 'ERP' })
      .catch(err => logger.warn(`[ReportePorFacturar] Reclasificación ${periodo}/${ejercicio} falló (no crítico): ${err.message}`));
  }
  return { dias, nuevas, error: null };
}

module.exports = { traerFacturasRecientes, diasRecientes };
