'use strict';

const axios = require('axios');

const ERP_CAJA_BASE_URL = (process.env.ERP_CAJA_BASE_URL || '').replace(/\/$/, '');
const ERP_TOKEN         = process.env.ERP_TOKEN || '';

async function sincronizarCuentasPendientes(params = {}) {
  if (!ERP_CAJA_BASE_URL) {
    throw new Error('ERP no configurado (ERP_CAJA_BASE_URL ausente)');
  }

  const queryParams = {};
  if (params.fechaDesde)    queryParams.fechaDesde    = params.fechaDesde;
  if (params.fechaHasta)    queryParams.fechaHasta    = params.fechaHasta;
  if (params.estadoCobro)   queryParams.estadoCobro   = params.estadoCobro;
  if (params.serieExterna)  queryParams.serieExterna  = String(params.serieExterna).trim();
  if (params.folioExterno)  queryParams.folioExterno  = String(params.folioExterno).trim();
  if (params.nombrePersona) queryParams.nombrePersona = String(params.nombrePersona).trim();

  let response;
  try {
    response = await axios.get(`${ERP_CAJA_BASE_URL}/cuentas-pendientes`, {
      params:  queryParams,
      headers: { Authorization: `Bearer ${ERP_TOKEN}` },
      timeout: 15000,
    });
  } catch (axErr) {
    const status = axErr.response?.status;
    const body   = JSON.stringify(axErr.response?.data ?? {});
    const { logger } = require('../../../shared/utils/logger');
    logger.error(`[ErpSync] ERP /cuentas-pendientes ${status}: ${body} | params=${JSON.stringify(queryParams)}`);
    throw axErr;
  }

  const raw = response.data?.Data?.cuentas || [];
  return { raw };
}

// ── Reintento con backoff para 429 ──────────────────────────────────────────
// El ERP regresa "retry after: X segundos" en el cuerpo del 429 (campo Data).
// Antes un solo 429 tronaba toda la generación de la póliza — ahora se
// espera el tiempo indicado (+1s de margen, tope 60s) y se reintenta, hasta
// MAX_INTENTOS veces (confirmado con el usuario 2026-08-05, después de que
// generar las 11 sucursales de un mismo periodo saturó el ERP).
const MAX_INTENTOS_429 = 3;
async function _getConReintento(url, params, logLabel) {
  for (let intento = 1; intento <= MAX_INTENTOS_429; intento++) {
    try {
      return await axios.get(url, {
        params,
        headers: { Authorization: `Bearer ${ERP_TOKEN}` },
        timeout: 15000,
      });
    } catch (axErr) {
      const status = axErr.response?.status;
      if (status === 429 && intento < MAX_INTENTOS_429) {
        const dataMsg   = String(axErr.response?.data?.Data ?? '');
        const match     = /retry after:\s*([\d.]+)/i.exec(dataMsg);
        const esperaSeg = Math.min((match ? Number(match[1]) : 10) + 1, 60);
        const { logger } = require('../../../shared/utils/logger');
        logger.warn(`[ErpSync] 429 en ${logLabel}, reintentando en ${esperaSeg.toFixed(1)}s (intento ${intento}/${MAX_INTENTOS_429})`);
        await new Promise(r => setTimeout(r, esperaSeg * 1000));
        continue;
      }
      throw axErr;
    }
  }
}

// ── Caché en memoria por lote de serie/folio ────────────────────────────────
// Generar las N sucursales de un mismo periodo repetía, para cada una, la
// MISMA consulta al ERP (mismo universo de CFDIs company-wide, ver
// `_fetchCfdisParaPuenteAmplio` en cfdi-poliza-generator.service.js) —
// multiplicaba la carga ×N y saturaba el ERP con 429 (confirmado con el
// usuario 2026-08-05). Se cachea por la clave de los PARES serie|folio
// consultados (ordenados, no depende del orden en que llegue cada lote) —
// TTL corto: cubre una sesión de generación de todas las sucursales sin
// arriesgar servir datos desactualizados por mucho tiempo.
//
// `rfc` es OBLIGATORIO en la clave — aunque hoy este sistema solo maneja un
// RFC, dos empresas distintas numeran folios por su cuenta y perfectamente
// podrían coincidir en "serie I0, folio 183"; sin el RFC en la clave, una
// respondería con los datos de la OTRA (confirmado el riesgo con el usuario
// 2026-08-05, corregido antes de que llegara a pasar).
const TTL_CACHE_MS = 20 * 60 * 1000;
const _cacheAlmacen     = new Map(); // clave → { data, ts }
const _cacheSaldosFavor = new Map();

function _claveLote(rfc, series, folios) {
  const pares = series.map((s, i) => `${s}|${folios[i]}`).sort();
  return `${rfc}::${pares.join(',')}`;
}

function _leerCache(cache, clave) {
  const entry = cache.get(clave);
  if (!entry) return undefined;
  if (Date.now() - entry.ts >= TTL_CACHE_MS) { cache.delete(clave); return undefined; }
  return entry.data;
}

// Consulta, en lote, el desglose de cobros de almacén hechos en OTRAS
// sucursales para una lista de documentos (serie+folio del "documento
// relacionado" del CFDI, no el serie/folio propio de la factura). Usado por
// cobros-sucursal-puente.service.js para armar las líneas de Caja/Bancos
// por identificar en la póliza de Ingreso.
async function obtenerDesglosesCobroAlmacen({ rfc, series, folios }) {
  if (!ERP_CAJA_BASE_URL) {
    throw new Error('ERP no configurado (ERP_CAJA_BASE_URL ausente)');
  }
  if (!rfc) throw new Error('obtenerDesglosesCobroAlmacen: rfc requerido (aísla la caché por empresa)');
  if (!series?.length || !folios?.length) return [];

  const clave = _claveLote(rfc, series, folios);
  const cacheado = _leerCache(_cacheAlmacen, clave);
  if (cacheado !== undefined) return cacheado;

  let response;
  try {
    response = await _getConReintento(`${ERP_CAJA_BASE_URL}/desgloses-cobro/almacen`, {
      series: series.join(','),
      folios: folios.join(','),
    }, '/desgloses-cobro/almacen');
  } catch (axErr) {
    const status = axErr.response?.status;
    const body   = JSON.stringify(axErr.response?.data ?? {});
    const { logger } = require('../../../shared/utils/logger');
    logger.error(`[ErpSync] ERP /desgloses-cobro/almacen ${status}: ${body} | series=${series.join(',')} folios=${folios.join(',')}`);
    throw axErr;
  }

  const cuentas = response.data?.Data?.cuentas || [];
  _cacheAlmacen.set(clave, { data: cuentas, ts: Date.now() });
  return cuentas;
}

// Consulta, en lote, saldos a favor GENERADOS (por una Devolución) y USADOS
// (aplicados a otra venta) para una lista de ventas (serie+folio interno,
// mismo `serieVenta`/`folioVenta` que `obtenerDesglosesCobroAlmacen`). Usado
// por cfdi-poliza-generator.service.js para registrar el pasivo de saldo a
// favor cuando una Devolución lo genera (confirmado con el usuario
// 2026-08-04) — antes solo se inferían de forma heurística desde el texto de
// `formasPago` en /desgloses-cobro/almacen, sin saber a qué Devolución/venta
// remontaban.
async function obtenerSaldosFavor({ rfc, series, folios }) {
  if (!ERP_CAJA_BASE_URL) {
    throw new Error('ERP no configurado (ERP_CAJA_BASE_URL ausente)');
  }
  if (!rfc) throw new Error('obtenerSaldosFavor: rfc requerido (aísla la caché por empresa)');
  if (!series?.length || !folios?.length) return [];

  const clave = _claveLote(rfc, series, folios);
  const cacheado = _leerCache(_cacheSaldosFavor, clave);
  if (cacheado !== undefined) return cacheado;

  let response;
  try {
    response = await _getConReintento(`${ERP_CAJA_BASE_URL}/desgloses-cobro/saldos-favor`, {
      series: series.join(','),
      folios: folios.join(','),
    }, '/desgloses-cobro/saldos-favor');
  } catch (axErr) {
    const status = axErr.response?.status;
    const body   = JSON.stringify(axErr.response?.data ?? {});
    const { logger } = require('../../../shared/utils/logger');
    logger.error(`[ErpSync] ERP /desgloses-cobro/saldos-favor ${status}: ${body} | series=${series.join(',')} folios=${folios.join(',')}`);
    throw axErr;
  }

  const cuentas = response.data?.Data?.cuentas || [];
  _cacheSaldosFavor.set(clave, { data: cuentas, ts: Date.now() });
  return cuentas;
}

module.exports = { sincronizarCuentasPendientes, obtenerDesglosesCobroAlmacen, obtenerSaldosFavor };
