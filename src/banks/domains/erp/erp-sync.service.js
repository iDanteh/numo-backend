'use strict';

const axios = require('axios');

// ERP_CAJA_BASE_TEST_URL (opcional) tiene prioridad sobre ERP_CAJA_BASE_URL cuando está
// presente — pensada para servidores de staging/test (ej. testnumo), donde un .env mal
// copiado puede pisar ERP_CAJA_BASE_URL con la URL de producción sin que nadie lo note
// (caso real, 2026-08-24: reversiones comparaban contra Kore de PRODUCCIÓN en testnumo,
// "atribución ambigua" con números que no tenían nada que ver). En producción esta
// variable simplemente no existe, así que el comportamiento no cambia ahí.
const ERP_CAJA_BASE_URL = (process.env.ERP_CAJA_BASE_TEST_URL || process.env.ERP_CAJA_BASE_URL || '').replace(/\/$/, '');
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
  if (params.origen)        queryParams.origen        = String(params.origen).trim();

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

// ── Reintento con backoff para 429 y timeouts ───────────────────────────────
// El ERP regresa "retry after: X segundos" en el cuerpo del 429 (campo Data).
// Antes un solo 429 tronaba toda la generación de la póliza — ahora se
// espera el tiempo indicado (+1s de margen, tope 60s) y se reintenta, hasta
// MAX_INTENTOS veces (confirmado con el usuario 2026-08-05, después de que
// generar las 11 sucursales de un mismo periodo saturó el ERP).
//
// Timeouts (2026-08-14, confirmado con el usuario): un timeout aislado de
// 15s en un endpoint "por centro" (consulta pesada, ej. un día completo de
// una sucursal grande) no debe hacer que el caller se rinda de inmediato y
// caiga al camino viejo/menos preciso — primero vale la pena reintentar,
// igual que ya se hace con 429, antes de darse por vencido. Backoff fijo (no
// hay "retry after" para un timeout) con un pequeño incremento por intento.
const MAX_INTENTOS_429 = 3;
async function _getConReintento(url, params, logLabel) {
  for (let intento = 1; intento <= MAX_INTENTOS_429; intento++) {
    try {
      return await axios.get(url, {
        params,
        headers: { Authorization: `Bearer ${ERP_TOKEN}` },
        timeout: 30000,
      });
    } catch (axErr) {
      const status    = axErr.response?.status;
      const esTimeout = axErr.code === 'ECONNABORTED' || /timeout/i.test(axErr.message || '');
      const { logger } = require('../../../shared/utils/logger');
      if (status === 429 && intento < MAX_INTENTOS_429) {
        const dataMsg   = String(axErr.response?.data?.Data ?? '');
        const match     = /retry after:\s*([\d.]+)/i.exec(dataMsg);
        const esperaSeg = Math.min((match ? Number(match[1]) : 10) + 1, 60);
        logger.warn(`[ErpSync] 429 en ${logLabel}, reintentando en ${esperaSeg.toFixed(1)}s (intento ${intento}/${MAX_INTENTOS_429})`);
        await new Promise(r => setTimeout(r, esperaSeg * 1000));
        continue;
      }
      if (esTimeout && intento < MAX_INTENTOS_429) {
        const esperaSeg = 3 * intento;
        logger.warn(`[ErpSync] timeout en ${logLabel}, reintentando en ${esperaSeg}s (intento ${intento}/${MAX_INTENTOS_429})`);
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
// Caché de las variantes "por centro + rango de fechas" (ver
// obtenerDesglosesCobroAlmacenPorCentro/obtenerSaldosFavorPorCentro) — clave
// distinta a la de series/folios porque el criterio de búsqueda es otro.
const _cacheAlmacenPorCentro     = new Map();
const _cacheSaldosFavorPorCentro = new Map();

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

// Consulta, por CENTRO (clave de serie de facturación, ej. "A0"/"B0"/"E0") y
// rango de fechas (ISO, ej. "2024-01-01T00:00:00Z"), todos los cobros de
// almacén hechos FÍSICAMENTE en ese centro — a diferencia de
// `obtenerDesglosesCobroAlmacen` (que requiere conocer de antemano series y
// folios de los documentos relacionados), esta variante no depende de que la
// sucursal VENDEDORA ya se haya generado: permite a la sucursal COBRADORA
// descubrir directamente lo que cobró de otras sucursales sin pasar por la
// cola `CobroSucursalPendiente` (ver cobros-sucursal-puente.service.js).
// Ya liberado en producción (confirmado con el usuario 2026-08-14) —
// ERP_CAJA_BASE_URL debe apuntar a https://app.cajas.tubosyconexiones.mx
// igual que el resto de los endpoints de este archivo.
async function obtenerDesglosesCobroAlmacenPorCentro({ rfc, centro, fechaDesde, fechaHasta }) {
  if (!ERP_CAJA_BASE_URL) {
    throw new Error('ERP no configurado (ERP_CAJA_BASE_URL ausente)');
  }
  if (!rfc) throw new Error('obtenerDesglosesCobroAlmacenPorCentro: rfc requerido (aísla la caché por empresa)');
  if (!centro || !fechaDesde || !fechaHasta) return [];

  const clave = `${rfc}::${centro}::${fechaDesde}::${fechaHasta}`;
  const cacheado = _leerCache(_cacheAlmacenPorCentro, clave);
  if (cacheado !== undefined) return cacheado;

  let response;
  try {
    response = await _getConReintento(`${ERP_CAJA_BASE_URL}/desgloses-cobro/almacen`, {
      centro, fechaDesde, fechaHasta,
    }, '/desgloses-cobro/almacen (por centro)');
  } catch (axErr) {
    const status = axErr.response?.status;
    const body   = JSON.stringify(axErr.response?.data ?? {});
    const { logger } = require('../../../shared/utils/logger');
    logger.error(`[ErpSync] ERP /desgloses-cobro/almacen (por centro) ${status}: ${body} | centro=${centro} fechaDesde=${fechaDesde} fechaHasta=${fechaHasta}`);
    throw axErr;
  }

  const cuentas = response.data?.Data?.cuentas || [];
  _cacheAlmacenPorCentro.set(clave, { data: cuentas, ts: Date.now() });
  return cuentas;
}

// Misma idea que `obtenerDesglosesCobroAlmacenPorCentro`, para saldos a
// favor generados/usados en un centro y rango de fechas — ver notas ahí.
async function obtenerSaldosFavorPorCentro({ rfc, centro, fechaDesde, fechaHasta }) {
  if (!ERP_CAJA_BASE_URL) {
    throw new Error('ERP no configurado (ERP_CAJA_BASE_URL ausente)');
  }
  if (!rfc) throw new Error('obtenerSaldosFavorPorCentro: rfc requerido (aísla la caché por empresa)');
  if (!centro || !fechaDesde || !fechaHasta) return [];

  const clave = `${rfc}::${centro}::${fechaDesde}::${fechaHasta}`;
  const cacheado = _leerCache(_cacheSaldosFavorPorCentro, clave);
  if (cacheado !== undefined) return cacheado;

  let response;
  try {
    response = await _getConReintento(`${ERP_CAJA_BASE_URL}/desgloses-cobro/saldos-favor`, {
      centro, fechaDesde, fechaHasta,
    }, '/desgloses-cobro/saldos-favor (por centro)');
  } catch (axErr) {
    const status = axErr.response?.status;
    const body   = JSON.stringify(axErr.response?.data ?? {});
    const { logger } = require('../../../shared/utils/logger');
    logger.error(`[ErpSync] ERP /desgloses-cobro/saldos-favor (por centro) ${status}: ${body} | centro=${centro} fechaDesde=${fechaDesde} fechaHasta=${fechaHasta}`);
    throw axErr;
  }

  const cuentas = response.data?.Data?.cuentas || [];
  _cacheSaldosFavorPorCentro.set(clave, { data: cuentas, ts: Date.now() });
  return cuentas;
}

module.exports = {
  sincronizarCuentasPendientes, obtenerDesglosesCobroAlmacen, obtenerSaldosFavor,
  obtenerDesglosesCobroAlmacenPorCentro, obtenerSaldosFavorPorCentro,
};
