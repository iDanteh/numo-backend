'use strict';

const axios = require('axios');
const globalConfigService = require('../../../shared/services/global-config.service');

// Configuraciones Globales, sección `bancos` (runtime/DB, ver
// shared/services/global-config.service.js — reemplaza lo que antes eran
// ERP_CAJA_BASE_URL/ERP_CAJA_BASE_TEST_URL en el .env) — EXCLUSIVA de Bancos /
// Solicitudes de Cobro / Reversiones (decisión explícita del usuario 2026-08-25:
// una sección de Configuraciones Globales no debe mezclar uso con otros módulos).
// Consolidada 2026-08-25 en una sola sección `bancos` junto con kore-formaspago
// y erp-fact — antes eran 3 secciones separadas, ahora es una sola con más claves.
//   - CUENTAS_PENDIENTES_URL: base para /cuentas-pendientes (sincronizarCuentasPendientes,
//     usado por solicitudes de cobro/reversiones/bank-sync). Cada ambiente (staging/prod)
//     tiene su propia fila en su propia Postgres con el valor correcto — ya no hace falta
//     branching por DEPLOY_ENV en el código, cada Postgres ya sabe cuál le corresponde.
//   - TOKEN: token del ERP para autenticar sincronizarCuentasPendientes — reemplaza
//     process.env.ERP_TOKEN SOLO para esta función; visor/services/erp.service.js sigue
//     leyendo el .env directo (mismo valor físico, dos consumidores independientes
//     durante la migración gradual).
async function _cuentasPendientesUrl() {
  const valor = await globalConfigService.getValue('bancos', 'CUENTAS_PENDIENTES_URL');
  return valor.replace(/\/$/, '');
}

async function _token() {
  return globalConfigService.getValue('bancos', 'TOKEN');
}

// obtenerDesglosesCobroAlmacen/obtenerSaldosFavor/*PorCentro (más abajo) son EXCLUSIVAS
// de Pólizas (cobros-sucursal-puente.service.js / cfdi-poliza-generator.service.js,
// dominio cfdi-mapping/polizas). Migradas 2026-08-28 a Configuraciones Globales, sección
// propia `polizas` (antes leían ERP_CAJA_BASE_URL/ERP_TOKEN del .env directo por decisión
// explícita del usuario de dejarlas fuera — esa decisión ya no aplica: "no funciona nada
// si no está ahí"). Nunca comparten sección con `bancos` (esa es solo Bancos/Cobro/Reversiones).
async function _cajaBaseUrlPolizas() {
  const valor = await globalConfigService.getValue('polizas', 'CAJA_BASE_URL');
  return valor.replace(/\/$/, '');
}

async function _tokenPolizas() {
  return globalConfigService.getValue('polizas', 'TOKEN');
}

// BUG CORREGIDO 2026-09-04 (caso real HORIZONTE HOTELERO B0-260900010/anticipo
// B0-260900009): esta era la ÚNICA función de este archivo que llamaba al ERP
// sin el reintento con backoff que ya usan `obtenerDesglosesCobroAlmacen`/
// `obtenerSaldosFavor` (ver `_getConReintento` arriba) — y con un timeout más
// corto (15s vs 30s). `/cuentas-pendientes` es además la consulta MÁS PESADA
// de todas (12,833 cuentas en una prueba real con solo ±5 días de rango) —
// bajo la carga real de una regeneración completa de póliza, un timeout aquí
// se traga en silencio (el caller de `_prefetchCuentasPendientesAnticipo`
// solo loguea y sigue con los mecanismos de respaldo, MENOS confiables), y
// `anticipoFolioRefProp` termina usando el folio crudo del CFDI del anticipo
// ("OPA-260900009") en vez del folio real de Kore ("OPA-00834") — se
// confirmó con datos reales que el dato SÍ existe en el ERP y el único
// factor no explicado era la fiabilidad de esta llamada. No se reutiliza
// `_getConReintento` tal cual porque esa usa `_tokenPolizas()` (sección
// `polizas`) — este endpoint es de la sección `bancos` (`_token()`), su
// propio token/URL no se tocan, solo el reintento+timeout.
async function sincronizarCuentasPendientes(params = {}) {
  const cuentasPendientesUrl = await _cuentasPendientesUrl();

  const queryParams = {};
  if (params.fechaDesde)    queryParams.fechaDesde    = params.fechaDesde;
  if (params.fechaHasta)    queryParams.fechaHasta    = params.fechaHasta;
  if (params.estadoCobro)   queryParams.estadoCobro   = params.estadoCobro;
  if (params.serieExterna)  queryParams.serieExterna  = String(params.serieExterna).trim();
  if (params.folioExterno)  queryParams.folioExterno  = String(params.folioExterno).trim();
  if (params.nombrePersona) queryParams.nombrePersona = String(params.nombrePersona).trim();
  if (params.origen)        queryParams.origen        = String(params.origen).trim();

  const token = await _token();
  let response;
  const MAX_INTENTOS = 3;
  for (let intento = 1; intento <= MAX_INTENTOS; intento++) {
    try {
      response = await axios.get(`${cuentasPendientesUrl}/cuentas-pendientes`, {
        params:  queryParams,
        headers: { Authorization: `Bearer ${token}` },
        timeout: 30000,
      });
      // BUG CORREGIDO 2026-09-04 (caso real HORIZONTE HOTELERO B0-260900010/
      // OPA-260900009): el ERP puede responder 200 con una lista PARCIAL bajo
      // la carga real de una regeneración completa de póliza (docenas de
      // llamadas concurrentes a distintos endpoints) — sin error, sin 429, sin
      // timeout, solo menos registros de los que realmente hay. Confirmado
      // con datos reales: la misma consulta (mismo rango de fechas) trajo 221
      // cuentas bajo carga real vs 12,833 en una consulta aislada — el
      // registro que necesitábamos (B0|260900009) faltaba en la respuesta
      // parcial. El ERP SÍ manda `Data.totalCount` (el total real, sin
      // truncar) junto con `Data.cuentas` — si no coinciden, la respuesta es
      // incompleta y se reintenta igual que un timeout/429, en vez de
      // confiar ciegamente en lo que haya llegado.
      {
        const totalCount = response.data?.Data?.totalCount;
        const cuentasLen = (response.data?.Data?.cuentas ?? []).length;
        if (process.env.DEBUG_OPA_UUID) {
          console.warn(`[DEBUG_CUENTAS_PENDIENTES] intento=${intento} totalCount=${JSON.stringify(totalCount)} cuentasLen=${cuentasLen} Data.keys=${JSON.stringify(Object.keys(response.data?.Data ?? {}))}`);
        }
        if (Number.isFinite(totalCount) && cuentasLen < totalCount && intento < MAX_INTENTOS) {
          const { logger } = require('../../../shared/utils/logger');
          const esperaSeg = 3 * intento;
          logger.warn(`[ErpSync] /cuentas-pendientes respuesta incompleta (${cuentasLen}/${totalCount}), reintentando en ${esperaSeg}s (intento ${intento}/${MAX_INTENTOS})`);
          await new Promise(r => setTimeout(r, esperaSeg * 1000));
          continue;
        }
      }
      break;
    } catch (axErr) {
      const status    = axErr.response?.status;
      const esTimeout = axErr.code === 'ECONNABORTED' || /timeout/i.test(axErr.message || '');
      const { logger } = require('../../../shared/utils/logger');
      if (status === 429 && intento < MAX_INTENTOS) {
        const dataMsg   = String(axErr.response?.data?.Data ?? '');
        const match     = /retry after:\s*([\d.]+)/i.exec(dataMsg);
        const esperaSeg = Math.min((match ? Number(match[1]) : 10) + 1, 60);
        logger.warn(`[ErpSync] 429 en /cuentas-pendientes, reintentando en ${esperaSeg.toFixed(1)}s (intento ${intento}/${MAX_INTENTOS})`);
        await new Promise(r => setTimeout(r, esperaSeg * 1000));
        continue;
      }
      if (esTimeout && intento < MAX_INTENTOS) {
        const esperaSeg = 3 * intento;
        logger.warn(`[ErpSync] timeout en /cuentas-pendientes, reintentando en ${esperaSeg}s (intento ${intento}/${MAX_INTENTOS})`);
        await new Promise(r => setTimeout(r, esperaSeg * 1000));
        continue;
      }
      const body = JSON.stringify(axErr.response?.data ?? {});
      logger.error(`[ErpSync] ERP /cuentas-pendientes ${status}: ${body} | params=${JSON.stringify(queryParams)}`);
      throw axErr;
    }
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
//
// 2026-08-24: 30s tampoco alcanzaba contra el ERP REAL de producción —
// medido con curl: /desgloses-cobro/saldos-favor (por centro) tardó 35.75s
// en responder (caso real Viguera/Hidalgo, ~24 días de rango). Con 30s, las
// 3 reintentos hacían timeout igual y el caller caía al camino "por
// serie/folio" (incompleto), dejando cobros reales sin encontrar y
// fragmentando la venta en líneas "Venta Sin Cobro" (caso real B0-260803791,
// $24,981.27 cobrados en Efectivo, solo $1,462.89 se reconciliaban).
const MAX_INTENTOS_429 = 3;
async function _getConReintento(url, params, logLabel) {
  const token = await _tokenPolizas();
  for (let intento = 1; intento <= MAX_INTENTOS_429; intento++) {
    try {
      return await axios.get(url, {
        params,
        headers: { Authorization: `Bearer ${token}` },
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

// BUG CORREGIDO 2026-09-04 (caso real Reforma/Hidalgo, cobro cruzado
// B0-260900438 nunca capturado): confirmado con datos reales que las
// consultas "por centro" (que traen TODAS las cuentas de un rango de fechas,
// no una lista acotada de series/folios conocidos) pueden responder 200 OK
// con una lista PARCIAL bajo la carga real de una regeneración completa de
// póliza — sin 429 ni timeout que `_getConReintento` pueda detectar. Mismo
// patrón ya confirmado y corregido en `sincronizarCuentasPendientes`
// (221 cuentas bajo carga real vs 12,833 en consulta aislada). El ERP manda
// `Data.totalCount` junto con `Data.cuentas` en AMBOS endpoints "por centro"
// (confirmado con una consulta real a /desgloses-cobro/almacen) — si no
// coinciden, se reintenta la llamada completa (con su propio backoff interno)
// en vez de confiar en la lista incompleta.
const MAX_INTENTOS_COMPLETO = 3;
async function _getConReintentoCompleto(url, params, logLabel) {
  let response;
  for (let intento = 1; intento <= MAX_INTENTOS_COMPLETO; intento++) {
    response = await _getConReintento(url, params, logLabel);
    const totalCount = response.data?.Data?.totalCount;
    const cuentasLen = (response.data?.Data?.cuentas ?? []).length;
    if (!Number.isFinite(totalCount) || cuentasLen >= totalCount || intento >= MAX_INTENTOS_COMPLETO) {
      return response;
    }
    const { logger } = require('../../../shared/utils/logger');
    const esperaSeg = 3 * intento;
    logger.warn(`[ErpSync] ${logLabel} respuesta incompleta (${cuentasLen}/${totalCount}), reintentando en ${esperaSeg}s (intento ${intento}/${MAX_INTENTOS_COMPLETO})`);
    await new Promise(r => setTimeout(r, esperaSeg * 1000));
  }
  return response;
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

// El ERP rechaza con HTTP 400 "rango de fechas mayor a 31 días sin criterio
// de factura" cualquier consulta "por centro" más amplia — y la generación
// de pólizas amplía el período ±1 día de tolerancia
// (TOLERANCIA_DIAS_FACTURACION_DIFERIDA en cfdi-poliza-generator.service.js),
// así que CUALQUIER mes de 30 o 31 días ya excede el límite (confirmado
// 2026-08-24, caso real Viguera/Hidalgo agosto: rango ampliado 31-jul a
// 1-sep = 33 días → HTTP 400 → el catch de `_prefetchAjustesFacturaPropia`
// lo trataba como falla genérica y caía en silencio al camino "por
// serie/folio", incompleto — de ahí las líneas "Venta Sin Cobro" recurrentes
// cada mes, no solo en agosto). Se trocea el rango en bloques ≤30 días y se
// combinan los resultados — transparente para el caller.
const MS_UN_DIA = 24 * 60 * 60 * 1000;
const MAX_DIAS_RANGO_ERP = 30;
function _trocearRango(fechaDesdeIso, fechaHastaIso) {
  const desde = new Date(fechaDesdeIso);
  const hasta = new Date(fechaHastaIso);
  const bloques = [];
  let cursor = desde;
  while (cursor < hasta) {
    const finBloque = new Date(Math.min(cursor.getTime() + MAX_DIAS_RANGO_ERP * MS_UN_DIA, hasta.getTime()));
    bloques.push({ fechaDesde: cursor.toISOString(), fechaHasta: finBloque.toISOString() });
    cursor = new Date(finBloque.getTime() + 1);
  }
  return bloques;
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
  if (!rfc) throw new Error('obtenerDesglosesCobroAlmacen: rfc requerido (aísla la caché por empresa)');
  if (!series?.length || !folios?.length) return [];

  const clave = _claveLote(rfc, series, folios);
  const cacheado = _leerCache(_cacheAlmacen, clave);
  if (cacheado !== undefined) return cacheado;

  const baseUrl = await _cajaBaseUrlPolizas();
  let response;
  try {
    response = await _getConReintento(`${baseUrl}/desgloses-cobro/almacen`, {
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
  if (!rfc) throw new Error('obtenerSaldosFavor: rfc requerido (aísla la caché por empresa)');
  if (!series?.length || !folios?.length) return [];

  const clave = _claveLote(rfc, series, folios);
  const cacheado = _leerCache(_cacheSaldosFavor, clave);
  if (cacheado !== undefined) return cacheado;

  const baseUrl = await _cajaBaseUrlPolizas();
  let response;
  try {
    response = await _getConReintento(`${baseUrl}/desgloses-cobro/saldos-favor`, {
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
// polizas.CAJA_BASE_URL debe apuntar a https://app.cajas.tubosyconexiones.mx
// igual que el resto de los endpoints de este archivo.
async function obtenerDesglosesCobroAlmacenPorCentro({ rfc, centro, fechaDesde, fechaHasta }) {
  if (!rfc) throw new Error('obtenerDesglosesCobroAlmacenPorCentro: rfc requerido (aísla la caché por empresa)');
  if (!centro || !fechaDesde || !fechaHasta) return [];

  const clave = `${rfc}::${centro}::${fechaDesde}::${fechaHasta}`;
  const cacheado = _leerCache(_cacheAlmacenPorCentro, clave);
  if (cacheado !== undefined) return cacheado;

  const baseUrl = await _cajaBaseUrlPolizas();
  let response;
  try {
    response = await _getConReintentoCompleto(`${baseUrl}/desgloses-cobro/almacen`, {
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
  if (!rfc) throw new Error('obtenerSaldosFavorPorCentro: rfc requerido (aísla la caché por empresa)');
  if (!centro || !fechaDesde || !fechaHasta) return [];

  const clave = `${rfc}::${centro}::${fechaDesde}::${fechaHasta}`;
  const cacheado = _leerCache(_cacheSaldosFavorPorCentro, clave);
  if (cacheado !== undefined) return cacheado;

  const baseUrl = await _cajaBaseUrlPolizas();
  let response;
  try {
    response = await _getConReintentoCompleto(`${baseUrl}/desgloses-cobro/saldos-favor`, {
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
