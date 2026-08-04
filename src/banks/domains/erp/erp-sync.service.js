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

// Consulta, en lote, el desglose de cobros de almacén hechos en OTRAS
// sucursales para una lista de documentos (serie+folio del "documento
// relacionado" del CFDI, no el serie/folio propio de la factura). Usado por
// cobros-sucursal-puente.service.js para armar las líneas de Caja/Bancos
// por identificar en la póliza de Ingreso.
async function obtenerDesglosesCobroAlmacen({ series, folios }) {
  if (!ERP_CAJA_BASE_URL) {
    throw new Error('ERP no configurado (ERP_CAJA_BASE_URL ausente)');
  }
  if (!series?.length || !folios?.length) return [];

  let response;
  try {
    response = await axios.get(`${ERP_CAJA_BASE_URL}/desgloses-cobro/almacen`, {
      params: {
        series: series.join(','),
        folios: folios.join(','),
      },
      headers: { Authorization: `Bearer ${ERP_TOKEN}` },
      timeout: 15000,
    });
  } catch (axErr) {
    const status = axErr.response?.status;
    const body   = JSON.stringify(axErr.response?.data ?? {});
    const { logger } = require('../../../shared/utils/logger');
    logger.error(`[ErpSync] ERP /desgloses-cobro/almacen ${status}: ${body} | series=${series.join(',')} folios=${folios.join(',')}`);
    throw axErr;
  }

  return response.data?.Data?.cuentas || [];
}

// Consulta, en lote, saldos a favor GENERADOS (por una Devolución) y USADOS
// (aplicados a otra venta) para una lista de ventas (serie+folio interno,
// mismo `serieVenta`/`folioVenta` que `obtenerDesglosesCobroAlmacen`). Usado
// por cfdi-poliza-generator.service.js para registrar el pasivo de saldo a
// favor cuando una Devolución lo genera (confirmado con el usuario
// 2026-08-04) — antes solo se inferían de forma heurística desde el texto de
// `formasPago` en /desgloses-cobro/almacen, sin saber a qué Devolución/venta
// remontaban.
async function obtenerSaldosFavor({ series, folios }) {
  if (!ERP_CAJA_BASE_URL) {
    throw new Error('ERP no configurado (ERP_CAJA_BASE_URL ausente)');
  }
  if (!series?.length || !folios?.length) return [];

  let response;
  try {
    response = await axios.get(`${ERP_CAJA_BASE_URL}/desgloses-cobro/saldos-favor`, {
      params: {
        series: series.join(','),
        folios: folios.join(','),
      },
      headers: { Authorization: `Bearer ${ERP_TOKEN}` },
      timeout: 15000,
    });
  } catch (axErr) {
    const status = axErr.response?.status;
    const body   = JSON.stringify(axErr.response?.data ?? {});
    const { logger } = require('../../../shared/utils/logger');
    logger.error(`[ErpSync] ERP /desgloses-cobro/saldos-favor ${status}: ${body} | series=${series.join(',')} folios=${folios.join(',')}`);
    throw axErr;
  }

  return response.data?.Data?.cuentas || [];
}

module.exports = { sincronizarCuentasPendientes, obtenerDesglosesCobroAlmacen, obtenerSaldosFavor };
