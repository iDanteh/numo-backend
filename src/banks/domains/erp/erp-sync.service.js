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

module.exports = { sincronizarCuentasPendientes };
