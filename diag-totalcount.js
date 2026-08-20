'use strict';
require('dotenv').config();
const axios = require('axios');

const ERP_CAJA_BASE_URL = (process.env.ERP_CAJA_BASE_URL || '').replace(/\/$/, '');
const ERP_TOKEN = process.env.ERP_TOKEN || '';
const CENTRO = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';

async function main() {
  const fechaDesde = new Date(`${FECHA}T00:00:00-06:00`).toISOString();
  const fechaHasta = new Date(`${FECHA}T23:59:59.999-06:00`).toISOString();

  const response = await axios.get(`${ERP_CAJA_BASE_URL}/desgloses-cobro/almacen`, {
    params: { centro: CENTRO, fechaDesde, fechaHasta },
    headers: { Authorization: `Bearer ${ERP_TOKEN}` },
    timeout: 30000,
  });

  const data = response.data?.Data ?? {};
  const cuentas = data.cuentas ?? [];
  console.log('Claves de Data:', Object.keys(data));
  console.log('Data.totalCount (si existe):', data.totalCount);
  console.log('cuentas.length real recibido:', cuentas.length);
  if (data.totalCount != null && data.totalCount !== cuentas.length) {
    console.log('*** DISCREPANCIA: el endpoint reporta mas registros de los que regresa (posible paginacion no manejada) ***');
  } else {
    console.log('(coinciden -- no hay evidencia de truncado por paginacion en esta consulta)');
  }
}

main().catch(e => { console.error('ERROR:', e.response?.status, JSON.stringify(e.response?.data ?? e.message)); process.exit(1); });
