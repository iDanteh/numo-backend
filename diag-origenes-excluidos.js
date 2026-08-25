'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';

async function main() {
  await connectMongo();

  const fechaDesdeIso = new Date(`${FECHA}T00:00:00-06:00`).toISOString();
  const fechaHastaIso = new Date(`${FECHA}T23:59:59.999-06:00`).toISOString();
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({
    rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso,
  });

  const reconocidos = new Set([...SERIES_CON_AUTH, 'CBT', 'APS']);
  const excluidos = [];
  const vistos = new Set();

  for (const cuenta of resultado) {
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro !== CENTRO) continue;
      const fechaCobroMx = new Date(cobro.fecha);
      fechaCobroMx.setHours(fechaCobroMx.getHours() - 6);
      const diaCobro = fechaCobroMx.toISOString().slice(0, 10);
      if (diaCobro !== FECHA) continue;

      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      if (reconocidos.has(origen)) continue;

      const dedupeKey = `${cobro.serieOrigen}|${cobro.folioOrigen}|${cuenta.serieVenta}|${cuenta.folioVenta}`;
      if (vistos.has(dedupeKey)) continue;
      vistos.add(dedupeKey);

      excluidos.push({
        origen,
        folioOrigen: cobro.folioOrigen,
        monto: cobro.monto,
        formasPago: cobro.formasPago,
        serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta,
        serieFactura: cuenta.serieFactura, folioFactura: cuenta.folioFactura,
        fecha: cobro.fecha,
      });
    }
  }

  console.log(`Total cobros excluidos (${CENTRO}, ${FECHA}):`, excluidos.length);
  const porOrigen = {};
  for (const e of excluidos) porOrigen[e.origen] = (porOrigen[e.origen] || 0) + Math.abs(Number(e.monto) || 0);
  console.log('Suma abs(monto) por origen excluido:', porOrigen);
  console.log('\nDetalle:');
  for (const e of excluidos) console.log(JSON.stringify(e));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
