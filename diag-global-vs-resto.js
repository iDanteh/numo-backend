'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';
const FOLIO_GLOBAL = process.env.DIAG_FOLIO_GLOBAL || '260801256';

async function main() {
  await connectMongo();

  const fechaDesdeIso = new Date(`${FECHA}T00:00:00-06:00`).toISOString();
  const fechaHastaIso = new Date(`${FECHA}T23:59:59.999-06:00`).toISOString();
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({
    rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso,
  });

  const porClaveGlobal = {};
  const porClaveResto = {};
  const vistos = new Set();

  for (const cuenta of resultado) {
    const esGlobal = cuenta.serieFactura === CENTRO && String(cuenta.folioFactura) === FOLIO_GLOBAL;
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro !== CENTRO) continue;
      const fechaCobroMx = new Date(cobro.fecha);
      fechaCobroMx.setHours(fechaCobroMx.getHours() - 6);
      const diaCobro = fechaCobroMx.toISOString().slice(0, 10);
      if (diaCobro !== FECHA) continue;

      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      if (origen !== 'CBT' && origen !== 'APS' && origen !== 'MIS' && !SERIES_CON_AUTH.includes(origen)) continue;

      const dedupeKey = `${cobro.serieOrigen}|${cobro.folioOrigen}|${cuenta.serieVenta}|${cuenta.folioVenta}`;
      if (vistos.has(dedupeKey)) continue;
      vistos.add(dedupeKey);

      const destino = esGlobal ? porClaveGlobal : porClaveResto;
      for (const fp of (cobro.formasPago ?? [])) {
        if (/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        const monto = (cobro.formasPago.length === 1 && cobro.monto != null)
          ? Math.abs(Number(cobro.monto) || 0)
          : (Number(fp.monto) || 0);
        const clave = fp.claveSat ?? '??';
        destino[clave] = (destino[clave] || 0) + monto;
      }
    }
  }

  console.log(`Suma por claveSat -- SOLO Global (${CENTRO}|${FOLIO_GLOBAL}):`, JSON.stringify(porClaveGlobal, null, 2));
  console.log(`\nSuma por claveSat -- TODO LO DEMAS (${CENTRO}, ${FECHA}):`, JSON.stringify(porClaveResto, null, 2));

  const totalGlobal01 = porClaveGlobal['01'] || 0;
  const totalResto01 = porClaveResto['01'] || 0;
  console.log(`\nEfectivo Global: ${totalGlobal01.toFixed(2)}`);
  console.log(`Efectivo Resto: ${totalResto01.toFixed(2)}`);
  console.log(`Efectivo TOTAL: ${(totalGlobal01 + totalResto01).toFixed(2)}`);

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
