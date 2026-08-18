'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'O0';
const FECHA = process.env.DIAG_FECHA || '2026-08-07';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const fechaDesdeIso = new Date(`${FECHA}T00:00:00-06:00`).toISOString();
  const fechaHastaIso = new Date(`${FECHA}T23:59:59.999-06:00`).toISOString();

  console.log(`\n=== Cuentas de ${CENTRO} el ${FECHA} con cobros TARJETA (claveSat 04/28) ===\n`);
  const cuentas = await obtenerDesglosesCobroAlmacenPorCentro({
    rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso,
  });
  console.log('Total cuentas devueltas:', cuentas.length);

  let ejemplosImpresos = 0;
  for (const cuenta of cuentas) {
    for (const cobro of (cuenta.cobros ?? [])) {
      const formasPagoTarjeta = (cobro.formasPago ?? []).filter(fp => ['04', '28'].includes((fp.claveSat ?? '').trim()));
      if (!formasPagoTarjeta.length) continue;
      console.log(JSON.stringify({
        serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta,
        serieFactura: cuenta.serieFactura, folioFactura: cuenta.folioFactura,
        cobroFecha: cobro.fecha, cobroMonto: cobro.monto, claveCentro: cobro.claveCentro,
        formasPago: cobro.formasPago,
      }, null, 2));
      ejemplosImpresos++;
      if (ejemplosImpresos >= 8) break;
    }
    if (ejemplosImpresos >= 8) break;
  }
  if (!ejemplosImpresos) console.log('No se encontraron cobros con formaPago Tarjeta (claveSat 04/28) en este rango.');

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
