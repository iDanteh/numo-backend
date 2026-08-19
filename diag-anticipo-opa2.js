'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const { obtenerDesglosesCobroAlmacen, obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'C0';
const FOLIOS = (process.env.DIAG_FOLIOS || '260800418,260800419,260701665').split(',');

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  console.log(`\n=== 1. obtenerDesglosesCobroAlmacen para ${CENTRO}: ${FOLIOS.join(', ')} ===\n`);
  const almacen = await obtenerDesglosesCobroAlmacen({ rfc: RFC, series: FOLIOS.map(() => CENTRO), folios: FOLIOS });
  console.log(JSON.stringify(almacen, null, 2));

  console.log(`\n=== 2. Busqueda amplia "por centro" TODO AGOSTO, filtrando cobros con serieOrigen=OPA ===\n`);
  const fechaDesdeIso = new Date('2026-08-01T00:00:00-06:00').toISOString();
  const fechaHastaIso = new Date('2026-08-19T23:59:59.999-06:00').toISOString();
  const porCentro = await obtenerDesglosesCobroAlmacenPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso });
  console.log('Total cuentas devueltas:', porCentro.length);
  let encontrados = 0;
  for (const cuenta of porCentro) {
    for (const cobro of (cuenta.cobros ?? [])) {
      if ((cobro.serieOrigen ?? '').toUpperCase() === 'OPA') {
        encontrados++;
        console.log('\n--- Cobro OPA encontrado ---');
        console.log(JSON.stringify({ cuenta: { serie: cuenta.serie, folio: cuenta.folio, serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta, serieFactura: cuenta.serieFactura, folioFactura: cuenta.folioFactura }, cobro }, null, 2));
      }
    }
  }
  console.log(`\nTotal cobros OPA encontrados en agosto: ${encontrados}`);

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
