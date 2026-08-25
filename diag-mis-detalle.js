'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'B0';
// Ventana amplia (varios dias) para juntar mas muestras de "MIS" y ver el
// patron completo -- solo 2 ocurrencias en un solo dia no bastan.
const DESDE = process.env.DIAG_DESDE || '2026-08-01';
const HASTA = process.env.DIAG_HASTA || '2026-08-17';

async function main() {
  await connectMongo();

  const fechaDesdeIso = new Date(`${DESDE}T00:00:00-06:00`).toISOString();
  const fechaHastaIso = new Date(`${HASTA}T23:59:59.999-06:00`).toISOString();
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({
    rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso,
  });

  // Cuentas que tienen AL MENOS un cobro con serieOrigen MIS -- imprimir la
  // cuenta COMPLETA (todos sus cobros, no solo el MIS) para ver si hay un
  // cobro companero en la misma cuenta que ya cubra ese monto.
  const cuentasConMis = resultado.filter(c => (c.cobros ?? []).some(co => (co.serieOrigen ?? '').toUpperCase() === 'MIS'));
  console.log(`Cuentas con al menos un cobro MIS (${CENTRO}, ${DESDE}..${HASTA}):`, cuentasConMis.length);

  for (const cuenta of cuentasConMis) {
    console.log('\n=== cuenta ===');
    console.log(JSON.stringify({
      serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta,
      serieFactura: cuenta.serieFactura, folioFactura: cuenta.folioFactura,
      almacen: cuenta.almacen, fechaCreacion: cuenta.fechaCreacion,
      cobros: cuenta.cobros,
    }, null, 2));
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
