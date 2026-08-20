'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerSaldosFavorPorCentro } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';

// Verificar la hipotesis del usuario: la Devolucion DEV-056363 (ligada a la
// factura B0-260802634, $132.59) genero un saldo a favor, y ese saldo se
// retiro EN EFECTIVO via un 'ABO' dentro de sus `usos` -- el mecanismo
// `ajustesEfectivoRetiroSF` ya existente en cfdi-poliza-generator.service.js
// deberia restar ese retiro del consolidado de Efectivo.
async function main() {
  await connectMongo();

  const fechaDesdeIso = new Date(`${FECHA}T00:00:00-06:00`).toISOString();
  const fechaHastaIso = new Date(`${FECHA}T23:59:59.999-06:00`).toISOString();
  const resultado = await obtenerSaldosFavorPorCentro({
    rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso,
  });

  console.log('Total cuentas con saldos a favor:', resultado.length);

  const encontrados = resultado.filter(c => c.folioVenta === '260802634' || c.serieVenta === 'B0' && c.folioVenta === '260802634');
  console.log('\nCoincidencias por folioVenta=260802634:');
  console.log(JSON.stringify(encontrados, null, 2));

  // Tambien buscar por el marcador DEV-056363 dentro de saldosFavorGenerados,
  // por si la venta que lo genero es otra.
  const porMarcador = [];
  for (const c of resultado) {
    for (const gen of (c.saldosFavorGenerados ?? [])) {
      if ((gen.folioOrigen ?? '') === '056363' || (gen.serieOrigen ?? '').toUpperCase() === 'DEV' && (gen.folioOrigen ?? '') === '056363') {
        porMarcador.push({ cuenta: { serieVenta: c.serieVenta, folioVenta: c.folioVenta }, gen });
      }
    }
  }
  console.log('\nCoincidencias por marcador DEV-056363:');
  console.log(JSON.stringify(porMarcador, null, 2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
