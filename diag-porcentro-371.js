'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'C0';
const FOLIO_BUSCADO = process.env.DIAG_FOLIO || '260802371';

async function main() {
  await connectMongo();

  const fechaDesdeIso = new Date('2026-08-12T00:00:00-06:00').toISOString();
  const fechaHastaIso = new Date('2026-08-14T23:59:59.999-06:00').toISOString();
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({
    rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso,
  });
  console.log('Total cuentas devueltas (por centro):', resultado.length);

  const cuenta = resultado.find(c => String(c.folioVenta) === FOLIO_BUSCADO);
  if (!cuenta) {
    console.log(`NO se encontro el ticket ${FOLIO_BUSCADO} en el camino "por centro".`);
  } else {
    console.log('Encontrado:', JSON.stringify(cuenta, null, 2));
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
