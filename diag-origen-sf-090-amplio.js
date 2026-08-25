'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const { obtenerSaldosFavorPorCentro, obtenerSaldosFavor } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'C0';
const FOLIO_BUSCADO = process.env.DIAG_FOLIO || '260800090';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  // 1. Consulta DIRECTA por serie/folio (sin depender de centro+fecha) — el
  // dato mas confiable, mismo endpoint que usa el camino "viejo".
  console.log(`\n=== 1. Consulta directa /saldos-favor para ${CENTRO}-${FOLIO_BUSCADO} ===\n`);
  const directo = await obtenerSaldosFavor({ rfc: RFC, series: [CENTRO], folios: [FOLIO_BUSCADO] });
  console.log(JSON.stringify(directo, null, 2));

  // 2. Consulta por centro+fecha, ventana AMPLIA (todo agosto) — replicar lo
  // que usa _prefetchAjustesFacturaPropia, buscando esta cuenta y cualquier
  // "uso" que apunte a este folioVenta, sin importar la fecha exacta.
  console.log(`\n=== 2. Consulta por centro+fecha, TODO AGOSTO, buscando ${FOLIO_BUSCADO} ===\n`);
  const fechaDesdeIso = new Date('2026-08-01T00:00:00-06:00').toISOString();
  const fechaHastaIso = new Date('2026-08-31T23:59:59.999-06:00').toISOString();
  const resultado = await obtenerSaldosFavorPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso });
  console.log('Total cuentas devueltas (todo agosto):', resultado.length);

  let encontrado = false;
  for (const cuenta of resultado) {
    if (String(cuenta.folioVenta) === FOLIO_BUSCADO) {
      encontrado = true;
      console.log('\n--- Cuenta propia (folioVenta coincide) ---');
      console.log(JSON.stringify(cuenta, null, 2));
    }
    for (const gen of (cuenta.saldosFavorGenerados ?? [])) {
      for (const uso of (gen.usos ?? [])) {
        if (String(uso.folioVenta) === FOLIO_BUSCADO) {
          encontrado = true;
          console.log('\n--- Encontrado como USO dentro de saldosFavorGenerados ---');
          console.log(JSON.stringify({ cuentaGeneradora: { serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta }, gen, uso }, null, 2));
        }
      }
    }
  }
  if (!encontrado) console.log('No se encontro ninguna coincidencia para', FOLIO_BUSCADO, 'en todo agosto.');

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
