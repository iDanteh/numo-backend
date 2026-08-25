'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const repo = require('./src/banks/domains/polizas/repositories/poliza.repository');
const { obtenerSaldosFavorPorCentro, obtenerSaldosFavor } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'C0';
const FOLIO_BUSCADO = process.env.DIAG_FOLIO || '260800403';
const POLIZA_ID = Number(process.env.DIAG_POLIZA_ID) || 421;

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  console.log(`\n=== 1. Consulta directa /saldos-favor para ${CENTRO}-${FOLIO_BUSCADO} ===\n`);
  const directo = await obtenerSaldosFavor({ rfc: RFC, series: [CENTRO], folios: [FOLIO_BUSCADO] });
  console.log(JSON.stringify(directo, null, 2));

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
    for (const uso of (cuenta.saldosFavorUsados ?? [])) {
      if (String(cuenta.folioVenta) === FOLIO_BUSCADO) {
        console.log('\n--- saldosFavorUsados de esta misma cuenta ---');
        console.log(JSON.stringify(uso, null, 2));
      }
    }
  }
  if (!encontrado) console.log('No se encontro ninguna coincidencia para', FOLIO_BUSCADO, 'en todo agosto.');

  console.log(`\n=== 3. Lineas en poliza ${POLIZA_ID} que mencionan ${FOLIO_BUSCADO} ===\n`);
  const poliza = await repo.findByIdLight(POLIZA_ID);
  const matching = poliza.movimientos.filter(m => (m.concepto || '').includes(FOLIO_BUSCADO) || (m.serie || '').includes(FOLIO_BUSCADO));
  console.log(`Total movimientos en poliza: ${poliza.movimientos.length}; que mencionan ${FOLIO_BUSCADO}: ${matching.length}\n`);
  for (const m of matching) {
    console.log({
      id: m.id, orden: m.orden, concepto: m.concepto, serie: m.serie, debe: m.debe, haber: m.haber,
      tipoOrigen: m.tipoOrigen, reglaNombre: m.reglaNombre, cuentaCodigo: m.cuenta?.codigo,
    });
  }

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
