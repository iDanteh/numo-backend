'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const { obtenerSaldosFavorPorCentro } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'C0';
const FECHA = process.env.DIAG_FECHA || '2026-08-03';
const FOLIO_BUSCADO = process.env.DIAG_FOLIO || '260800090';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const fechaDesdeIso = new Date(`${FECHA}T00:00:00-06:00`).toISOString();
  const fechaHastaIso = new Date(`${FECHA}T23:59:59.999-06:00`).toISOString();

  const resultado = await obtenerSaldosFavorPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso });
  console.log('Total cuentas devueltas:', resultado.length);

  console.log(`\n=== Cuentas donde ${FOLIO_BUSCADO} aparece como USO (uso.folioVenta) ===\n`);
  for (const cuenta of resultado) {
    for (const gen of (cuenta.saldosFavorGenerados ?? [])) {
      for (const uso of (gen.usos ?? [])) {
        if (String(uso.folioVenta) === FOLIO_BUSCADO) {
          console.log(JSON.stringify({
            generadoPor: { serieOrigen: gen.serieOrigen, folioOrigen: gen.folioOrigen, fecha: gen.fecha },
            uso: { serieVenta: uso.serieVenta, folioVenta: uso.folioVenta, montoUsado: uso.montoUsado, fecha: uso.fecha, montoSobrante: uso.montoSobrante },
            cuentaGeneradora: { serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta },
          }, null, 2));
        }
      }
    }
    for (const uso of (cuenta.saldosFavorUsados ?? [])) {
      if (String(uso.serieVenta ?? cuenta.serieVenta) === 'C0' && String(cuenta.folioVenta) === FOLIO_BUSCADO) {
        console.log('saldosFavorUsados directo:', JSON.stringify({ cuenta: { serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta }, uso }, null, 2));
      }
    }
  }

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
