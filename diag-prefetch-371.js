'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { _prefetchAjustesFacturaPropia } = require('./src/banks/domains/cfdi-mapping/cfdi-poliza-generator.service.js');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'C0';
const FOLIO = process.env.DIAG_FOLIO || '260800657';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const cfdi = await CFDI.findOne({ serie: SERIE, folio: FOLIO, source: 'ERP' }).lean();
  if (!cfdi) { console.log('CFDI no encontrado'); process.exit(1); }
  console.log('CFDI encontrado:', cfdi.uuid, cfdi.fecha);

  const cfdiConRegla = [{ cfdi, rule: { cuentaCargo: '1101010003' } }];

  const fechaDesde = new Date('2026-08-13T00:00:00-06:00');
  const fechaHasta = new Date('2026-08-13T23:59:59.999-06:00');

  const { desglosePagoReal, usoCaminoPorCentro } = await _prefetchAjustesFacturaPropia(cfdiConRegla, RFC, {
    centroPropioClave: SERIE, fechaDesde, fechaHasta,
  });
  console.log('usoCaminoPorCentro:', usoCaminoPorCentro);

  const key = `${cfdi.serie}|${cfdi.folio}`;
  const fps = desglosePagoReal.get(key) ?? [];
  console.log(`Total formasPago para ${key}:`, fps.length);

  const suma = fps.reduce((s, fp) => s + (Number(fp.monto) || 0), 0);
  console.log('Suma total:', suma.toFixed(2));

  const del371 = fps.filter(fp => fp.folioVentaTicket === '260802371');
  console.log(`\nEntradas con folioVentaTicket=260802371: ${del371.length}`);
  console.log(JSON.stringify(del371, null, 2));

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
