'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { _prefetchAjustesFacturaPropia } = require('./src/banks/domains/cfdi-mapping/cfdi-poliza-generator.service.js');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';
const FOLIO_OBJETIVO = process.env.DIAG_FOLIO || '260801256';

async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const desde = new Date(`${FECHA}T00:00:00-06:00`);
  const hasta = new Date(`${FECHA}T23:59:59.999-06:00`);
  const cfdis = await CFDI.find({
    'emisor.rfc': RFC, serie: SERIE, tipoDeComprobante: 'I', source: 'ERP',
    fecha: { $gte: desde, $lte: hasta },
  }).select('uuid serie folio fecha total metodoPago formaPago tipoDeComprobante receptor.nombre').lean();

  const cfdiObjetivo = cfdis.find(c => c.folio === FOLIO_OBJETIVO);
  console.log('CFDI objetivo:', JSON.stringify(cfdiObjetivo));

  const cfdiConRegla = cfdis.map(cfdi => ({ cfdi, rule: { cuentaCargo: '1101010003' } }));
  const { desglosePagoReal, puntosUsado, saldoFavorUsado } = await _prefetchAjustesFacturaPropia(cfdiConRegla, RFC, {
    centroPropioClave: SERIE, fechaDesde: desde, fechaHasta: hasta,
  });

  const key = `${SERIE}|${FOLIO_OBJETIVO}`;
  const formasPago = desglosePagoReal.get(key) ?? [];
  console.log(`\nTotal lineas de formasPago para ${key}:`, formasPago.length);

  const porClave = {};
  let sumaTotal = 0;
  for (const fp of formasPago) {
    const clave = fp.claveSat ?? '??';
    porClave[clave] = (porClave[clave] || 0) + (Number(fp.monto) || 0);
    sumaTotal += Number(fp.monto) || 0;
  }
  console.log('Suma por claveSat:', JSON.stringify(porClave, null, 2));
  console.log('Suma total de formasPago:', sumaTotal.toFixed(2));
  console.log('Total declarado del CFDI:', cfdiObjetivo?.total);
  console.log('Diferencia (CFDI.total - sumaFormasPago):', ((Number(cfdiObjetivo?.total) || 0) - sumaTotal).toFixed(2));

  const puntos = puntosUsado.get(key);
  const sf = saldoFavorUsado.get(key);
  console.log('\nPuntos usados en esta factura:', puntos);
  console.log('Saldo a favor usado en esta factura:', JSON.stringify(sf));

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
