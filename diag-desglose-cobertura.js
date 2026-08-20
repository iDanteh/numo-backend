'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { _prefetchAjustesFacturaPropia } = require('./src/banks/domains/cfdi-mapping/cfdi-poliza-generator.service.js');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';

// Reproduce lo que hace la generacion real: para cada factura tipo I del
// batch, revisa si `desglosePagoReal` tiene un split real -- si NO lo tiene,
// esa factura cae al `formaPago` declarado por el CFDI (que casi siempre
// viene mal/generico, segun los comentarios del propio codigo). Esto mide
// cuanto dinero (y bajo que formaPago declarado) esta cayendo a ese fallback,
// para explicar la brecha de Efectivo en la poliza REAL (no en los
// diagnosticos crudos, que no replican este fallback).
async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const desde = new Date(`${FECHA}T00:00:00-06:00`);
  const hasta = new Date(`${FECHA}T23:59:59.999-06:00`);
  const cfdis = await CFDI.find({
    'emisor.rfc': RFC, serie: SERIE, tipoDeComprobante: 'I', source: 'ERP',
    fecha: { $gte: desde, $lte: hasta },
  }).select('uuid serie folio fecha total metodoPago formaPago receptor.nombre').lean();
  console.log(`Total CFDIs tipo I del batch (${SERIE}, ${FECHA}):`, cfdis.length);

  const cfdiConRegla = cfdis.map(cfdi => ({ cfdi, rule: { cuentaCargo: '1101010003' } }));
  const { desglosePagoReal, usoCaminoPorCentro, cobrosCobradoraDirecta, puntosUsado, saldoFavorUsado } = await _prefetchAjustesFacturaPropia(cfdiConRegla, RFC, {
    centroPropioClave: SERIE, fechaDesde: desde, fechaHasta: hasta,
  });

  console.log('usoCaminoPorCentro:', usoCaminoPorCentro);
  console.log('cobrosCobradoraDirecta.length:', cobrosCobradoraDirecta?.length);
  console.log('puntosUsado.size:', puntosUsado?.size);
  console.log('saldoFavorUsado.size:', saldoFavorUsado?.size);
  console.log('Claves con desglosePagoReal encontrado:', desglosePagoReal.size);
  console.log('Todas las claves de desglosePagoReal:', JSON.stringify([...desglosePagoReal.keys()]));

  let totalConDesglose = 0;
  let totalSinDesglose = 0;
  const sinDesglosePorFormaPago = {};
  const sinDesgloseDetalle = [];

  for (const cfdi of cfdis) {
    const key = `${cfdi.serie}|${cfdi.folio}`;
    const tieneDesglose = desglosePagoReal.has(key);
    if (tieneDesglose) {
      totalConDesglose += Number(cfdi.total) || 0;
    } else {
      totalSinDesglose += Number(cfdi.total) || 0;
      const fp = cfdi.formaPago ?? 'SIN_DATO';
      sinDesglosePorFormaPago[fp] = (sinDesglosePorFormaPago[fp] || 0) + (Number(cfdi.total) || 0);
      sinDesgloseDetalle.push({ serie: cfdi.serie, folio: cfdi.folio, total: cfdi.total, formaPago: cfdi.formaPago, receptor: cfdi.receptor?.nombre, uuid: cfdi.uuid });
    }
  }

  console.log(`\nTotal $ de facturas CON desglosePagoReal: $${totalConDesglose.toFixed(2)}`);
  console.log(`Total $ de facturas SIN desglosePagoReal (caen al formaPago declarado del CFDI): $${totalSinDesglose.toFixed(2)}`);
  console.log('\nSuma de facturas SIN desglose, agrupadas por formaPago declarado en el CFDI:');
  console.log(JSON.stringify(sinDesglosePorFormaPago, null, 2));
  console.log('\nDetalle de facturas SIN desglosePagoReal:');
  for (const d of sinDesgloseDetalle) console.log(JSON.stringify(d));

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
