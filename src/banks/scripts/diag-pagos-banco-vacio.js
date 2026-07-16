'use strict';

/**
 * diag-pagos-banco-vacio.js
 * Investiga por que el reporte "Pagos del banco" (tipoDeComprobante='P' con
 * doctosRelacionados) no muestra nada en ciertos periodos, aunque las
 * polizas de esos mismos periodos si tengan CFDIs con banco vinculado.
 * Solo lectura.
 *
 * Uso:
 *   node src/banks/scripts/diag-pagos-banco-vacio.js <rfc> <ejercicio> <periodo>
 */

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../../visor/models/CFDI');

const rfc = process.argv[2];
const ejercicio = parseInt(process.argv[3]);
const periodo = parseInt(process.argv[4]);

if (!rfc || !ejercicio || !periodo) {
  console.error('Uso: node diag-pagos-banco-vacio.js <rfc> <ejercicio> <periodo>');
  process.exit(1);
}

async function main() {
  await connectMongo();

  const totalP = await CFDI.countDocuments({
    $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
    ejercicio, periodo,
    tipoDeComprobante: 'P',
    isActive: true,
  });
  console.log('Total CFDI tipo P (cualquier fuente/estado) en ' + rfc + ' ' + ejercicio + '-' + periodo + ': ' + totalP);

  const totalPConDoctos = await CFDI.countDocuments({
    $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
    ejercicio, periodo,
    tipoDeComprobante: 'P',
    isActive: true,
    'complementoPago.pagos.doctosRelacionados.0': { $exists: true },
  });
  console.log('...de esos, con complementoPago.pagos.doctosRelacionados no vacio: ' + totalPConDoctos);

  const porSource = await CFDI.aggregate([
    { $match: {
      $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio, periodo,
      tipoDeComprobante: 'P',
    } },
    { $group: { _id: { source: '$source', isActive: '$isActive' }, count: { $sum: 1 } } },
  ]);
  console.log('Desglose por source/isActive:');
  console.log(porSource);

  if (totalPConDoctos > 0) {
    const muestra = await CFDI.find({
      $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio, periodo,
      tipoDeComprobante: 'P',
      isActive: true,
      'complementoPago.pagos.doctosRelacionados.0': { $exists: true },
    }).select('uuid source complementoPago.pagos.fechaPago complementoPago.pagos.doctosRelacionados').limit(3).lean();
    console.log('Muestra de 3 CFDIs P con doctosRelacionados:');
    console.log(JSON.stringify(muestra, null, 2));
  } else {
    console.log('CERO CFDIs tipo P con doctosRelacionados en este periodo -- por eso el reporte sale vacio.');
    console.log('Revisando si hay CFDIs tipo P en este periodo SIN doctosRelacionados (para descartar problema de estructura)...');
    const sinDoctos = await CFDI.find({
      $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio, periodo,
      tipoDeComprobante: 'P',
      isActive: true,
    }).select('uuid source complementoPago').limit(3).lean();
    console.log(JSON.stringify(sinDoctos, null, 2));
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(function (err) { console.error(err); process.exit(1); });
