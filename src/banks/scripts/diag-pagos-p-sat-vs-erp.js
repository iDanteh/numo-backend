'use strict';

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../../visor/models/CFDI');

const rfc = process.argv[2];
const ejercicio = parseInt(process.argv[3]);
const periodo = parseInt(process.argv[4]);

async function main() {
  await connectMongo();

  for (const source of ['SAT', 'ERP']) {
    const total = await CFDI.countDocuments({
      $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio, periodo,
      tipoDeComprobante: 'P',
      isActive: true,
      source,
    });
    const conDoctos = await CFDI.countDocuments({
      $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio, periodo,
      tipoDeComprobante: 'P',
      isActive: true,
      source,
      'complementoPago.pagos.doctosRelacionados.0': { $exists: true },
    });
    console.log(source + ': total=' + total + ' conDoctosRelacionados=' + conDoctos);

    const muestra = await CFDI.find({
      $or: [{ 'emisor.rfc': rfc }, { 'receptor.rfc': rfc }],
      ejercicio, periodo,
      tipoDeComprobante: 'P',
      isActive: true,
      source,
    }).select('uuid complementoPago.pagos.doctosRelacionados complementoPago.pagos.fechaPago').limit(2).lean();
    console.log(source + ' muestra:');
    console.log(JSON.stringify(muestra, null, 2));
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(function (err) { console.error(err); process.exit(1); });
