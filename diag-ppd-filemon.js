'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

const RFC = process.env.DIAG_RFC || 'CCO011113663';

async function main() {
  await connectMongo();

  console.log('\n=== 1. CFDI por UUID directo (FILEMON A0-260801889) ===\n');
  const porUuid = await CFDI.findOne({ uuid: '5C07B49D-D5CE-408A-ADB1-6B67C0B5A3AB' })
    .select('serie folio uuid metodoPago receptor.nombre emisor.rfc').lean();
  console.log(JSON.stringify(porUuid, null, 2));

  console.log('\n=== 2. CFDI por serie/folio de FACTURA (lo que usa cobrosCobradoraDirecta) ===\n');
  const porSerieFolio = await CFDI.find({
    'emisor.rfc': RFC,
    $or: [{ serie: 'A0', folio: '260801889' }],
  }).select('serie folio uuid metodoPago receptor.nombre').lean();
  console.log(JSON.stringify(porSerieFolio, null, 2));

  console.log('\n=== 3. Todos los CFDIs de FILEMON con metodoPago (para ver el patron) ===\n');
  const deFilemon = await CFDI.find({
    'emisor.rfc': RFC,
    'receptor.nombre': /FILEMON AGUDO/i,
  }).select('serie folio uuid metodoPago fecha total').sort({ fecha: -1 }).limit(15).lean();
  console.log(JSON.stringify(deFilemon, null, 2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
