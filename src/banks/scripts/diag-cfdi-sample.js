'use strict';

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../../visor/models/CFDI');

async function main() {
  await connectMongo();

  // Ver un CFDI reciente del RFC para entender la estructura
  const sample = await CFDI.findOne({
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    tipoDeComprobante: 'I',
  })
    .select('fecha ejercicio periodo source satStatus isActive tipoDeComprobante metodoPago emisor.rfc receptor.rfc')
    .lean();

  console.log('Sample CFDI:');
  console.log(JSON.stringify(sample, null, 2));

  // Ver qué valores distintos tienen ejercicio y periodo
  const ejercicios = await CFDI.distinct('ejercicio', {
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
  });
  console.log('\nValores de ejercicio:', ejercicios.sort());

  const periodos = await CFDI.distinct('periodo', {
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    ejercicio: ejercicios[ejercicios.length - 1],
  });
  console.log('Valores de periodo (ultimo ejercicio):', periodos.sort((a,b) => a-b));

  await disconnectMongo();
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
