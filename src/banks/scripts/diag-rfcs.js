'use strict';

require('dotenv').config();

const { connectMongo, disconnectMongo } = require('../../config/database.mongo');
const CFDI = require('../../visor/models/CFDI');

async function main() {
  await connectMongo();

  // 1. Sin ningún filtro — cuántos CFDIs hay en total con ese RFC
  const totalRFC = await CFDI.countDocuments({
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
  });
  console.log('Total CFDIs RFC CCO011113663 (sin filtros):', totalRFC);

  // 2. Solo con ejercicio/periodo
  const total25Feb = await CFDI.countDocuments({
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    ejercicio: 2025, periodo: 2,
  });
  console.log('Con ejercicio:2025 periodo:2:', total25Feb);

  // 3. Añadir tipo I
  const totalI = await CFDI.countDocuments({
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    ejercicio: 2025, periodo: 2, tipoDeComprobante: 'I',
  });
  console.log('Con tipoDeComprobante:I:', totalI);

  // 4. Añadir source SAT
  const totalSAT = await CFDI.countDocuments({
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    ejercicio: 2025, periodo: 2, tipoDeComprobante: 'I', source: 'SAT',
  });
  console.log('Con source:SAT:', totalSAT);

  // 5. Añadir satStatus Vigente
  const totalVigente = await CFDI.countDocuments({
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    ejercicio: 2025, periodo: 2, tipoDeComprobante: 'I', source: 'SAT', satStatus: 'Vigente',
  });
  console.log('Con satStatus:Vigente:', totalVigente);

  // 6. Añadir isActive true
  const totalActivo = await CFDI.countDocuments({
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    ejercicio: 2025, periodo: 2, tipoDeComprobante: 'I', source: 'SAT', satStatus: 'Vigente', isActive: true,
  });
  console.log('Con isActive:true:', totalActivo);

  // 7. Ver qué valores distintos tienen source y satStatus para este RFC/periodo
  const sources = await CFDI.distinct('source', {
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    ejercicio: 2025, periodo: 2, tipoDeComprobante: 'I',
  });
  console.log('Valores de source:', sources);

  const statuses = await CFDI.distinct('satStatus', {
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    ejercicio: 2025, periodo: 2, tipoDeComprobante: 'I',
  });
  console.log('Valores de satStatus:', statuses);

  // 8. Ver sources disponibles para tipo I Feb 2026
  const sources26 = await CFDI.distinct('source', {
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    ejercicio: 2026, periodo: 2, tipoDeComprobante: 'I',
  });
  console.log('\nSources tipo I Feb 2026:', sources26);

  const totalSinFiltro = await CFDI.countDocuments({
    $or: [{ 'emisor.rfc': 'CCO011113663' }, { 'receptor.rfc': 'CCO011113663' }],
    ejercicio: 2026, periodo: 2, tipoDeComprobante: 'I',
  });
  console.log('Total tipo I Feb 2026 (sin filtro source):', totalSinFiltro);

  await disconnectMongo();
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
