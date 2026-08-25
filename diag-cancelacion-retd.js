'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');

// Investigar si la cancelacion RETD (ticket B0 260802634, factura B0 260802634,
// ABO 260809345, OPERADORA DE FRANQUICIAS SEB, $132.59, 11-ago) tiene una Nota
// de Credito (CFDI tipo E) real asociada -- si existe, el pipeline de polizas
// YA la neteria via el manejo existente de NC, y el gap solo viviria en el
// diagnostico standalone (que no cruza contra CFDIs), no en produccion.
async function main() {
  await connectMongo();

  const receptor = /FRANQUICIAS SEB/i;
  const candidatos = await CFDI.find({
    fecha: { $gte: new Date('2026-08-01T00:00:00Z'), $lte: new Date('2026-08-20T23:59:59Z') },
    'receptor.nombre': receptor,
  }).select('uuid tipoDeComprobante serie folio fecha total documentosRelacionados receptor.nombre').lean();

  console.log(`CFDIs encontrados para receptor ~FRANQUICIAS SEB (ago 2026): ${candidatos.length}`);
  for (const c of candidatos) {
    console.log(JSON.stringify({
      uuid: c.uuid, tipo: c.tipoDeComprobante, serie: c.serie, folio: c.folio,
      fecha: c.fecha, total: c.total, documentosRelacionados: c.documentosRelacionados,
    }));
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
