'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');
const { _extraerDocumentosRelacionados } = require('./src/banks/domains/cfdi-mapping/cobros-sucursal-puente.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'B0';
const FECHA_DESDE = process.env.DIAG_DESDE || '2026-08-07T00:00:00-06:00';
const FECHA_HASTA = process.env.DIAG_HASTA || '2026-08-07T23:59:59.999-06:00';

async function main() {
  await connectMongo();

  const cfdis = await CFDI.find({
    'emisor.rfc': RFC,
    serie: SERIE,
    tipoDeComprobante: 'I',
    fecha: { $gte: new Date(FECHA_DESDE), $lte: new Date(FECHA_HASTA) },
  }).select('uuid serie folio total metodoPago documentosRelacionados cfdiRelacionados').lean();

  console.log(`Total facturas tipo I, serie ${SERIE}, ${FECHA_DESDE} a ${FECHA_HASTA}: ${cfdis.length}`);

  // Replica EXACTA de ticketsPropioPorClave en cfdi-poliza-generator.service.js
  const ticketsPropioPorClave = new Map();
  let sinDocumentosRelacionados = 0;
  for (const cfdi of cfdis) {
    if (cfdi.metodoPago !== 'PUE') continue;
    if (!cfdi.documentosRelacionados?.length) { sinDocumentosRelacionados++; continue; }
    const ticket = _extraerDocumentosRelacionados(cfdi)[0];
    if (!ticket || ticket.serie === 'OPA' || (ticket.serie === cfdi.serie && ticket.folio === cfdi.folio)) continue;
    ticketsPropioPorClave.set(`${cfdi.serie}|${cfdi.folio}`, ticket);
  }
  console.log(`Facturas PUE sin documentosRelacionados: ${sinDocumentosRelacionados}`);
  console.log(`Facturas con un ticket candidato (ticketsPropioPorClave): ${ticketsPropioPorClave.size}`);

  const candidatosPorClave = new Map(cfdis.map(c => [`${c.serie}|${c.folio}`, c]));
  const facturasPorTicket = new Map(); // ticketKey -> [facturaKey,...]
  for (const [facturaKey, ticket] of ticketsPropioPorClave) {
    const ticketKey = `${ticket.serie}|${ticket.folio}`;
    const arr = facturasPorTicket.get(ticketKey) ?? [];
    arr.push(facturaKey);
    facturasPorTicket.set(ticketKey, arr);
  }

  const gruposCompartidos = [...facturasPorTicket.entries()].filter(([, facturas]) => facturas.length > 1);
  console.log(`\nGrupos con MAS DE UNA factura compartiendo el mismo ticket (documentosRelacionados, SIN validar): ${gruposCompartidos.length}`);

  for (const [ticketKey, facturaKeys] of gruposCompartidos) {
    console.log(`\n--- ticket candidato ${ticketKey} <- ${facturaKeys.length} facturas ---`);
    for (const facturaKey of facturaKeys) {
      const cfdi = candidatosPorClave.get(facturaKey);
      // Verificacion cruzada: hay ALGUNA relacion oficial del SAT (cfdiRelacionados)
      // entre esta factura y cualquier otra del mismo grupo?
      const relSatUuids = new Set((cfdi.cfdiRelacionados ?? []).flatMap(r => (r.uuids ?? (r.uuid ? [r.uuid] : []))).map(u => (u || '').toUpperCase()));
      const otrasDelGrupoUuids = facturaKeys
        .filter(fk => fk !== facturaKey)
        .map(fk => candidatosPorClave.get(fk)?.uuid?.toUpperCase())
        .filter(Boolean);
      const respaldadoPorSat = otrasDelGrupoUuids.some(u => relSatUuids.has(u));
      console.log(`  ${facturaKey} | uuid=${cfdi.uuid} | total=${cfdi.total} | respaldado por cfdiRelacionados SAT: ${respaldadoPorSat}`);
    }
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
