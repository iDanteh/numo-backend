'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = 'CCO011113663';
const CENTRO = 'B0';
const SERIE_FACTURA = 'B0';
const FOLIO_FACTURA = '260801256';

function diaMx(fechaIso) {
  if (!fechaIso) return null;
  return new Date(new Date(fechaIso).getTime() - 6 * 3600 * 1000).toISOString().slice(0, 10);
}

async function main() {
  await connectMongo();
  const cfdi = await CFDI.findOne({ serie: SERIE_FACTURA, folio: FOLIO_FACTURA, source: 'SAT' }).select('uuid fecha total').lean();
  const diaCfdi = diaMx(cfdi.fecha);
  const fechaDesdeISO = new Date(`${diaCfdi}T00:00:00-06:00`).toISOString();
  const fechaHastaISO = new Date(`${diaCfdi}T23:59:59.999-06:00`).toISOString();

  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeISO, fechaHasta: fechaHastaISO });

  // Ticket (serieVenta|folioVenta) -> lista de folioFactura que lo reclaman.
  const facturasPorTicket = new Map();
  for (const c of resultado) {
    if (!c.serieFactura || !c.folioFactura) continue;
    const ticketKey = `${c.serieVenta}|${c.folioVenta}`;
    const arr = facturasPorTicket.get(ticketKey) ?? new Set();
    arr.add(`${c.serieFactura}|${c.folioFactura}`);
    facturasPorTicket.set(ticketKey, arr);
  }

  const cuentasDeEstaFactura = resultado.filter(c => c.serieFactura === SERIE_FACTURA && c.folioFactura === FOLIO_FACTURA);
  console.log(`Tickets de esta factura: ${cuentasDeEstaFactura.length}`);

  const compartidos = cuentasDeEstaFactura.filter(c => facturasPorTicket.get(`${c.serieVenta}|${c.folioVenta}`).size > 1);
  console.log(`\nTickets COMPARTIDOS con otras facturas: ${compartidos.length}`);
  for (const c of compartidos) {
    const facs = [...facturasPorTicket.get(`${c.serieVenta}|${c.folioVenta}`)];
    const montoTotalCobro = (c.cobros ?? []).reduce((s, cb) => s + Math.abs(Number(cb.monto) || 0), 0);
    console.log(JSON.stringify({ folioVenta: c.folioVenta, facturas: facs, montoTotalCobro }));
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
