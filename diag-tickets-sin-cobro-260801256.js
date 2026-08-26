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
  const cuentas = resultado.filter(c => c.serieFactura === SERIE_FACTURA && c.folioFactura === FOLIO_FACTURA);

  console.log(`Total tickets ligados: ${cuentas.length}`);

  const sinCobros = cuentas.filter(c => !(c.cobros ?? []).length);
  console.log(`\nTickets con cobros=[] (vacio): ${sinCobros.length}`);
  for (const c of sinCobros) console.log(JSON.stringify({ folioVenta: c.folioVenta, fechaCreacion: c.fechaCreacion }));

  const sinMontoReal = [];
  for (const c of cuentas) {
    if (!(c.cobros ?? []).length) continue;
    let montoReal = 0;
    for (const cobro of c.cobros) {
      for (const fp of (cobro.formasPago ?? [])) {
        if (/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        const monto = (c.cobros.length === 1 && cobro.formasPago?.length === 1 && cobro.monto != null) ? Math.abs(Number(cobro.monto) || 0) : (Number(fp.monto) || 0);
        montoReal += monto;
      }
    }
    if (montoReal <= 0.01) sinMontoReal.push(c);
  }
  console.log(`\nTickets con cobros pero monto real = 0 (solo SF/Puntos u origen no reconocido): ${sinMontoReal.length}`);
  for (const c of sinMontoReal) console.log(JSON.stringify({ folioVenta: c.folioVenta, cobros: c.cobros }));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
