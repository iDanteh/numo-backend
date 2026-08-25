'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = 'CCO011113663';
const CENTRO = 'B0';
const SERIE_FACTURA = 'B0';
const FOLIO_FACTURA = '260801256';
const TASA_IVA = 0.16;

function diaMx(fechaIso) {
  if (!fechaIso) return null;
  return new Date(new Date(fechaIso).getTime() - 6 * 3600 * 1000).toISOString().slice(0, 10);
}

async function main() {
  await connectMongo();
  const cfdi = await CFDI.findOne({ serie: SERIE_FACTURA, folio: FOLIO_FACTURA, source: 'SAT' })
    .select('uuid fecha total subTotal conceptos').lean();
  const diaCfdi = diaMx(cfdi.fecha);

  console.log(`CFDI total=${cfdi.total} subTotal=${cfdi.subTotal} conceptos=${cfdi.conceptos.length}`);

  const UN_DIA_MS = 24 * 3600 * 1000;
  const fechaDesdeAmpliada = new Date(new Date(`${diaCfdi}T00:00:00-06:00`).getTime() - UN_DIA_MS);
  const fechaHastaAmpliada = new Date(new Date(`${diaCfdi}T23:59:59.999-06:00`).getTime() + UN_DIA_MS);
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeAmpliada.toISOString(), fechaHasta: fechaHastaAmpliada.toISOString() });
  const cuentasPorTicket = new Map();
  for (const c of resultado) {
    if (c.serieFactura !== SERIE_FACTURA || c.folioFactura !== FOLIO_FACTURA) continue;
    cuentasPorTicket.set(c.folioVenta, c);
  }

  let sumEsperado = 0, sumEncontrado = 0, sumDiff = 0;
  const conDiferencia = [];
  const sinCuentaEnErp = [];

  for (const concepto of cfdi.conceptos) {
    const folioTicket = concepto.noIdentificacion;
    const montoEsperado = Math.round(Number(concepto.importe) * (1 + TASA_IVA) * 100) / 100;
    sumEsperado += montoEsperado;

    const cuenta = cuentasPorTicket.get(folioTicket);
    if (!cuenta) {
      sinCuentaEnErp.push({ folioTicket, montoEsperado });
      sumDiff += montoEsperado;
      continue;
    }
    let montoEncontrado = 0;
    for (const cobro of (cuenta.cobros ?? [])) {
      montoEncontrado += Math.abs(Number(cobro.monto) || 0);
    }
    sumEncontrado += montoEncontrado;
    const diff = Math.round((montoEsperado - montoEncontrado) * 100) / 100;
    if (Math.abs(diff) > 0.5) {
      conDiferencia.push({ folioTicket, montoEsperado, montoEncontrado, diff });
      sumDiff += diff;
    }
  }

  console.log(`\nSuma esperada (conceptos * 1.16): ${sumEsperado.toFixed(2)}`);
  console.log(`Suma encontrada (cobros.monto abs, TODOS los tipos): ${sumEncontrado.toFixed(2)}`);
  console.log(`Suma de diferencias (tickets con diff>0.5 + sin cuenta ERP): ${sumDiff.toFixed(2)}`);

  console.log(`\nTickets del CFDI SIN cuenta en el ERP (${sinCuentaEnErp.length}):`);
  for (const t of sinCuentaEnErp) console.log(JSON.stringify(t));

  console.log(`\nTickets con diferencia notable entre esperado y encontrado (${conDiferencia.length}):`);
  for (const t of conDiferencia.sort((a, b) => Math.abs(b.diff) - Math.abs(a.diff))) console.log(JSON.stringify(t));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
