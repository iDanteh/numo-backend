'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

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

  const cfdi = await CFDI.findOne({ serie: SERIE_FACTURA, folio: FOLIO_FACTURA, source: 'SAT' })
    .select('uuid folio serie total fecha metodoPago formaPago').lean();
  console.log('CFDI factura:', JSON.stringify(cfdi));

  const diaCfdi = cfdi ? diaMx(cfdi.fecha) : null;
  const fechaDesdeISO = new Date(`${diaCfdi}T00:00:00-06:00`).toISOString();
  const fechaHastaISO = new Date(`${diaCfdi}T23:59:59.999-06:00`).toISOString();

  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeISO, fechaHasta: fechaHastaISO });

  const cuentasDeEstaFactura = resultado.filter(c => c.serieFactura === SERIE_FACTURA && c.folioFactura === FOLIO_FACTURA);
  console.log(`\nCuentas (tickets) ligadas a la factura ${SERIE_FACTURA}-${FOLIO_FACTURA}: ${cuentasDeEstaFactura.length}`);

  let totalFormasPagoReal = 0;
  let totalCobrosDescartados = 0;
  const descartados = [];
  for (const cuenta of cuentasDeEstaFactura) {
    for (const cobro of (cuenta.cobros ?? [])) {
      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      const origenOk = origen === 'CBT' || origen === 'APS' || origen === 'MIS' || SERIES_CON_AUTH.includes(origen);
      const dCobro = diaMx(cobro.fecha);
      const mismoDia = dCobro === diaCfdi;
      if (!origenOk || !mismoDia) {
        totalCobrosDescartados += Math.abs(Number(cobro.monto) || 0);
        descartados.push({ folioVenta: cuenta.folioVenta, origen, mismoDia, diaCobro: dCobro, monto: cobro.monto, fecha: cobro.fecha });
        continue;
      }
      const formasPago = cobro.formasPago ?? [];
      for (const fp of formasPago) {
        if (/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        const monto = (formasPago.length === 1 && cobro.monto != null) ? Math.abs(Number(cobro.monto) || 0) : (Number(fp.monto) || 0);
        totalFormasPagoReal += monto;
      }
    }
  }

  console.log(`\nTotal CFDI (montoCargo aprox, sin IVA descontar nada): ${cfdi?.total}`);
  console.log(`Total formasPagoReal encontrado (Efectivo+Tarjeta+Transferencia, sin SF/Puntos): ${totalFormasPagoReal.toFixed(2)}`);
  console.log(`Diferencia (posible "exceso"/Venta Sin Cobro): ${(Number(cfdi?.total) - totalFormasPagoReal).toFixed(2)}`);

  console.log(`\nCobros descartados (origen no calificado o dia distinto), ${descartados.length} casos, suma=${totalCobrosDescartados.toFixed(2)}:`);
  for (const d of descartados) console.log(JSON.stringify(d));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
