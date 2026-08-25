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
const TOLERANCIA = 1;

function diaMx(fechaIso) {
  if (!fechaIso) return null;
  return new Date(new Date(fechaIso).getTime() - 6 * 3600 * 1000).toISOString().slice(0, 10);
}
function diffDiasMx(fechaIso, diaYaResuelto) {
  const diaCobro = diaMx(fechaIso);
  if (!diaCobro || !diaYaResuelto) return null;
  const a = new Date(`${diaCobro}T00:00:00Z`).getTime();
  const b = new Date(`${diaYaResuelto}T00:00:00Z`).getTime();
  return Math.round(Math.abs(a - b) / 86400000);
}

async function main() {
  await connectMongo();
  const cfdi = await CFDI.findOne({ serie: SERIE_FACTURA, folio: FOLIO_FACTURA, source: 'SAT' }).select('uuid fecha total').lean();
  const diaCfdi = diaMx(cfdi.fecha);
  console.log('diaCfdi:', diaCfdi, 'total:', cfdi.total);

  // Ventana ampliada +-1 dia, igual que el camino "por centro" real.
  const UN_DIA_MS = 24 * 3600 * 1000;
  const fechaDesdeAmpliada = new Date(new Date(`${diaCfdi}T00:00:00-06:00`).getTime() - TOLERANCIA * UN_DIA_MS);
  const fechaHastaAmpliada = new Date(new Date(`${diaCfdi}T23:59:59.999-06:00`).getTime() + TOLERANCIA * UN_DIA_MS);

  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeAmpliada.toISOString(), fechaHasta: fechaHastaAmpliada.toISOString() });
  const cuentas = resultado.filter(c => c.serieFactura === SERIE_FACTURA && c.folioFactura === FOLIO_FACTURA);
  console.log(`Tickets ligados (ventana +-1 dia): ${cuentas.length}`);

  let totalFormasPagoReal = 0;
  const excluidosPorDia = [];
  for (const c of cuentas) {
    for (const cobro of (c.cobros ?? [])) {
      const diff = diffDiasMx(cobro.fecha, diaCfdi);
      if (diff === null || diff > TOLERANCIA) {
        excluidosPorDia.push({ folioVenta: c.folioVenta, origen: cobro.serieOrigen, fecha: cobro.fecha, monto: cobro.monto, diff });
        continue;
      }
      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      const origenOk = origen === 'CBT' || origen === 'APS' || origen === 'MIS' || SERIES_CON_AUTH.includes(origen);
      if (!origenOk) continue;
      const formasPago = cobro.formasPago ?? [];
      for (const fp of formasPago) {
        if (/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        const monto = (formasPago.length === 1 && cobro.monto != null) ? Math.abs(Number(cobro.monto) || 0) : (Number(fp.monto) || 0);
        totalFormasPagoReal += monto;
      }
    }
  }

  console.log(`\nTotal formasPagoReal (ventana +-1 dia, filtrado por tolerancia real): ${totalFormasPagoReal.toFixed(2)}`);
  console.log(`Excluidos por estar fuera de tolerancia de dia: ${excluidosPorDia.length}`);
  for (const e of excluidosPorDia) console.log(JSON.stringify(e));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
