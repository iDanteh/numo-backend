'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = 'CCO011113663';
const CENTRO = 'B0';
const DIA_OBJETIVO = '2026-08-11';

async function main() {
  await connectMongo();

  // Ventana amplia como la usa el generador (+-1 dia para el fetch, pero filtramos exacto por dia de cobro).
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({
    rfc: RFC, centro: CENTRO,
    fechaDesde: '2026-08-01T00:00:00-06:00', fechaHasta: '2026-08-20T23:59:59.999-06:00',
  });

  const detalle = [];
  let totalEfectivo = 0, totalTarjeta = 0;
  for (const cuenta of resultado) {
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro && cobro.claveCentro !== CENTRO) continue;
      const d = new Date(cobro.fecha); d.setHours(d.getHours() - 6);
      const dia = d.toISOString().slice(0, 10);
      if (dia !== DIA_OBJETIVO) continue;
      for (const fp of (cobro.formasPago ?? [])) {
        const monto = (cobro.formasPago.length === 1) ? Math.abs(Number(cobro.monto) || 0) : (Number(fp.monto) || 0);
        if (fp.claveSat === '01') totalEfectivo += monto;
        if (fp.claveSat === '04' || fp.claveSat === '28') totalTarjeta += monto;
        if (fp.claveSat === '01' || fp.claveSat === '04' || fp.claveSat === '28') {
          detalle.push({ folioVenta: cuenta.folioVenta, folioFactura: cuenta.folioFactura, monto, tipo: fp.claveSat === '01' ? 'EF' : 'TARJ' });
        }
      }
    }
  }
  console.log(`Efectivo real (centro=${CENTRO}) para ${DIA_OBJETIVO}: ${totalEfectivo.toFixed(2)}`);
  console.log(`Tarjeta real (centro=${CENTRO}) para ${DIA_OBJETIVO}: ${totalTarjeta.toFixed(2)}`);

  const foliosFactura = [...new Set(detalle.map(d => d.folioFactura).filter(Boolean))];
  const facturas = await CFDI.find({ serie: CENTRO, folio: { $in: foliosFactura }, source: 'SAT' }).select('folio fecha').lean();
  const facturaPorFolio = new Map(facturas.map(f => [f.folio, f]));

  console.log('\nFacturas fuera de tolerancia (+-1 dia):');
  let sumaFueraEf = 0, sumaFueraTarj = 0;
  const porFolio = new Map();
  for (const d of detalle) {
    const key = d.folioFactura;
    if (!porFolio.has(key)) porFolio.set(key, { ef: 0, tarj: 0 });
    const p = porFolio.get(key);
    if (d.tipo === 'EF') p.ef += d.monto; else p.tarj += d.monto;
  }
  for (const [folioFactura, montos] of porFolio) {
    const f = facturaPorFolio.get(folioFactura);
    const fechaFactura = f ? new Date(f.fecha).toISOString().slice(0, 10) : 'NO ENCONTRADA';
    const diffDias = f ? Math.round((new Date(fechaFactura) - new Date(DIA_OBJETIVO)) / 86400000) : null;
    const fuera = diffDias !== null && Math.abs(diffDias) > 1;
    if (fuera) {
      sumaFueraEf += montos.ef; sumaFueraTarj += montos.tarj;
      console.log(`  folioFactura=${folioFactura} fechaFactura=${fechaFactura} diffDias=${diffDias} EF=${montos.ef.toFixed(2)} TARJ=${montos.tarj.toFixed(2)}`);
    }
  }
  console.log(`\nSuma fuera de tolerancia: EF=${sumaFueraEf.toFixed(2)} TARJ=${sumaFueraTarj.toFixed(2)}`);

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
