'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'B0';
const FECHA_DESDE = process.env.DIAG_DESDE || '2026-07-25T00:00:00-06:00';
const FECHA_HASTA = process.env.DIAG_HASTA || '2026-08-16T23:59:59.999-06:00';
const DIA_OBJETIVO = process.env.DIAG_DIA || '2026-08-07';

async function main() {
  await connectMongo();

  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: FECHA_DESDE, fechaHasta: FECHA_HASTA });

  const detalleTarjetaDelDia = [];
  let totalTarjetaDia = 0;
  for (const cuenta of resultado) {
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro && cobro.claveCentro !== CENTRO) continue; // excluye cruces de sucursal, igual que produccion
      const d = new Date(cobro.fecha);
      d.setHours(d.getHours() - 6);
      const dia = d.toISOString().slice(0, 10);
      if (dia !== DIA_OBJETIVO) continue;
      for (const fp of (cobro.formasPago ?? [])) {
        if (fp.claveSat !== '04' && fp.claveSat !== '28') continue;
        // Bug conocido del ERP: cuando el cobro trae un solo formaPago, a veces
        // repite el TOTAL del pago (que puede cubrir varios tickets) en vez del
        // monto real de este ticket. El monto real de este ticket es cobro.monto
        // cuando solo hay un formaPago.
        const monto = (cobro.formasPago.length === 1)
          ? Math.abs(Number(cobro.monto) || 0)
          : (Number(fp.monto) || 0);
        totalTarjetaDia += monto;
        detalleTarjetaDelDia.push({ folioVenta: cuenta.folioVenta, folioFactura: cuenta.folioFactura, serieOrigen: cobro.serieOrigen, monto, nombreFp: fp.nombre, montoOriginalFp: Number(fp.monto) || 0, corregido: cobro.formasPago.length === 1 });
      }
    }
  }

  const totalSinCorregir = detalleTarjetaDelDia.reduce((s, d) => s + d.montoOriginalFp, 0);
  const renglonesCorregidos = detalleTarjetaDelDia.filter(d => d.corregido && d.monto !== d.montoOriginalFp);

  console.log(`Tarjeta real (solo claveCentro=${CENTRO}) para ${DIA_OBJETIVO}: ${totalTarjetaDia.toFixed(2)}`);
  console.log(`  (sin corrección de "monto repetido" hubiera dado: ${totalSinCorregir.toFixed(2)}, diferencia: ${(totalSinCorregir - totalTarjetaDia).toFixed(2)})`);
  console.log(`  Renglones afectados por la corrección: ${renglonesCorregidos.length}`);
  for (const r of renglonesCorregidos) {
    console.log(`    folioVenta=${r.folioVenta} folioFactura=${r.folioFactura} montoOriginalFp=${r.montoOriginalFp.toFixed(2)} -> montoCorregido=${r.monto.toFixed(2)}`);
  }
  console.log(`Total renglones: ${detalleTarjetaDelDia.length}`);

  const foliosFactura = [...new Set(detalleTarjetaDelDia.map(d => d.folioFactura).filter(Boolean))];
  const facturas = await CFDI.find({ serie: CENTRO, folio: { $in: foliosFactura } }).select('folio fecha total formaPago metodoPago tipoDeComprobante').lean();
  const facturaPorFolio = new Map(facturas.map(f => [f.folio, f]));

  console.log('\nFacturas involucradas:');
  let sumaFueraTolerancia = 0;
  for (const folioFactura of foliosFactura) {
    const f = facturaPorFolio.get(folioFactura);
    const fechaFactura = f ? new Date(f.fecha).toISOString().slice(0, 10) : 'NO ENCONTRADA';
    const diffDias = f ? Math.round((new Date(fechaFactura) - new Date(DIA_OBJETIVO)) / 86400000) : null;
    const montoTarjetaDeEstaFactura = detalleTarjetaDelDia.filter(d => d.folioFactura === folioFactura).reduce((s, d) => s + d.monto, 0);
    const fueraTolerancia = diffDias !== null && Math.abs(diffDias) > 1;
    if (fueraTolerancia) sumaFueraTolerancia += montoTarjetaDeEstaFactura;
    console.log(`  folioFactura=${folioFactura} fechaFactura=${fechaFactura} diffDias=${diffDias} montoTarjeta=${montoTarjetaDeEstaFactura.toFixed(2)} ${fueraTolerancia ? '<<< FUERA DE TOLERANCIA' : ''}`);
  }
  console.log(`\nSuma Tarjeta cuyo folioFactura se timbro fuera de +-1 dia: ${sumaFueraTolerancia.toFixed(2)}`);

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
