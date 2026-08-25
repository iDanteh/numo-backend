'use strict';
require('dotenv').config();
const { PolizaMovimiento, Poliza, AccountPlan } = require('./src/shared/models/postgres');
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = 'CCO011113663';
const CENTRO = 'B0';
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
  // 1. Poliza activa del 7-ago para B0 y sus lineas COBRO-SIN-FACTURA.
  const polizas7ago = await Poliza.findAll({ where: { fecha: '2026-08-07' }, raw: true });
  console.log('Polizas 2026-08-07 (todas):', polizas7ago.map(p => `id=${p.id} estado=${p.estado} centro=${p.centroCostoId ?? p.centro ?? '?'}`));

  const movsCSF = await PolizaMovimiento.findAll({ where: { reglaNombre: 'COBRO-SIN-FACTURA' }, raw: true, order: [['polizaId', 'DESC']] });
  const polizaIdsCSF = [...new Set(movsCSF.map(m => m.polizaId))];
  const polizasCSF = await Poliza.findAll({ where: { id: polizaIdsCSF }, attributes: ['id', 'fecha', 'estado'], raw: true });
  const polizaPorId = new Map(polizasCSF.map(p => [p.id, p]));
  console.log('\n-- Todas las lineas COBRO-SIN-FACTURA en toda la BD --');
  for (const m of movsCSF) {
    const p = polizaPorId.get(m.polizaId);
    console.log(JSON.stringify({ polizaId: m.polizaId, polizaFecha: p?.fecha, polizaEstado: p?.estado, formaPago: m.formaPago, debe: m.debe, concepto: m.concepto }));
  }

  // 2. Replicar la logica interna de _cobrosSinFacturaPorCentro para el 7-ago,
  //    ver si el ticket B0-260801859 (venta real del CFDI 260801224) califica.
  await connectMongo();
  const CFDI = require('./src/visor/models/CFDI');

  const fechaDesdeISO = new Date('2026-08-07T00:00:00-06:00').toISOString();
  const fechaHastaISO = new Date('2026-08-07T23:59:59.999-06:00').toISOString();
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeISO, fechaHasta: fechaHastaISO });

  const foliosFacturaReferenciados = new Set();
  for (const cuenta of resultado) {
    if (cuenta.serieFactura && cuenta.folioFactura) foliosFacturaReferenciados.add(`${cuenta.serieFactura}|${cuenta.folioFactura}`);
  }
  const orConditions = [...foliosFacturaReferenciados].map(k => { const [serie, folio] = k.split('|'); return { serie, folio }; });
  const cfdisReferenciados = orConditions.length ? await CFDI.find({ $or: orConditions }).select('serie folio fecha').lean() : [];
  const diaCfdiPorFolioFactura = new Map();
  for (const c of cfdisReferenciados) {
    const key = `${c.serie}|${c.folio}`;
    const dia = diaMx(c.fecha);
    const actual = diaCfdiPorFolioFactura.get(key);
    if (!actual || dia < actual) diaCfdiPorFolioFactura.set(key, dia);
  }

  console.log('\n-- Buscando ticket B0-260801859 en resultado ERP del 7-ago --');
  let totalTarjetaInyectadaSimulada = 0;
  for (const cuenta of resultado) {
    const esNuestroTicket = cuenta.serieVenta === 'B0' && String(cuenta.folioVenta) === '260801859';
    const facturaKey = (cuenta.serieFactura && cuenta.folioFactura) ? `${cuenta.serieFactura}|${cuenta.folioFactura}` : null;
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro !== CENTRO) continue;
      const dCobro = diaMx(cobro.fecha);
      if (dCobro !== '2026-08-07') continue;
      let motivo = 'OK-pasa-filtros';
      let saltado = false;
      if (facturaKey) {
        const diaCfdi = diaCfdiPorFolioFactura.get(facturaKey);
        if (diaCfdi && diffDiasMx(cobro.fecha, diaCfdi) <= TOLERANCIA) { motivo = `SALTADO (diaCfdi=${diaCfdi} dentro de tolerancia)`; saltado = true; }
      }
      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      const origenOk = origen === 'CBT' || origen === 'APS' || origen === 'MIS' || SERIES_CON_AUTH.includes(origen);
      if (!saltado && !origenOk) { motivo = `SALTADO (serieOrigen=${origen} no calificado)`; saltado = true; }

      if (esNuestroTicket || !saltado) {
        console.log(JSON.stringify({
          esNuestroTicket, serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta,
          serieFactura: cuenta.serieFactura, folioFactura: cuenta.folioFactura,
          serieOrigen: cobro.serieOrigen, fecha: cobro.fecha, monto: cobro.monto,
          formasPago: cobro.formasPago, motivo,
        }));
      }
      if (!saltado) {
        for (const fp of (cobro.formasPago ?? [])) {
          if (/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
          const monto = (cobro.formasPago.length === 1 && cobro.monto != null) ? Math.abs(Number(cobro.monto) || 0) : (Number(fp.monto) || 0);
          if ((fp.claveSat ?? '').trim() === '04' || (fp.claveSat ?? '').trim() === '28') totalTarjetaInyectadaSimulada += monto;
        }
      }
    }
  }
  console.log(`\nTotal Tarjeta que _cobrosSinFacturaPorCentro inyectaria para 7-ago (simulado): ${totalTarjetaInyectadaSimulada.toFixed(2)}`);

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
