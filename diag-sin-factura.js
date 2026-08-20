'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';

// Busca cobros reales (Efectivo/Tarjeta/etc, origen reconocido) de cuentas
// que NO tienen serieFactura/folioFactura -- dinero real cobrado pero sin
// ninguna factura (ni Global ni individual) a la cual atarse. El pipeline de
// polizas (CFDI-driven) no puede representar este dinero en absoluto porque
// no existe ningun CFDI; los diagnosticos crudos SI lo cuentan (no dependen
// de CFDI), lo que explicaria la brecha grande contra la poliza real.
async function main() {
  await connectMongo();

  const fechaDesdeIso = new Date(`${FECHA}T00:00:00-06:00`).toISOString();
  const fechaHastaIso = new Date(`${FECHA}T23:59:59.999-06:00`).toISOString();
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({
    rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso,
  });

  const porClave = {};
  const detalle = [];
  const vistos = new Set();

  for (const cuenta of resultado) {
    if (cuenta.serieFactura && cuenta.folioFactura) continue; // SI tiene factura -- no es el caso que buscamos
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro !== CENTRO) continue;
      const fechaCobroMx = new Date(cobro.fecha);
      fechaCobroMx.setHours(fechaCobroMx.getHours() - 6);
      const diaCobro = fechaCobroMx.toISOString().slice(0, 10);
      if (diaCobro !== FECHA) continue;

      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      if (origen !== 'CBT' && origen !== 'APS' && origen !== 'MIS' && !SERIES_CON_AUTH.includes(origen)) continue;

      const dedupeKey = `${cobro.serieOrigen}|${cobro.folioOrigen}|${cuenta.serieVenta}|${cuenta.folioVenta}`;
      if (vistos.has(dedupeKey)) continue;
      vistos.add(dedupeKey);

      for (const fp of (cobro.formasPago ?? [])) {
        if (/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        const monto = (cobro.formasPago.length === 1 && cobro.monto != null)
          ? Math.abs(Number(cobro.monto) || 0)
          : (Number(fp.monto) || 0);
        const clave = fp.claveSat ?? '??';
        porClave[clave] = (porClave[clave] || 0) + monto;
      }
      detalle.push({
        serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta,
        serieOrigen: cobro.serieOrigen, folioOrigen: cobro.folioOrigen,
        monto: cobro.monto, formasPago: cobro.formasPago,
      });
    }
  }

  console.log(`Suma por claveSat de cobros SIN factura (${CENTRO}, ${FECHA}):`, JSON.stringify(porClave, null, 2));
  console.log(`\nTotal registros sin factura: ${detalle.length}`);
  console.log('\nDetalle:');
  for (const d of detalle) console.log(JSON.stringify(d));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
