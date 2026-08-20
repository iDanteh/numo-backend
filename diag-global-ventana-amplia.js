'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'B0';
const FOLIO_GLOBAL = process.env.DIAG_FOLIO_GLOBAL || '260801256';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';
const DIAS_VENTANA = Number(process.env.DIAG_DIAS_VENTANA || 15);

// Consulta "por centro" pero con una ventana MUCHO mas amplia (+/- N dias)
// que la produccion real (que usa TOLERANCIA_DIAS_FACTURACION_DIFERIDA = 1
// dia) -- si aparecen cobros de esta factura FUERA de esa ventana estrecha,
// ahi esta el origen del "exceso" que hoy se manda a Caja sin identificar.
async function main() {
  await connectMongo();

  const fechaDesde = new Date(`${FECHA}T00:00:00-06:00`);
  fechaDesde.setDate(fechaDesde.getDate() - DIAS_VENTANA);
  const fechaHasta = new Date(`${FECHA}T23:59:59.999-06:00`);
  fechaHasta.setDate(fechaHasta.getDate() + DIAS_VENTANA);

  console.log(`Consultando ventana ${fechaDesde.toISOString()} a ${fechaHasta.toISOString()} (+/-${DIAS_VENTANA} dias)...`);
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({
    rfc: RFC, centro: CENTRO, fechaDesde: fechaDesde.toISOString(), fechaHasta: fechaHasta.toISOString(),
  });
  console.log('Total "cuentas" (tickets) en la ventana amplia para todo el centro:', resultado.length);

  const cuentasDeLaGlobal = resultado.filter(c => c.serieFactura === CENTRO && String(c.folioFactura) === FOLIO_GLOBAL);
  console.log(`Tickets ligados a la factura ${CENTRO}-${FOLIO_GLOBAL} en la ventana amplia:`, cuentasDeLaGlobal.length);

  const porClave = {};
  const vistos = new Set();
  const detalleFueraDeTolerancia = [];
  let totalFueraDeTolerancia = 0;

  for (const cuenta of cuentasDeLaGlobal) {
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro !== CENTRO) continue;
      const fechaCobroMx = new Date(cobro.fecha);
      fechaCobroMx.setHours(fechaCobroMx.getHours() - 6);
      const diaCobro = fechaCobroMx.toISOString().slice(0, 10);

      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      if (origen !== 'CBT' && origen !== 'APS' && origen !== 'MIS' && !SERIES_CON_AUTH.includes(origen)) continue;

      const dedupeKey = `${cobro.serieOrigen}|${cobro.folioOrigen}|${cuenta.serieVenta}|${cuenta.folioVenta}`;
      if (vistos.has(dedupeKey)) continue;
      vistos.add(dedupeKey);

      const formasPago = cobro.formasPago ?? [];
      let montoEsteCobro = 0;
      for (const fp of formasPago) {
        if (/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        const monto = (formasPago.length === 1 && cobro.monto != null)
          ? Math.abs(Number(cobro.monto) || 0)
          : (Number(fp.monto) || 0);
        const clave = fp.claveSat ?? '??';
        porClave[clave] = (porClave[clave] || 0) + monto;
        montoEsteCobro += monto;
      }

      // Tolerancia real de produccion: +/-1 dia respecto a la fecha de la
      // factura (2026-08-11). Fuera de eso, produccion NUNCA lo ve.
      const diffDias = Math.abs((new Date(diaCobro) - new Date(FECHA)) / 86400000);
      if (diffDias > 1) {
        detalleFueraDeTolerancia.push({
          serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta,
          serieOrigen: cobro.serieOrigen, folioOrigen: cobro.folioOrigen,
          diaCobro, diffDias, monto: montoEsteCobro, formasPago,
        });
        totalFueraDeTolerancia += montoEsteCobro;
      }
    }
  }

  console.log('\nSuma por claveSat (ventana amplia, TODOS los cobros de esta factura):', JSON.stringify(porClave, null, 2));
  const total = Object.values(porClave).reduce((s, v) => s + v, 0);
  console.log('Suma total (todas las formas de pago):', total.toFixed(2));
  console.log('\nDe eso, cuanto esta FUERA de la tolerancia real de produccion (+/-1 dia):', totalFueraDeTolerancia.toFixed(2));
  console.log('\nDetalle de cobros fuera de tolerancia:');
  for (const d of detalleFueraDeTolerancia) console.log(JSON.stringify(d));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
