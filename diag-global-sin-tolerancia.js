'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacen } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'B0';
const FOLIO_GLOBAL = process.env.DIAG_FOLIO_GLOBAL || '260801256';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';

// Consulta el endpoint POR FACTURA (serie+folio propio, SIN ninguna
// restricción de fecha/tolerancia) para ver si trae MAS tickets que la
// consulta "por centro" (que usa una ventana de fecha con tolerancia) --
// si aqui aparecen tickets que "por centro" no encontro, ese es el origen
// del "exceso" que se esta yendo a Caja sin identificar.
async function main() {
  await connectMongo();

  const cuentas = await obtenerDesglosesCobroAlmacen({ rfc: RFC, series: [SERIE], folios: [FOLIO_GLOBAL] });
  console.log(`Total "cuentas" (tickets) devueltos por /desgloses-cobro/almacen para ${SERIE}-${FOLIO_GLOBAL}:`, cuentas.length);
  console.log('RAW:', JSON.stringify(cuentas, null, 2));

  const porClave = {};
  const detalle = [];
  const vistos = new Set();
  let sumaTotalTickets = 0;
  const folioVentaVistos = new Set();

  for (const cuenta of cuentas) {
    if (cuenta.serieFactura !== SERIE || String(cuenta.folioFactura) !== FOLIO_GLOBAL) continue;
    if (!folioVentaVistos.has(`${cuenta.serieVenta}|${cuenta.folioVenta}`)) {
      folioVentaVistos.add(`${cuenta.serieVenta}|${cuenta.folioVenta}`);
    }
    for (const cobro of (cuenta.cobros ?? [])) {
      const fechaCobroMx = new Date(cobro.fecha);
      fechaCobroMx.setHours(fechaCobroMx.getHours() - 6);
      const diaCobro = fechaCobroMx.toISOString().slice(0, 10);

      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      const reconocido = origen === 'CBT' || origen === 'APS' || origen === 'MIS' || SERIES_CON_AUTH.includes(origen);

      const dedupeKey = `${cobro.serieOrigen}|${cobro.folioOrigen}|${cuenta.serieVenta}|${cuenta.folioVenta}`;
      const yaVisto = vistos.has(dedupeKey);
      if (!yaVisto) vistos.add(dedupeKey);

      detalle.push({
        serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta,
        serieOrigen: cobro.serieOrigen, folioOrigen: cobro.folioOrigen,
        claveCentro: cobro.claveCentro, monto: cobro.monto, fecha: cobro.fecha,
        diaCobro, dentroDelDiaObjetivo: diaCobro === FECHA,
        reconocido, yaVisto,
        formasPago: cobro.formasPago,
      });

      if (!reconocido || yaVisto) continue;
      const formasPago = cobro.formasPago ?? [];
      for (const fp of formasPago) {
        if (/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        const monto = (formasPago.length === 1 && cobro.monto != null)
          ? Math.abs(Number(cobro.monto) || 0)
          : (Number(fp.monto) || 0);
        const clave = fp.claveSat ?? '??';
        porClave[clave] = (porClave[clave] || 0) + monto;
        sumaTotalTickets += monto;
      }
    }
  }

  console.log('Total tickets (serieVenta/folioVenta) distintos ligados a esta factura:', folioVentaVistos.size);
  console.log('\nSuma por claveSat (SIN restriccion de fecha, todo lo que traiga el ERP para esta factura):', JSON.stringify(porClave, null, 2));
  console.log('Suma total (todas las formas de pago):', sumaTotalTickets.toFixed(2));

  console.log('\nDetalle de cobros que NO caen en el dia objetivo o no son reconocidos:');
  for (const d of detalle) {
    if (!d.dentroDelDiaObjetivo || !d.reconocido || d.yaVisto) {
      console.log(JSON.stringify(d));
    }
  }

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
