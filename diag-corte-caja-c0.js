'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'C0';

async function main() {
  await connectMongo();

  // Ventana estrecha: solo el 13-ago, sin tolerancia de dias, para que el
  // corte de caja sea exacto a ESE dia (no factura, dia de COBRO real).
  const fechaDesdeIso = new Date('2026-08-13T00:00:00-06:00').toISOString();
  const fechaHastaIso = new Date('2026-08-13T23:59:59.999-06:00').toISOString();
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({
    rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso,
  });
  console.log('Total cuentas devueltas:', resultado.length);

  const porClave = {};
  let totalPuntos = 0;
  const vistos = new Set(); // evitar contar el mismo cobro (folioOrigen) 2 veces si aparece en 2 cuentas

  for (const cuenta of resultado) {
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro !== CENTRO) continue; // solo lo cobrado FISICAMENTE en C0 ese dia
      const fechaCobro = (cobro.fecha || '').slice(0, 10);
      // Filtro de dia real de cobro (no de factura) -- ajustar zona horaria Mexico.
      const fechaCobroMx = new Date(cobro.fecha);
      fechaCobroMx.setHours(fechaCobroMx.getHours() - 6);
      const diaCobro = fechaCobroMx.toISOString().slice(0, 10);
      if (diaCobro !== '2026-08-13') continue;

      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      if (origen !== 'CBT' && !SERIES_CON_AUTH.includes(origen)) continue;

      const dedupeKey = `${cobro.serieOrigen}|${cobro.folioOrigen}|${cuenta.serieVenta}|${cuenta.folioVenta}`;
      if (vistos.has(dedupeKey)) continue;
      vistos.add(dedupeKey);

      for (const fp of (cobro.formasPago ?? [])) {
        if (/puntos/i.test(fp.nombre ?? '')) { totalPuntos += Number(fp.monto) || 0; continue; }
        if (/saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        const monto = (cobro.formasPago.length === 1 && cobro.monto != null)
          ? Math.abs(Number(cobro.monto) || 0)
          : (Number(fp.monto) || 0);
        const clave = fp.claveSat ?? '??';
        porClave[clave] = (porClave[clave] || 0) + monto;
      }
    }
  }

  console.log('Suma por claveSat (TODO lo cobrado fisicamente en C0 el 13-ago):', porClave);
  const efectivo = porClave['01'] || 0;
  const tarjeta = (porClave['28'] || 0) + (porClave['04'] || 0);
  const transferencia = porClave['03'] || 0;
  console.log(`\nEfectivo (01): ${efectivo.toFixed(2)}`);
  console.log(`Tarjeta (28+04): ${tarjeta.toFixed(2)}`);
  console.log(`Transferencia (03): ${transferencia.toFixed(2)}`);
  console.log(`Puntos: ${totalPuntos.toFixed(2)}`);
  console.log('\nReferencia esperada: Efectivo 89006.71, Tarjeta 128692.19');

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
