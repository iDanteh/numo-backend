'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11'; // dia "de negocio" objetivo

// Prueba distintos cortes horarios (0..23, hora Mexico) para el limite de
// "dia de negocio", para ver si alguno hace que Efectivo/Tarjeta cierren
// EXACTO contra la referencia del ERP -- en vez de asumir medianoche.
async function main() {
  await connectMongo();

  // Ventana amplia (dia anterior 00:00 a dia siguiente 23:59, hora MX) para
  // tener TODOS los cobros candidatos sin volver a pegarle al ERP por cada
  // corte horario que probemos.
  const desdeAmplio = new Date(`${FECHA}T00:00:00-06:00`);
  desdeAmplio.setUTCDate(desdeAmplio.getUTCDate() - 1);
  const hastaAmplio = new Date(`${FECHA}T23:59:59.999-06:00`);
  hastaAmplio.setUTCDate(hastaAmplio.getUTCDate() + 1);

  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({
    rfc: RFC, centro: CENTRO, fechaDesde: desdeAmplio.toISOString(), fechaHasta: hastaAmplio.toISOString(),
  });

  // Recolectar TODOS los cobros validos (reconocidos) de este centro con su
  // timestamp MX (hora local, sin redondear a dia) y su desglose real.
  const cobrosValidos = [];
  const vistos = new Set();
  for (const cuenta of resultado) {
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro !== CENTRO) continue;
      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      if (origen !== 'CBT' && origen !== 'APS' && !SERIES_CON_AUTH.includes(origen)) continue;
      const dedupeKey = `${cobro.serieOrigen}|${cobro.folioOrigen}|${cuenta.serieVenta}|${cuenta.folioVenta}`;
      if (vistos.has(dedupeKey)) continue;
      vistos.add(dedupeKey);

      const fechaMx = new Date(new Date(cobro.fecha).getTime() - 6 * 3600 * 1000);
      for (const fp of (cobro.formasPago ?? [])) {
        if (/puntos/i.test(fp.nombre ?? '')) continue;
        if (/saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        const monto = (cobro.formasPago.length === 1 && cobro.monto != null)
          ? Math.abs(Number(cobro.monto) || 0)
          : (Number(fp.monto) || 0);
        cobrosValidos.push({ fechaMx, claveSat: fp.claveSat ?? '??', monto });
      }
    }
  }

  console.log(`Total lineas de forma de pago candidatas: ${cobrosValidos.length}\n`);
  console.log('corteHora | Efectivo(01) | Tarjeta(28+04) | Transferencia(03)');
  for (let corteHora = 0; corteHora < 24; corteHora++) {
    const diaObjetivo = new Date(`${FECHA}T00:00:00-06:00`);
    const inicio = new Date(diaObjetivo.getTime() + corteHora * 3600 * 1000);
    const fin = new Date(inicio.getTime() + 24 * 3600 * 1000);
    const porClave = {};
    for (const c of cobrosValidos) {
      if (c.fechaMx.getTime() < inicio.getTime() || c.fechaMx.getTime() >= fin.getTime()) continue;
      porClave[c.claveSat] = (porClave[c.claveSat] || 0) + c.monto;
    }
    const efectivo = porClave['01'] || 0;
    const tarjeta = (porClave['28'] || 0) + (porClave['04'] || 0);
    const transferencia = porClave['03'] || 0;
    console.log(`${String(corteHora).padStart(2, '0')}:00 MX | ${efectivo.toFixed(2)} | ${tarjeta.toFixed(2)} | ${transferencia.toFixed(2)}`);
  }
  console.log('\nReferencia objetivo: Efectivo 256295.27, Tarjeta 114363.62');

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
