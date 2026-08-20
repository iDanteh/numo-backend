'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerDesglosesCobroAlmacenPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';

// Busca dos posibles causas del residual de Efectivo:
//  1) Un mismo folioOrigen (un solo pago real) que cierra VARIOS tickets
//     distintos -- el bug documentado en el codigo dice que en ese caso
//     `formasPago[].monto` puede traer el TOTAL repetido en cada ticket en
//     vez del monto real de cada uno (cobro.monto SI trae el monto correcto
//     por ticket, pero solo se usa cuando formasPago.length===1).
//  2) Cobros con formasPago.length>1 donde la suma de fp.monto no coincide
//     con |cobro.monto| -- señal de que algo no cierra a nivel de ese cobro.
async function main() {
  await connectMongo();

  const fechaDesdeIso = new Date(`${FECHA}T00:00:00-06:00`).toISOString();
  const fechaHastaIso = new Date(`${FECHA}T23:59:59.999-06:00`).toISOString();
  const resultado = await obtenerDesglosesCobroAlmacenPorCentro({
    rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso,
  });

  const porFolioOrigen = new Map(); // folioOrigen -> [{serieVenta,folioVenta,cobro}]
  const sospechosos = [];

  for (const cuenta of resultado) {
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro !== CENTRO) continue;
      const fechaCobroMx = new Date(cobro.fecha);
      fechaCobroMx.setHours(fechaCobroMx.getHours() - 6);
      const diaCobro = fechaCobroMx.toISOString().slice(0, 10);
      if (diaCobro !== FECHA) continue;

      const origen = (cobro.serieOrigen ?? '').toUpperCase();
      if (origen !== 'CBT' && origen !== 'APS' && !SERIES_CON_AUTH.includes(origen)) continue;

      const key = `${cobro.serieOrigen}|${cobro.folioOrigen}`;
      const lista = porFolioOrigen.get(key) ?? [];
      lista.push({ serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta, cobro });
      porFolioOrigen.set(key, lista);

      const fps = cobro.formasPago ?? [];
      if (fps.length > 1) {
        const sumaFp = fps.reduce((s, fp) => s + (Number(fp.monto) || 0), 0);
        if (Math.abs(sumaFp - Math.abs(Number(cobro.monto) || 0)) > 0.02) {
          sospechosos.push({ key, serieVenta: cuenta.serieVenta, folioVenta: cuenta.folioVenta, cobroMonto: cobro.monto, sumaFp, formasPago: fps });
        }
      }
    }
  }

  console.log('--- folioOrigen que aparecen en MAS DE UN ticket distinto ---');
  let algunRepetido = false;
  for (const [key, lista] of porFolioOrigen) {
    const ticketsUnicos = new Set(lista.map(l => `${l.serieVenta}|${l.folioVenta}`));
    if (ticketsUnicos.size > 1) {
      algunRepetido = true;
      console.log(`\n${key} -> ${ticketsUnicos.size} tickets distintos:`);
      for (const l of lista) {
        console.log(`  ticket ${l.serieVenta}-${l.folioVenta}: cobro.monto=${l.cobro.monto}, formasPago=${JSON.stringify(l.cobro.formasPago)}`);
      }
    }
  }
  if (!algunRepetido) console.log('(ninguno encontrado)');

  console.log('\n--- cobros con formasPago.length>1 donde suma(fp.monto) != |cobro.monto| ---');
  if (!sospechosos.length) console.log('(ninguno encontrado)');
  for (const s of sospechosos) console.log(JSON.stringify(s));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
