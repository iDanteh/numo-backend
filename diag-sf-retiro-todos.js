'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { obtenerSaldosFavorPorCentro } = require('./src/banks/domains/erp/erp-sync.service');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';

function _diaMx(fechaIso) {
  if (!fechaIso) return null;
  return new Date(new Date(fechaIso).getTime() - 6 * 3600 * 1000).toISOString().slice(0, 10);
}

// Replica EXACTA de la logica de `ajustesEfectivoRetiroSF` en
// _prefetchSaldosFavorGenerados (cfdi-poliza-generator.service.js) para ver
// TODOS los casos que estan siendo restados del consolidado de Efectivo --
// sospecha: varios saldos a favor distintos "usan" la MISMA referencia
// generica ABO 260800585 (CAJA CONTABILIDAD HIDALGO), sumando de mas.
async function main() {
  await connectMongo();

  const fechaDesdeIso = new Date(`${FECHA}T00:00:00-06:00`).toISOString();
  const fechaHastaIso = new Date(`${FECHA}T23:59:59.999-06:00`).toISOString();
  const resultado = await obtenerSaldosFavorPorCentro({
    rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso,
  });

  let totalRetiro = 0;
  const detalle = [];
  for (const cuenta of resultado) {
    for (const gen of (cuenta.saldosFavorGenerados ?? [])) {
      const usos = gen.usos ?? [];
      const diaGen = _diaMx(gen.fecha);
      const usosMismoDia = diaGen ? usos.filter(u => _diaMx(u.fecha) === diaGen) : [];
      for (const u of usosMismoDia) {
        const origen = (u.serieOrigen ?? u.serieVenta ?? '').toUpperCase();
        if (origen !== 'ABO') continue;
        const montoRetiro = Math.abs(Number(u.montoUsado)) || 0;
        if (montoRetiro <= 0) continue;
        totalRetiro += montoRetiro;
        detalle.push({
          ventaQueGeneroSF: `${cuenta.serieVenta}-${cuenta.folioVenta}`,
          genSerieOrigen: gen.serieOrigen, genFolioOrigen: gen.folioOrigen, genMonto: gen.monto,
          usoSerieVenta: u.serieVenta, usoFolioVenta: u.folioVenta, montoUsado: u.montoUsado, fechaUso: u.fecha,
        });
      }
    }
  }

  console.log(`Total "retiro efectivo de SF" detectado (${CENTRO}, ${FECHA}): $${totalRetiro.toFixed(2)}\n`);
  console.log('Detalle:');
  for (const d of detalle) console.log(JSON.stringify(d));

  // Agrupar por la referencia de uso (folioVenta) para ver si se repite.
  const porReferencia = {};
  for (const d of detalle) {
    const k = `${d.usoSerieVenta}-${d.usoFolioVenta}`;
    porReferencia[k] = (porReferencia[k] || 0) + (Number(d.montoUsado) || 0);
  }
  console.log('\nSuma por referencia de uso (serieVenta-folioVenta):');
  console.log(JSON.stringify(porReferencia, null, 2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
