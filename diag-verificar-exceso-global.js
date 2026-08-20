'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const CFDI = require('./src/visor/models/CFDI');
const { obtenerDesglosesCobroAlmacenPorCentro, obtenerSaldosFavorPorCentro } = require('./src/banks/domains/erp/erp-sync.service');
const { SERIES_CON_AUTH } = require('./src/banks/domains/erp/erp-auth.utils');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const CENTRO = process.env.DIAG_SERIE || 'B0';
const FOLIO_GLOBAL = process.env.DIAG_FOLIO_GLOBAL || '260801256';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';
const UUID_GLOBAL = process.env.DIAG_UUID || '23503D5C-99D0-481C-9D6F-82C052EEAE50';

// Re-deriva CADA UNO de los 4 numeros del "exceso" desde su fuente original,
// de forma independiente -- para verificar que ninguno esta mal/desactualizado
// antes de confiar en el calculo final.
async function main() {
  await connectMongo();

  // 1. montoCargo -- directo del CFDI real (source=SAT, satStatus=Vigente)
  const cfdi = await CFDI.findOne({ uuid: UUID_GLOBAL, source: 'SAT', satStatus: 'Vigente' })
    .select('uuid total subTotal descuento impuestos folio serie fecha').lean();
  if (!cfdi) {
    console.log('ERROR: no se encontro el CFDI SAT Vigente para ese uuid.');
    process.exit(1);
  }
  console.log('1) montoCargo (CFDI.total, source=SAT, Vigente):', cfdi.total);
  console.log('   subTotal:', cfdi.subTotal, '| descuento:', cfdi.descuento, '| iva trasladado:', cfdi.impuestos?.totalImpuestosTrasladados);
  console.log('   Verificacion: subTotal + IVA - descuento =',
    (Number(cfdi.subTotal || 0) + Number(cfdi.impuestos?.totalImpuestosTrasladados || 0) - Number(cfdi.descuento || 0)).toFixed(2));

  const fechaDesdeIso = new Date(`${FECHA}T00:00:00-06:00`).toISOString();
  const fechaHastaIso = new Date(`${FECHA}T23:59:59.999-06:00`).toISOString();

  // 2. montoSFUsado -- directo de /saldos-favor "por centro"
  const saldosFavor = await obtenerSaldosFavorPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso });
  console.log(`\n2) /saldos-favor por centro: ${saldosFavor.length} registros totales`);
  const sfDeEstaFactura = saldosFavor.filter(s =>
    (s.serieFactura === CENTRO && String(s.folioFactura) === FOLIO_GLOBAL) ||
    (s.serie === CENTRO && String(s.folio) === FOLIO_GLOBAL),
  );
  console.log('   Registros de saldo a favor ligados a esta factura:', sfDeEstaFactura.length);
  console.log('   RAW:', JSON.stringify(sfDeEstaFactura, null, 2));
  const sumaSFEncontrada = sfDeEstaFactura.reduce((s, r) => s + (Number(r.montoUsado ?? r.monto ?? 0) || 0), 0);
  console.log('   Suma SF usado (segun este endpoint):', sumaSFEncontrada.toFixed(2), '(vs el 9730.72 asumido)');

  // 3. montoPuntosUsado y totalFormasPagoReal -- directo de /desgloses-cobro/almacen por centro
  const resultadoAlmacen = await obtenerDesglosesCobroAlmacenPorCentro({ rfc: RFC, centro: CENTRO, fechaDesde: fechaDesdeIso, fechaHasta: fechaHastaIso });
  const cuentasDeLaGlobal = resultadoAlmacen.filter(c => c.serieFactura === CENTRO && String(c.folioFactura) === FOLIO_GLOBAL);
  console.log(`\n3) /desgloses-cobro/almacen por centro: ${cuentasDeLaGlobal.length} tickets ligados a esta factura`);

  let montoPuntosUsado = 0;
  let totalFormasPagoReal = 0;
  const vistos = new Set();
  const porClave = {};
  for (const cuenta of cuentasDeLaGlobal) {
    for (const cobro of (cuenta.cobros ?? [])) {
      if (cobro.claveCentro !== CENTRO) continue;
      const dedupeKey = `${cobro.serieOrigen}|${cobro.folioOrigen}|${cuenta.serieVenta}|${cuenta.folioVenta}`;
      if (vistos.has(dedupeKey)) continue;
      vistos.add(dedupeKey);

      const origen = (cobro.serieOrigen ?? '').toUpperCase();

      // Puntos: SOLO origen CBT, solo formasPago cuyo nombre matchea /puntos/i
      if (origen === 'CBT') {
        for (const fp of (cobro.formasPago ?? [])) {
          if (/puntos/i.test(fp.nombre ?? '')) montoPuntosUsado += Number(fp.monto) || 0;
        }
      }

      if (origen !== 'CBT' && origen !== 'APS' && origen !== 'MIS' && !SERIES_CON_AUTH.includes(origen)) continue;
      const formasPago = cobro.formasPago ?? [];
      for (const fp of formasPago) {
        if (/puntos|saldo\s*a\s*favor/i.test(fp.nombre ?? '')) continue;
        const monto = (formasPago.length === 1 && cobro.monto != null)
          ? Math.abs(Number(cobro.monto) || 0)
          : (Number(fp.monto) || 0);
        totalFormasPagoReal += monto;
        const clave = fp.claveSat ?? '??';
        porClave[clave] = (porClave[clave] || 0) + monto;
      }
    }
  }
  console.log('   montoPuntosUsado (re-derivado):', montoPuntosUsado.toFixed(2), '(vs el 968.92 asumido)');
  console.log('   totalFormasPagoReal (re-derivado):', totalFormasPagoReal.toFixed(2), '(vs el 238038.83 asumido)');
  console.log('   Desglose por claveSat:', JSON.stringify(porClave, null, 2));

  // 4. Recalcular el exceso de punta a punta con los numeros RE-VERIFICADOS
  const montoCargoReal = Number(cfdi.total);
  const restante = Math.round((montoCargoReal - sumaSFEncontrada - montoPuntosUsado) * 100) / 100;
  const excesoCubrir = Math.round((restante - totalFormasPagoReal) * 100) / 100;
  console.log('\n4) RECALCULO END-TO-END con numeros re-verificados:');
  console.log(`   restante = ${montoCargoReal} - ${sumaSFEncontrada.toFixed(2)} - ${montoPuntosUsado.toFixed(2)} = ${restante}`);
  console.log(`   excesoCubrir = ${restante} - ${totalFormasPagoReal.toFixed(2)} = ${excesoCubrir}`);
  console.log('   (Cruzado con facturasVendedorCubiertas=22909.15 ya conocido, exceso neto esperado ~5087.28)');
  console.log('   excesoCubrir NETO (menos cross-branch conocido):', (excesoCubrir - 22909.15).toFixed(2));

  await disconnectMongo();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
