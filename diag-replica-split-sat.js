'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { _prefetchAjustesFacturaPropia, _uuidsPorFechaEfectiva } = require('./src/banks/domains/cfdi-mapping/cfdi-poliza-generator.service.js');

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';
const EJERCICIO = process.env.DIAG_EJERCICIO || '2026';
const PERIODO = process.env.DIAG_PERIODO || '8';

// Replica el lote EXACTO (source='SAT', satStatus='Vigente', filtrado por
// _uuidsPorFechaEfectiva) que usa la generacion real -- a diferencia de los
// diagnosticos anteriores de hoy, que usaban source='ERP' con un filtro de
// fecha propio (incluia 2 CFDIs que la generacion real excluye).
async function main() {
  await connectMongo();
  await sequelize.authenticate();

  const uuidsPorFecha = await _uuidsPorFechaEfectiva({
    rfc: RFC, ejercicio: EJERCICIO, periodo: PERIODO, tipoCfdi: 'I',
    fechaInicio: FECHA, fechaFin: FECHA,
  });

  const cfdisSat = await CFDI.find({
    'emisor.rfc': RFC, ejercicio: Number(EJERCICIO), periodo: Number(PERIODO),
    tipoDeComprobante: 'I', source: 'SAT', satStatus: 'Vigente',
    uuid: { $in: [...uuidsPorFecha] }, isActive: true, serie: SERIE,
  }).select('uuid serie folio fecha total metodoPago formaPago tipoDeComprobante receptor.nombre').lean();
  console.log(`Total CFDIs reales del batch (source=SAT, serie=${SERIE}, ${FECHA}):`, cfdisSat.length);

  const desde = new Date(`${FECHA}T00:00:00-06:00`);
  const hasta = new Date(`${FECHA}T23:59:59.999-06:00`);
  const cfdiConRegla = cfdisSat.map(cfdi => ({ cfdi, rule: { cuentaCargo: '1101010003' } }));
  const { desglosePagoReal, puntosUsado, saldoFavorUsado } = await _prefetchAjustesFacturaPropia(cfdiConRegla, RFC, {
    centroPropioClave: SERIE, fechaDesde: desde, fechaHasta: hasta,
  });

  let totalEfectivo = 0;
  const detalleExceso = [];
  for (const cfdi of cfdisSat) {
    const key = `${cfdi.serie}|${cfdi.folio}`;
    const formasPago = desglosePagoReal.get(key) ?? [];
    const totalFormasPagoReal = formasPago.reduce((s, fp) => s + (Number(fp.monto) || 0), 0);
    const montoSFUsado = Number(saldoFavorUsado.get(key)?.monto) || 0;
    const montoPuntosUsado = Number(puntosUsado.get(key)) || 0;
    const montoCargo = Number(cfdi.total) || 0;

    let efectivoDeEstaFactura = 0;
    for (const fp of formasPago) {
      if ((fp.claveSat ?? '').trim() === '01') efectivoDeEstaFactura += Number(fp.monto) || 0;
    }

    const esCasoAjusteSFPuntos = montoSFUsado > 0 || montoPuntosUsado > 0;
    if (!formasPago.length) {
      // Fallback: cae al formaPago declarado.
      if (!esCasoAjusteSFPuntos && (cfdi.formaPago ?? '') === '01') {
        // sinCobrosEnSucursal=true -> se excluye (Venta Sin Cobro) -- NO se suma.
        detalleExceso.push({ folio: cfdi.folio, tipo: 'FALLBACK_EXCLUIDO_SIN_COBRO', total: cfdi.total });
      }
      continue;
    }

    if (esCasoAjusteSFPuntos) {
      const restante = montoCargo - montoSFUsado - Math.min(montoPuntosUsado, montoCargo - montoSFUsado);
      const excesoCubrir = totalFormasPagoReal > 0 ? Math.round((restante - totalFormasPagoReal) * 100) / 100 : 0;
      if (excesoCubrir > 0.01 && totalFormasPagoReal > 0) {
        efectivoDeEstaFactura += excesoCubrir; // exceso SIEMPRE va a Caja en esta rama (sin excluir)
        detalleExceso.push({ folio: cfdi.folio, tipo: 'SF_PUNTOS_EXCESO_A_CAJA', excesoCubrir, restante, totalFormasPagoReal });
      }
    } else {
      const excesoCasoNormal = totalFormasPagoReal > 0 ? Math.round((montoCargo - totalFormasPagoReal) * 100) / 100 : 0;
      if (Math.abs(excesoCasoNormal) > 0.02) {
        detalleExceso.push({ folio: cfdi.folio, tipo: 'NORMAL_EXCESO_VENTA_SIN_COBRO_EXCLUIDO', excesoCasoNormal, totalFormasPagoReal, montoCargo, formaPagoCFDI: cfdi.formaPago });
      }
      // El exceso aqui se EXCLUYE (Venta Sin Cobro) -- no se suma a efectivoDeEstaFactura.
    }

    totalEfectivo += efectivoDeEstaFactura;
  }

  console.log('\nTotal Efectivo calculado (replica fiel del batch real):', totalEfectivo.toFixed(2));
  console.log('\nCasos con exceso o fallback relevante:');
  for (const d of detalleExceso) console.log(JSON.stringify(d));

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
