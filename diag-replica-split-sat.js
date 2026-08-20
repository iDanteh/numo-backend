'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const CFDI = require('./src/visor/models/CFDI');
const { _prefetchAjustesFacturaPropia, _uuidsPorFechaEfectiva } = require('./src/banks/domains/cfdi-mapping/cfdi-poliza-generator.service.js');
const mappingSvc = require('./src/banks/domains/cfdi-mapping/cfdi-mapping.service.js');
const CfdiMappingRule = require('./src/shared/models/postgres/CfdiMappingRule');

const CODIGOS_CUENTAS_CAJA_O_BANCO = new Set([
  '1101010003', '1102011005', '1102011001', '1102012001',
  '1102013001', '1102014001', '1102015001', '1102016001',
]);

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

  // Regla REAL por CFDI (antes hardcodeada a 1101010003 -- eso asumia
  // gateBase=true para las 116 facturas, lo cual sobreestimo el Efectivo).
  const rules = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']] });
  const cfdiConRegla = cfdisSat.map(cfdi => ({ cfdi, rule: mappingSvc.findRuleInList(cfdi, rules) }));

  const sinRegla = cfdiConRegla.filter(({ rule }) => !rule);
  console.log('CFDIs sin ninguna regla matcheada:', sinRegla.length);
  for (const { cfdi } of sinRegla) console.log('  SIN REGLA:', cfdi.serie, cfdi.folio, cfdi.total);

  const noGateBase = cfdiConRegla.filter(({ rule }) => rule && !CODIGOS_CUENTAS_CAJA_O_BANCO.has(rule.cuentaCargo));
  console.log('\nCFDIs cuya regla NO apunta a Caja/Bancos (gateBase=false, sin split):', noGateBase.length);
  for (const { cfdi, rule } of noGateBase) {
    console.log(`  ${cfdi.serie}-${cfdi.folio} total=${cfdi.total} formaPagoCFDI=${cfdi.formaPago} reglaNombre=${rule.nombre} cuentaCargoRegla=${rule.cuentaCargo}`);
  }
  const noGateBaseUuids = new Set(noGateBase.map(({ cfdi }) => cfdi.uuid));

  const { desglosePagoReal, puntosUsado, saldoFavorUsado } = await _prefetchAjustesFacturaPropia(cfdiConRegla, RFC, {
    centroPropioClave: SERIE, fechaDesde: desde, fechaHasta: hasta,
  });

  let totalEfectivo = 0;
  let efectivoExcluidoPorGateBase = 0;
  const detalleExceso = [];
  for (const cfdi of cfdisSat) {
    if (noGateBaseUuids.has(cfdi.uuid)) {
      // gateBase=false: la regla real NO es Caja/Bancos -- el split nunca
      // ocurre, todo el montoCargo va a la cuenta que diga la regla, sin
      // importar la forma de pago real. Medimos aqui cuanto "efectivo real"
      // se estaria perdiendo de este calculo por esta razon.
      const key = `${cfdi.serie}|${cfdi.folio}`;
      const formasPago = desglosePagoReal.get(key) ?? [];
      const efectivoQueSeHubieraContado = formasPago
        .filter(fp => (fp.claveSat ?? '').trim() === '01')
        .reduce((s, fp) => s + (Number(fp.monto) || 0), 0);
      efectivoExcluidoPorGateBase += efectivoQueSeHubieraContado;
      continue;
    }
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

  console.log('\nTotal Efectivo calculado (replica fiel del batch real, respetando gateBase):', totalEfectivo.toFixed(2));
  console.log('Efectivo real que NO entra por gateBase=false (regla especifica, sin split):', efectivoExcluidoPorGateBase.toFixed(2));
  console.log('\nCasos con exceso o fallback relevante:');
  for (const d of detalleExceso) console.log(JSON.stringify(d));

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
