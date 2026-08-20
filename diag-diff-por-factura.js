'use strict';
require('dotenv').config();
const { connectMongo, disconnectMongo } = require('./src/config/database.mongo');
const { sequelize } = require('./src/config/database.postgres');
const { Op } = require('sequelize');
const CFDI = require('./src/visor/models/CFDI');
const { _prefetchAjustesFacturaPropia, _uuidsPorFechaEfectiva } = require('./src/banks/domains/cfdi-mapping/cfdi-poliza-generator.service.js');
const mappingSvc = require('./src/banks/domains/cfdi-mapping/cfdi-mapping.service.js');
const CfdiMappingRule = require('./src/shared/models/postgres/CfdiMappingRule');
const AccountPlan = require('./src/shared/models/postgres/AccountPlan');
const PolizaMovimiento = require('./src/shared/models/postgres/PolizaMovimiento');

const CODIGOS_CUENTAS_CAJA_O_BANCO = new Set([
  '1101010003', '1102011005', '1102011001', '1102012001',
  '1102013001', '1102014001', '1102015001', '1102016001',
]);

const RFC = process.env.DIAG_RFC || 'CCO011113663';
const SERIE = process.env.DIAG_SERIE || 'B0';
const FECHA = process.env.DIAG_FECHA || '2026-08-11';
const EJERCICIO = process.env.DIAG_EJERCICIO || '2026';
const PERIODO = process.env.DIAG_PERIODO || '8';

// Compara, factura por factura, lo que MI calculo (misma logica real de
// cfdi-mapping.service.js) atribuye a Efectivo vs lo que esta REALMENTE
// persistido en poliza_movimientos para esos mismos uuids -- para encontrar
// exactamente donde difiere en vez de seguir comparando solo totales.
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
  }).lean();
  console.log(`Total CFDIs reales del batch (source=SAT, serie=${SERIE}, ${FECHA}):`, cfdisSat.length);

  const desde = new Date(`${FECHA}T00:00:00-06:00`);
  const hasta = new Date(`${FECHA}T23:59:59.999-06:00`);

  const rules = await CfdiMappingRule.findAll({ where: { isActive: true }, order: [['prioridad', 'ASC']] });
  const cfdiConRegla = cfdisSat.map(cfdi => ({ cfdi, rule: mappingSvc.findRuleInList(cfdi, rules) }));
  const noGateBaseUuids = new Set(
    cfdiConRegla.filter(({ rule }) => rule && !CODIGOS_CUENTAS_CAJA_O_BANCO.has(rule.cuentaCargo)).map(({ cfdi }) => cfdi.uuid),
  );

  const { desglosePagoReal, puntosUsado, saldoFavorUsado } = await _prefetchAjustesFacturaPropia(cfdiConRegla, RFC, {
    centroPropioClave: SERIE, fechaDesde: desde, fechaHasta: hasta,
  });

  const miEfectivoPorUuid = new Map();
  for (const cfdi of cfdisSat) {
    if (noGateBaseUuids.has(cfdi.uuid)) continue;
    const key = `${cfdi.serie}|${cfdi.folio}`;
    const formasPago = desglosePagoReal.get(key) ?? [];
    const totalFormasPagoReal = formasPago.reduce((s, fp) => s + (Number(fp.monto) || 0), 0);
    const montoSFUsado = Number(saldoFavorUsado.get(key)?.monto) || 0;
    const montoPuntosUsado = Number(puntosUsado.get(key)) || 0;
    const montoCargo = Number(cfdi.total) || 0;

    let efectivo = 0;
    for (const fp of formasPago) {
      if ((fp.claveSat ?? '').trim() === '01') efectivo += Number(fp.monto) || 0;
    }
    const esCasoAjusteSFPuntos = montoSFUsado > 0 || montoPuntosUsado > 0;
    if (!formasPago.length) {
      miEfectivoPorUuid.set(cfdi.uuid, Math.round(efectivo * 100) / 100);
      continue;
    }
    if (esCasoAjusteSFPuntos) {
      const restante = montoCargo - montoSFUsado - Math.min(montoPuntosUsado, montoCargo - montoSFUsado);
      const excesoCubrir = totalFormasPagoReal > 0 ? Math.round((restante - totalFormasPagoReal) * 100) / 100 : 0;
      if (excesoCubrir > 0.01 && totalFormasPagoReal > 0) efectivo += excesoCubrir;
    }
    miEfectivoPorUuid.set(cfdi.uuid, Math.round(efectivo * 100) / 100);
  }
  const miTotal = [...miEfectivoPorUuid.values()].reduce((s, v) => s + v, 0);
  console.log('\nMi total Efectivo (por-uuid, suma):', miTotal.toFixed(2));

  // Lo REAL persistido: poliza_movimientos con cfdi_uuid en este batch,
  // cuenta = Caja (1101010003), sumado por cfdi_uuid.
  const cajaRow = await AccountPlan.findOne({ where: { codigo: '1101010003' }, attributes: ['id'], raw: true });
  const cajaId = cajaRow?.id;
  const todosUuids = cfdisSat.map(c => c.uuid.toUpperCase());
  const movsReales = await PolizaMovimiento.findAll({
    where: { cfdiUuid: { [Op.in]: todosUuids }, cuentaId: cajaId },
    attributes: ['cfdiUuid', 'debe', 'haber', 'tipoOrigen', 'polizaId'],
    raw: true,
  });
  console.log('Movimientos reales encontrados en Caja para estos uuids:', movsReales.length);

  const realPorUuid = new Map();
  for (const m of movsReales) {
    const u = (m.cfdiUuid || '').toUpperCase();
    const neto = Number(m.debe || 0) - Number(m.haber || 0);
    realPorUuid.set(u, (realPorUuid.get(u) ?? 0) + neto);
  }
  const realTotal = [...realPorUuid.values()].reduce((s, v) => s + v, 0);
  console.log('Total REAL persistido en Caja para estos uuids:', realTotal.toFixed(2));

  console.log('\n--- DIFERENCIAS (mi calculo vs real persistido), por factura ---');
  const todasClaves = new Set([...miEfectivoPorUuid.keys()].map(u => u.toUpperCase()), ...realPorUuid.keys());
  let totalDiff = 0;
  for (const u of todasClaves) {
    const mio = miEfectivoPorUuid.get(u) ?? miEfectivoPorUuid.get(u.toLowerCase()) ?? 0;
    const real = realPorUuid.get(u) ?? 0;
    const diff = Math.round((mio - real) * 100) / 100;
    if (Math.abs(diff) > 0.02) {
      const cfdi = cfdisSat.find(c => c.uuid.toUpperCase() === u);
      console.log(`  ${cfdi ? cfdi.serie + '-' + cfdi.folio : '???'} uuid=${u} mio=${mio.toFixed(2)} real=${real.toFixed(2)} diff=${diff.toFixed(2)}`);
      totalDiff += diff;
    }
  }
  console.log('\nSuma de diferencias:', totalDiff.toFixed(2));

  await disconnectMongo();
  await sequelize.close();
  process.exit(0);
}

main().catch(e => { console.error('ERROR:', e); process.exit(1); });
