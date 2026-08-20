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
const Poliza = require('./src/shared/models/postgres/Poliza');

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
  const movsTodas = await PolizaMovimiento.findAll({
    where: { cfdiUuid: { [Op.in]: todosUuids }, cuentaId: cajaId },
    attributes: ['cfdiUuid', 'debe', 'haber', 'tipoOrigen', 'polizaId'],
    raw: true,
  });
  console.log('Movimientos totales en Caja para estos uuids (TODAS las polizas/regeneraciones):', movsTodas.length);

  // Puede haber VARIAS polizas (regeneraciones repetidas durante esta
  // sesion) con movimientos para los mismos uuids -- quedarse solo con la
  // MAS RECIENTE (createdAt) para no sumar duplicados de corridas viejas.
  const polizaIds = [...new Set(movsTodas.map(m => m.polizaId))];
  const polizas = await Poliza.findAll({ where: { id: { [Op.in]: polizaIds } }, attributes: ['id', 'createdAt', 'estado'], raw: true });
  console.log('\nPolizas distintas encontradas:', polizas.map(p => `id=${p.id} estado=${p.estado} createdAt=${p.createdAt}`).join(' | '));
  const polizaMasReciente = polizas.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt))[0];
  console.log('Usando poliza mas reciente: id=', polizaMasReciente?.id);

  const movsReales = movsTodas.filter(m => m.polizaId === polizaMasReciente?.id);
  console.log('Movimientos reales (solo poliza mas reciente):', movsReales.length);

  // Desglose por tipoOrigen -- para ver que categorias, aunque posteen a
  // Caja, se extraen del consolidado "Depositos consolidados (Efectivo)"
  // para mostrarse como fila aparte.
  const porTipoOrigen = new Map();
  for (const m of movsReales) {
    const tag = m.tipoOrigen ?? '(sin tipoOrigen / default)';
    const neto = Number(m.debe || 0) - Number(m.haber || 0);
    porTipoOrigen.set(tag, (porTipoOrigen.get(tag) ?? 0) + neto);
  }
  console.log('\nDesglose por tipoOrigen (dentro de mis 116 uuids, poliza mas reciente):');
  for (const [tag, monto] of porTipoOrigen) console.log(`  ${tag}: ${monto.toFixed(2)}`);

  // TODOS los movimientos de Caja de esta poliza (sin filtrar por uuid) --
  // para ver el total real de la cuenta y comparar contra el consolidado
  // mostrado ($219,089.06) y contra la suma de solo mis 116 uuids.
  const movsCajaTodaLaPoliza = await PolizaMovimiento.findAll({
    where: { polizaId: polizaMasReciente?.id, cuentaId: cajaId },
    attributes: ['cfdiUuid', 'debe', 'haber', 'tipoOrigen'],
    raw: true,
  });
  const totalCajaPoliza = movsCajaTodaLaPoliza.reduce((s, m) => s + Number(m.debe || 0) - Number(m.haber || 0), 0);
  console.log('\nTotal Caja de TODA la poliza (todos los uuids, incluyendo fuera de mis 116):', totalCajaPoliza.toFixed(2));
  const porTipoOrigenTodaPoliza = new Map();
  for (const m of movsCajaTodaLaPoliza) {
    const tag = m.tipoOrigen ?? '(sin tipoOrigen / default)';
    const neto = Number(m.debe || 0) - Number(m.haber || 0);
    porTipoOrigenTodaPoliza.set(tag, (porTipoOrigenTodaPoliza.get(tag) ?? 0) + neto);
  }
  console.log('\nDesglose por tipoOrigen (TODA la poliza, cuenta Caja):');
  for (const [tag, monto] of porTipoOrigenTodaPoliza) console.log(`  ${tag}: ${monto.toFixed(2)}`);

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
